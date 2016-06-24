/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.kinesis;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;

import com.facebook.presto.kinesis.decoder.KinesisFieldDecoder;
import com.facebook.presto.kinesis.decoder.KinesisRowDecoder;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class KinesisRecordSet
        implements RecordSet
{
    private static final Logger log = Logger.get(KinesisRecordSet.class);

    private static final byte [] EMPTY_BYTE_ARRAY = new byte [0];

    private final KinesisSplit split;
    private final ConnectorSession session;
    private final KinesisClientProvider clientManager;
    private final KinesisConnectorConfig kinesisConnectorConfig;

    private final KinesisRowDecoder messageDecoder;
    private final Map<KinesisColumnHandle, KinesisFieldDecoder<?>> messageFieldDecoders;

    private final List<KinesisColumnHandle> columnHandles;
    private final List<Type> columnTypes;

    private final int batchSize;
    private final int fetchAttempts;
    private final long sleepTime;

    //for checkpointing
    private final boolean checkpointEnabled;
    private String lastReadSeqNo;
    private KinesisShardCheckpointer kinesisShardCheckpointer;

    private final Set<KinesisFieldValueProvider> globalInternalFieldValueProviders;

    KinesisRecordSet(KinesisSplit split,
            ConnectorSession session,
            KinesisClientProvider clientManager,
            List<KinesisColumnHandle> columnHandles,
            KinesisRowDecoder messageDecoder,
            Map<KinesisColumnHandle, KinesisFieldDecoder<?>> messageFieldDecoders,
            KinesisConnectorConfig kinesisConnectorConfig)
    {
        this.split = checkNotNull(split, "split is null");
        this.session = checkNotNull(session, "session is null");
        this.kinesisConnectorConfig = checkNotNull(kinesisConnectorConfig, "KinesisConnectorConfig is null");

        this.globalInternalFieldValueProviders = ImmutableSet.of(
                KinesisInternalFieldDescription.SHARD_ID_FIELD.forByteValue(split.getShardId().getBytes()),
                KinesisInternalFieldDescription.SEGMENT_START_FIELD.forByteValue(split.getStart().getBytes()));

        this.clientManager = checkNotNull(clientManager, "clientManager is null");

        this.messageDecoder = checkNotNull(messageDecoder, "rowDecoder is null");
        this.messageFieldDecoders = checkNotNull(messageFieldDecoders, "messageFieldDecoders is null");

        this.columnHandles = checkNotNull(columnHandles, "columnHandles is null");

        ImmutableList.Builder<Type> typeBuilder = ImmutableList.builder();

        for (KinesisColumnHandle handle : columnHandles) {
            typeBuilder.add(handle.getType());
        }

        this.columnTypes = typeBuilder.build();

        this.batchSize = kinesisConnectorConfig.getBatchSize();
        this.fetchAttempts = kinesisConnectorConfig.getFetchAttempts();
        this.sleepTime = kinesisConnectorConfig.getSleepTime().toMillis();

        this.checkpointEnabled = kinesisConnectorConfig.isCheckpointEnabled();
        this.lastReadSeqNo = null;
        this.kinesisShardCheckpointer = null;
        checkpoint();
    }

    public void checkpoint()
    {
        if (checkpointEnabled) {
            if (kinesisShardCheckpointer == null) {
                AmazonDynamoDBClient dynamoDBClient = clientManager.getDynamoDBClient();
                long dynamoReadCapacity = kinesisConnectorConfig.getDynamoReadCapacity();
                long dynamoWriteCapacity = kinesisConnectorConfig.getDynamoWriteCapacity();
                long checkpointIntervalMs = kinesisConnectorConfig.getCheckpointIntervalMS().toMillis();
                String logicalProcessName = kinesisConnectorConfig.getLogicalProcessName();
                String dynamoDBTable = split.getStreamName();
                int curIterationNumber = kinesisConnectorConfig.getIterationNumber();

                String sessionIterationNo = KinesisTableHandle.getSessionProperty(this.session, "iteration_number");
                String sessionLogicalName = KinesisTableHandle.getSessionProperty(this.session, "checkpoint_logical_name");

                if (sessionIterationNo != null) {
                    curIterationNumber = Integer.parseInt(sessionIterationNo);
                }

                if (sessionLogicalName != null) {
                    logicalProcessName = sessionLogicalName;
                }

                kinesisShardCheckpointer = new KinesisShardCheckpointer(dynamoDBClient,
                        dynamoDBTable,
                        split,
                        logicalProcessName,
                        curIterationNumber,
                        checkpointIntervalMs,
                        dynamoReadCapacity,
                        dynamoWriteCapacity);

                lastReadSeqNo = kinesisShardCheckpointer.getLastReadSeqNo();
            }
        }
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new KinesisRecordCursor();
    }

    public class KinesisRecordCursor
            implements RecordCursor
    {
        private long batchesRead = 0;
        private long totalBytes = 0;
        private long totalMessages = 0;
        private long lastReadTime = 0;

        private  String shardIterator;
        private KinesisFieldValueProvider[] fieldValueProviders;
        private List<Record> kinesisRecords;
        private Iterator<Record> listIterator;
        private GetRecordsRequest getRecordsRequest;
        private GetRecordsResult getRecordsResult;

        @Override
        public long getTotalBytes()
        {
            return totalBytes;
        }

        @Override
        public long getCompletedBytes()
        {
            return totalBytes;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public Type getType(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");
            return columnHandles.get(field).getType();
        }

        @Override
        public boolean advanceNextPosition()
        {
            if (shardIterator == null && getRecordsRequest == null) {
                getIterator(); // first shard iterator
                log.debug("Retrieved first shard iterator from AWS Kinesis.");
            }

            if (getRecordsRequest == null || (!listIterator.hasNext() && shouldGetMoreRecords())) {
                getKinesisRecords();
            }

            if (listIterator.hasNext()) {
                return nextRow();
            }
            else {
                log.info("Read all of the records from the shard:  %d batches and %d messages.", batchesRead, totalMessages);
                return false;
            }
        }

        /** Another place to control the logic of whether or not to continue. */
        private boolean shouldGetMoreRecords()
        {
            return shardIterator != null && batchesRead < fetchAttempts;
        }

        /** Retrieves the next batch of records from Kinesis using the shard iterator. */
        private void getKinesisRecords()
                throws ResourceNotFoundException
        {
            long now = System.currentTimeMillis();
            if (now - lastReadTime <= sleepTime) {
                try {
                    Thread.sleep(now - lastReadTime);
                }
                catch (InterruptedException e) {
                    log.error("Sleep interrupted.", e);
                }
            }
            getRecordsRequest = new GetRecordsRequest();
            getRecordsRequest.setShardIterator(shardIterator);
            getRecordsRequest.setLimit(batchSize);

            log.debug("Performing getRecords from Kinesis.");
            getRecordsResult = clientManager.getClient().getRecords(getRecordsRequest);
            lastReadTime = System.currentTimeMillis();

            kinesisRecords = getRecordsResult.getRecords();
            log.debug("Fetched %d records from Kinesis.  MillisBehindLatest=%d", kinesisRecords.size(), getRecordsResult.getMillisBehindLatest());
            listIterator = kinesisRecords.iterator();
            batchesRead++;

            // TODO: use millisBehindLatest to see if there are really more records
            shardIterator = getRecordsResult.getNextShardIterator();
        }

        /** Working from the internal list, advance to the next row and decode it. */
        private boolean nextRow()
        {
            Record currentRecord = listIterator.next();
            String partitionKey = currentRecord.getPartitionKey();
            log.debug("Reading record with partition key %s", partitionKey);

            byte[] messageData = EMPTY_BYTE_ARRAY;
            ByteBuffer message = currentRecord.getData();
            if (message != null) {
                messageData = new byte[message.remaining()];
                message.get(messageData);
            }
            totalBytes += messageData.length;
            totalMessages++;

            log.debug("Fetching %d bytes from current record. %d messages read so far", messageData.length, totalMessages);

            Set<KinesisFieldValueProvider> fieldValueProviders = new HashSet<>();

            fieldValueProviders.addAll(globalInternalFieldValueProviders);
            fieldValueProviders.add(KinesisInternalFieldDescription.SEGMENT_COUNT_FIELD.forLongValue(totalMessages));
            fieldValueProviders.add(KinesisInternalFieldDescription.SHARD_SEQUENCE_ID_FIELD.forByteValue(currentRecord.getSequenceNumber().getBytes()));
            fieldValueProviders.add(KinesisInternalFieldDescription.MESSAGE_FIELD.forByteValue(messageData));
            fieldValueProviders.add(KinesisInternalFieldDescription.MESSAGE_LENGTH_FIELD.forLongValue(messageData.length));
            fieldValueProviders.add(KinesisInternalFieldDescription.MESSAGE_VALID_FIELD.forBooleanValue(messageDecoder.decodeRow(messageData, fieldValueProviders, columnHandles, messageFieldDecoders)));
            fieldValueProviders.add(KinesisInternalFieldDescription.PARTITION_KEY_FIELD.forByteValue(partitionKey.getBytes()));

            this.fieldValueProviders = new KinesisFieldValueProvider[columnHandles.size()];

            for (int i = 0; i < columnHandles.size(); i++) {
                for (KinesisFieldValueProvider fieldValueProvider : fieldValueProviders) {
                    if (fieldValueProvider.accept(columnHandles.get(i))) {
                        this.fieldValueProviders[i] = fieldValueProvider;
                        break;
                    }
                }
            }

            lastReadSeqNo = currentRecord.getSequenceNumber();
            if (checkpointEnabled) {
                kinesisShardCheckpointer.checkpointIfTimeUp(lastReadSeqNo);
            }

            return true;
        }

        @Override
        public boolean getBoolean(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");

            checkFieldType(field, boolean.class);
            return !isNull(field) && fieldValueProviders[field].getBoolean();
        }

        @Override
        public long getLong(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");

            checkFieldType(field, long.class);
            return isNull(field) ? 0L : fieldValueProviders[field].getLong();
        }

        @Override
        public double getDouble(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");

            checkFieldType(field, double.class);
            return isNull(field) ? 0.0d : fieldValueProviders[field].getDouble();
        }

        @Override
        public Slice getSlice(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");

            checkFieldType(field, Slice.class);
            return isNull(field) ? Slices.EMPTY_SLICE : fieldValueProviders[field].getSlice();
        }

        @Override
        public Object getObject(int i)
        {
            // TODO: review if we want to support this
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isNull(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");

            return fieldValueProviders[field] == null || fieldValueProviders[field].isNull();
        }

        @Override
        public void close()
        {
            log.info("Read complete");
            if (checkpointEnabled && lastReadSeqNo != null) {
                kinesisShardCheckpointer.checkpoint(lastReadSeqNo);
            }
        }

        private void checkFieldType(int field, Class<?> expected)
        {
            Class<?> actual = getType(field).getJavaType();
            checkArgument(actual == expected, "Expected field %s to be type %s but is %s", field, expected, actual);
        }

        private void getIterator()
                throws ResourceNotFoundException
        {
            GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
            getShardIteratorRequest.setStreamName(split.getStreamName());
            getShardIteratorRequest.setShardId(split.getShardId());
            if (lastReadSeqNo == null) {
                getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON");
            }
            else {
                getShardIteratorRequest.setShardIteratorType("AFTER_SEQUENCE_NUMBER");
                getShardIteratorRequest.setStartingSequenceNumber(lastReadSeqNo);
            }

            GetShardIteratorResult getShardIteratorResult = clientManager.getClient().getShardIterator(getShardIteratorRequest);
            shardIterator = getShardIteratorResult.getShardIterator();
        }
    }
}
