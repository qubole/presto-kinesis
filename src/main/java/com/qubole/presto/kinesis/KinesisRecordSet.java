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
package com.qubole.presto.kinesis;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.qubole.presto.kinesis.decoder.KinesisFieldDecoder;
import com.qubole.presto.kinesis.decoder.KinesisRowDecoder;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.nio.ByteBuffer;
import java.util.Date;
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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class KinesisRecordSet
        implements RecordSet
{
    /** Indicates how close to current we want to be before stopping the fetch of records in a query. */
    public static final int MILLIS_BEHIND_LIMIT = 10000;

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
    private final int maxBatches;
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

        // Note: these default to what is in the configuration if not given in the session
        this.batchSize = SessionVariables.getBatchSize(this.session);
        this.maxBatches = SessionVariables.getMaxBatches(this.session);

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

                String sessionIterationNo = SessionVariables.getSessionProperty(this.session, SessionVariables.ITERATION_NUMBER);
                String sessionLogicalName = SessionVariables.getSessionProperty(this.session, SessionVariables.CHECKPOINT_LOGICAL_NAME);

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
        // TODO: total bytes here only includes records we iterate through, not total read from Kinesis.
        // This may not be an issue, but if total vs. completed is an important signal to Presto then
        // the implementation below could be a problem.  Need to investigate.
        private long batchesRead = 0;
        private long messagesRead = 0;
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

        /**
         * Advances the cursor by one position, retrieving more records from Kinesis if needed.
         *
         * We retrieve records from Kinesis in batches, using the getRecordsRequest.  After a
         * getRecordsRequest we keep iterating through that list of records until we run out.  Then
         * we will get another batch unless we've hit the limit or have caught up.
         *
         * @return
         */
        @Override
        public boolean advanceNextPosition()
        {
            if (shardIterator == null && getRecordsRequest == null) {
                getIterator(); // first shard iterator
                log.info("Starting read.  Retrieved first shard iterator from AWS Kinesis.");
            }

            if (getRecordsRequest == null || (!listIterator.hasNext() && shouldGetMoreRecords())) {
                getKinesisRecords();
            }

            if (listIterator.hasNext()) {
                return nextRow();
            }
            else {
                log.info("Read all of the records from the shard:  %d batches and %d messages and %d total bytes.", batchesRead, totalMessages, totalBytes);
                return false;
            }
        }

        /** Determine whether or not to retrieve another batch of records from Kinesis. */
        private boolean shouldGetMoreRecords()
        {
            return shardIterator != null && batchesRead < maxBatches &&
                    getMillisBehindLatest() > MILLIS_BEHIND_LIMIT;
        }

        /**
         * Retrieves the next batch of records from Kinesis using the shard iterator.
         *
         * Most of the time this results in one getRecords call.  However we allow for
         * a call to return an empty list, and we'll try again if we are far enough
         * away from the latest record.
         */
        private void getKinesisRecords()
                throws ResourceNotFoundException
        {
            // Normally this loop will execute once, but we have to allow for the odd Kinesis
            // behavior, per the docs:
            // A single call to getRecords might return an empty record list, even when the shard contains
            // more records at later sequence numbers
            boolean fetchedRecords = false;
            int attempts = 0;
            while (!fetchedRecords && attempts < fetchAttempts) {
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

                getRecordsResult = clientManager.getClient().getRecords(getRecordsRequest);
                lastReadTime = System.currentTimeMillis();

                shardIterator = getRecordsResult.getNextShardIterator();
                kinesisRecords = getRecordsResult.getRecords();
                if (kinesisConnectorConfig.isLogBatches()) {
                    log.info("Fetched %d records from Kinesis.  MillisBehindLatest=%d", kinesisRecords.size(), getRecordsResult.getMillisBehindLatest());
                }

                fetchedRecords = (kinesisRecords.size() > 0 || getMillisBehindLatest() <= MILLIS_BEHIND_LIMIT);
                attempts++;
            }

            listIterator = kinesisRecords.iterator();
            batchesRead++;
            messagesRead += kinesisRecords.size();
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

            // Note: older version of SDK used in Presto doesn't support getApproximateArrivalTimestamp so can't get message timestamp!
            fieldValueProviders.addAll(globalInternalFieldValueProviders);
            fieldValueProviders.add(KinesisInternalFieldDescription.SEGMENT_COUNT_FIELD.forLongValue(totalMessages));
            fieldValueProviders.add(KinesisInternalFieldDescription.SHARD_SEQUENCE_ID_FIELD.forByteValue(currentRecord.getSequenceNumber().getBytes()));
            fieldValueProviders.add(KinesisInternalFieldDescription.MESSAGE_FIELD.forByteValue(messageData));
            fieldValueProviders.add(KinesisInternalFieldDescription.MESSAGE_LENGTH_FIELD.forLongValue(messageData.length));
            fieldValueProviders.add(KinesisInternalFieldDescription.MESSAGE_TIMESTAMP.forLongValue(currentRecord.getApproximateArrivalTimestamp().getTime()));
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

        /** Protect against possibly null values if this isn't set (not expected) */
        private long getMillisBehindLatest()
        {
            if (getRecordsResult != null && getRecordsResult.getMillisBehindLatest() != null) {
                return getRecordsResult.getMillisBehindLatest();
            }
            else {
                return MILLIS_BEHIND_LIMIT + 1;
            }
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
            log.info("Closing cursor - read complete.  Total read: %d batches %d messages, processed: %d messages and %d bytes.",
                    batchesRead, messagesRead, totalMessages, totalBytes);
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

            // Explanation: when we have a sequence number from a prior read or checkpoint, always use it.
            // Otherwise, decide if starting at a timestamp or the trim horizon based on configuration.
            // If starting at a timestamp, sue the session variable ITER_START_TIMESTAMP when given, otherwise
            // fallback on starting at ITER_OFFSET_SECONDS from timestamp.
            if (lastReadSeqNo == null) {
                // Important: shard iterator type AT_TIMESTAMP requires 1.11.x or above of the AWS SDK.
                if (SessionVariables.getIterFromTimestamp(session)) {
                    getShardIteratorRequest.setShardIteratorType("AT_TIMESTAMP");
                    long iterStartTs = SessionVariables.getIterStartTimestamp(session);
                    if (iterStartTs == 0) {
                        long startTs = System.currentTimeMillis() - (SessionVariables.getIterOffsetSeconds(session) * 1000);
                        getShardIteratorRequest.setTimestamp(new Date(startTs));
                    }
                    else {
                        getShardIteratorRequest.setTimestamp(new Date(iterStartTs));
                    }
                }
                else {
                    getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON");
                }
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
