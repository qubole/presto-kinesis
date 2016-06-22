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
package com.facebook.presto.kinesis.util;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.regions.Region;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.AddTagsToStreamRequest;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DeleteStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.amazonaws.services.kinesis.model.ListTagsForStreamRequest;
import com.amazonaws.services.kinesis.model.ListTagsForStreamResult;
import com.amazonaws.services.kinesis.model.MergeShardsRequest;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.RemoveTagsFromStreamRequest;
import com.amazonaws.services.kinesis.model.SequenceNumberRange;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.SplitShardRequest;
import com.amazonaws.services.kinesis.model.StreamDescription;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * Mock kinesis client for testing that is primarily used for reading from the
 * stream as we do here in Presto.
 *
 * This is to help prove that the API is being used correctly and debug any
 * issues that arise.
 *
 * Created by derekbennett on 6/20/16.
 */
public class MockKinesisClient extends AmazonKinesisClient
{
    private String endpoint = "";
    private Region region = null;
    ArrayList<InternalStream> streams = new ArrayList<InternalStream>();

    //// Support classes

    public static class InternalShard extends Shard
    {
        private ArrayList<Record> recs = new ArrayList<Record>();
        private String streamName = "";
        private int index = 0;

        public InternalShard(String owningStream, int anIndex)
        {
            super();
            this.streamName = owningStream;
            this.index = anIndex;
            this.setShardId(this.streamName + "_" + this.index);
        }

        public ArrayList<Record> getRecords()
        {
            return recs;
        }

        public String getStreamName()
        {
            return streamName;
        }

        public int getIndex()
        {
            return index;
        }

        public void addRecord(Record rec)
        {
            recs.add(rec);
        }

        public void clearRecords()
        {
            recs.clear();
        }
    }

    public static class InternalStream
    {
        private String streamName = "";
        private String streamARN = "";
        private String streamStatus = "CREATING";
        private int retentionPeriodHours = 24;
        private ArrayList<InternalShard> shards = new ArrayList<InternalShard>();
        private int sequenceNo = 100;
        private int nextShard = 0;

        public InternalStream(String aName, int nbShards, boolean isActive)
        {
            this.streamName = aName;
            this.streamARN = "local:fake.stream:" + aName;
            if (isActive) {
                this.streamStatus = "ACTIVE";
            }

            for (int i = 0; i < nbShards; i++) {
                InternalShard newShard = new InternalShard(this.streamName, i);
                newShard.setSequenceNumberRange((new SequenceNumberRange()).withStartingSequenceNumber("100").withEndingSequenceNumber("999"));
                this.shards.add(newShard);
            }
        }

        public String getStreamName()
        {
            return streamName;
        }

        public String getStreamARN()
        {
            return streamARN;
        }

        public String getStreamStatus()
        {
            return streamStatus;
        }

        public int getRetentionPeriodHours()
        {
            return retentionPeriodHours;
        }

        public ArrayList<InternalShard> getShards()
        {
            return shards;
        }

        public ArrayList<InternalShard> getShardsFrom(String afterShardId)
        {
            String[] comps = afterShardId.split("_");
            if (comps.length == 2) {
                ArrayList<InternalShard> returnArray = new ArrayList<InternalShard>();
                int afterIndex = Integer.parseInt(comps[1]);
                if (shards.size() > afterIndex + 1) {
                    for (InternalShard shard : shards) {
                        if (shard.getIndex() > afterIndex) {
                            returnArray.add(shard);
                        }
                    }
                }

                return returnArray;
            }
            else {
                return new ArrayList<InternalShard>();
            }
        }

        public void activate()
        {
            this.streamStatus = "ACTIVE";
        }

        public PutRecordResult putRecord(ByteBuffer data, String partitionKey)
        {
            // Create record and insert into the shards.  Initially just do it
            // on a round robin basis.
            Record rec = new Record();
            rec = rec.withData(data).withPartitionKey(partitionKey).withSequenceNumber(String.valueOf(sequenceNo));

            if (nextShard == shards.size()) {
                nextShard = 0;
            }
            InternalShard shard = shards.get(nextShard);
            shard.addRecord(rec);

            PutRecordResult result = new PutRecordResult();
            result.setSequenceNumber(String.valueOf(sequenceNo));
            result.setShardId(shard.getShardId());

            nextShard++;
            sequenceNo++;

            return result;
        }

        public void clearRecords()
        {
            for (InternalShard shard : this.shards) {
                shard.clearRecords();
            }
        }
    }

    public static class ShardIterator
    {
        public String streamId = "";
        public int shardIndex = 0;
        public int recordIndex = 0;

        public ShardIterator(String aStreamId, int aShard, int aRecord)
        {
            this.streamId = aStreamId;
            this.shardIndex = aShard;
            this.recordIndex = aRecord;
        }

        public String makeString()
        {
            return this.streamId + "_" + this.shardIndex + "_" + this.recordIndex;
        }

        public static ShardIterator fromStreamAndShard(String streamName, String shardId)
        {
            ShardIterator newInst = null;
            String[] comps = shardId.split("_");
            if (streamName.equals(comps[0]) && comps[1].matches("[0-9]+")) {
                newInst = new ShardIterator(comps[0], Integer.parseInt(comps[1]), 0);
            }

            return newInst;
        }

        public static ShardIterator fromString(String input)
        {
            ShardIterator newInst = null;
            String[] comps = input.split("_");
            if (comps.length == 3) {
                if (comps[1].matches("[0-9]+") && comps[2].matches("[0-9]+")) {
                    newInst = new ShardIterator(comps[0], Integer.parseInt(comps[1]), Integer.parseInt(comps[2]));
                }
            }

            return newInst;
        }
    }

    public MockKinesisClient()
    {
        super();
    }

    protected InternalStream getStream(String name)
    {
        InternalStream foundStream = null;
        for (InternalStream stream : this.streams) {
            if (stream.getStreamName().equals(name)) {
                foundStream = stream;
                break;
            }
        }
        return foundStream;
    }

    protected ArrayList<Shard> getShards(InternalStream theStream)
    {
        ArrayList<Shard> externalList = new ArrayList<Shard>();
        for (InternalShard intshard : theStream.getShards()) {
            externalList.add(intshard);
        }

        return externalList;
    }

    protected ArrayList<Shard> getShards(InternalStream theStream, String fromShardId)
    {
        ArrayList<Shard> externalList = new ArrayList<Shard>();
        for (InternalShard intshard : theStream.getShardsFrom(fromShardId)) {
            externalList.add(intshard);
        }

        return externalList;
    }

    /** Clears everything, including all stream and shard definitions. */
    public void clearAll()
    {
        this.streams.clear();
    }

    /** Clears records from shards but leaves stream and shard structure in place. */
    public void clearRecords()
    {
        for (InternalStream stream : this.streams) {
            stream.clearRecords();
        }
    }

    @Override
    public void setEndpoint(String s) throws IllegalArgumentException
    {
        this.endpoint = s;
    }

    @Override
    public void setRegion(Region region) throws IllegalArgumentException
    {
        this.region = region;
    }

    @Override
    public void addTagsToStream(AddTagsToStreamRequest addTagsToStreamRequest) throws AmazonServiceException, AmazonClientException
    {
        throw new UnsupportedOperationException("MockKinesisClient doesn't support this.");
    }

    @Override
    public PutRecordResult putRecord(PutRecordRequest putRecordRequest) throws AmazonServiceException, AmazonClientException
    {
        // Setup method to add a new record:
        InternalStream theStream = this.getStream(putRecordRequest.getStreamName());
        if (theStream != null) {
            PutRecordResult result = theStream.putRecord(putRecordRequest.getData(), putRecordRequest.getPartitionKey());
            return result;
        }
        else {
            throw new AmazonClientException("This stream does not exist!");
        }
    }

    @Override
    public void createStream(CreateStreamRequest createStreamRequest) throws AmazonServiceException, AmazonClientException
    {
        // Setup method to create a new stream:
        InternalStream stream = new InternalStream(createStreamRequest.getStreamName(), createStreamRequest.getShardCount(), true);
        this.streams.add(stream);
        return;
    }

    @Override
    public void deleteStream(DeleteStreamRequest deleteStreamRequest) throws AmazonServiceException, AmazonClientException
    {
        throw new UnsupportedOperationException("MockKinesisClient doesn't support this.");
    }

    @Override
    public void mergeShards(MergeShardsRequest mergeShardsRequest) throws AmazonServiceException, AmazonClientException
    {
        throw new UnsupportedOperationException("MockKinesisClient doesn't support this.");
    }

    @Override
    public PutRecordsResult putRecords(PutRecordsRequest putRecordsRequest) throws AmazonServiceException, AmazonClientException
    {
        // Setup method to add a batch of new records:
        InternalStream theStream = this.getStream(putRecordsRequest.getStreamName());
        if (theStream != null) {
            PutRecordsResult result = new PutRecordsResult();
            ArrayList<PutRecordsResultEntry> resultList = new ArrayList<PutRecordsResultEntry>();
            for (PutRecordsRequestEntry entry : putRecordsRequest.getRecords()) {
                PutRecordResult putResult = theStream.putRecord(entry.getData(), entry.getPartitionKey());
                resultList.add((new PutRecordsResultEntry()).withShardId(putResult.getShardId()).withSequenceNumber(putResult.getSequenceNumber()));
            }

            result.setRecords(resultList);
            return result;
        }
        else {
            throw new AmazonClientException("This stream does not exist!");
        }
    }

    @Override
    public DescribeStreamResult describeStream(DescribeStreamRequest describeStreamRequest) throws AmazonServiceException, AmazonClientException
    {
        InternalStream theStream = this.getStream(describeStreamRequest.getStreamName());
        if (theStream != null) {
            StreamDescription desc = new StreamDescription();
            desc = desc.withStreamName(theStream.getStreamName()).withStreamStatus(theStream.getStreamStatus()).withStreamARN(theStream.getStreamARN());

            if (describeStreamRequest.getExclusiveStartShardId() == null || describeStreamRequest.getExclusiveStartShardId().isEmpty()) {
                desc.setShards(this.getShards(theStream));
                desc.setHasMoreShards(false);
            }
            else {
                // Filter from given shard Id, or may not have any more
                String startId = describeStreamRequest.getExclusiveStartShardId();
                desc.setShards(this.getShards(theStream, startId));
                desc.setHasMoreShards(false);
            }

            DescribeStreamResult result = new DescribeStreamResult();
            result = result.withStreamDescription(desc);
            return result;
        }
        else {
            throw new AmazonClientException("This stream does not exist!");
        }
    }

    @Override
    public GetShardIteratorResult getShardIterator(GetShardIteratorRequest getShardIteratorRequest) throws AmazonServiceException, AmazonClientException
    {
        ShardIterator iter = ShardIterator.fromStreamAndShard(getShardIteratorRequest.getStreamName(), getShardIteratorRequest.getShardId());
        if (iter != null) {
            InternalStream theStream = this.getStream(iter.streamId);
            if (theStream != null) {
                String seqAsString = getShardIteratorRequest.getStartingSequenceNumber();
                if (seqAsString != null && !seqAsString.isEmpty()) {
                    int sequence = Integer.parseInt(seqAsString);
                    iter.recordIndex = sequence;
                }
                else {
                    iter.recordIndex = 0;
                }

                GetShardIteratorResult result = new GetShardIteratorResult();
                return result.withShardIterator(iter.makeString());
            }
            else {
                throw new AmazonClientException("Unknown stream or bad shard iterator!");
            }
        }
        else {
            throw new AmazonClientException("Bad stream or shard iterator!");
        }
    }

    @Override
    public ListTagsForStreamResult listTagsForStream(ListTagsForStreamRequest listTagsForStreamRequest) throws AmazonServiceException, AmazonClientException
    {
        return null;
    }

    @Override
    public GetRecordsResult getRecords(GetRecordsRequest getRecordsRequest) throws AmazonServiceException, AmazonClientException
    {
        ShardIterator iter = ShardIterator.fromString(getRecordsRequest.getShardIterator());
        if (iter == null) {
            throw new AmazonClientException("Bad shard iterator.");
        }

        GetRecordsResult result = null;
        InternalStream stream = this.getStream(iter.streamId);
        if (stream != null) {
            InternalShard shard = stream.getShards().get(iter.shardIndex);

            if (iter.recordIndex == 0) {
                result = new GetRecordsResult();
                result.setRecords(shard.getRecords()); // TODO: getting all for now
                result.setNextShardIterator(null); // TODO: for now getting all of them
            }
            else {
                result = new GetRecordsResult();
                result.setRecords(new ArrayList<Record>()); // empty
                result.setNextShardIterator(null);
            }
        }
        else {
            throw new AmazonClientException("Unknown stream or bad shard iterator.");
        }

        return result;
    }

    @Override
    public void splitShard(SplitShardRequest splitShardRequest) throws AmazonServiceException, AmazonClientException
    {
        throw new UnsupportedOperationException("MockKinesisClient doesn't support this.");
    }

    @Override
    public void removeTagsFromStream(RemoveTagsFromStreamRequest removeTagsFromStreamRequest) throws AmazonServiceException, AmazonClientException
    {
        return;
    }

    @Override
    public ListStreamsResult listStreams(ListStreamsRequest listStreamsRequest) throws AmazonServiceException, AmazonClientException
    {
        return null;
    }

    @Override
    public ListStreamsResult listStreams() throws AmazonServiceException, AmazonClientException
    {
        return null;
    }

    @Override
    public PutRecordResult putRecord(String s, ByteBuffer byteBuffer, String s1) throws AmazonServiceException, AmazonClientException
    {
        throw new UnsupportedOperationException("MockKinesisClient doesn't support this.");
    }

    @Override
    public PutRecordResult putRecord(String s, ByteBuffer byteBuffer, String s1, String s2) throws AmazonServiceException, AmazonClientException
    {
        throw new UnsupportedOperationException("MockKinesisClient doesn't support this.");
    }

    @Override
    public void createStream(String s, Integer integer) throws AmazonServiceException, AmazonClientException
    {
        this.createStream((new CreateStreamRequest()).withStreamName(s).withShardCount(integer));
    }

    @Override
    public void deleteStream(String s) throws AmazonServiceException, AmazonClientException
    {
        throw new UnsupportedOperationException("MockKinesisClient doesn't support this.");
    }

    @Override
    public void mergeShards(String s, String s1, String s2) throws AmazonServiceException, AmazonClientException
    {
        throw new UnsupportedOperationException("MockKinesisClient doesn't support this.");
    }

    @Override
    public DescribeStreamResult describeStream(String s) throws AmazonServiceException, AmazonClientException
    {
        return null;
    }

    @Override
    public DescribeStreamResult describeStream(String s, String s1) throws AmazonServiceException, AmazonClientException
    {
        return null;
    }

    @Override
    public DescribeStreamResult describeStream(String s, Integer integer, String s1) throws AmazonServiceException, AmazonClientException
    {
        return null;
    }

    @Override
    public GetShardIteratorResult getShardIterator(String s, String s1, String s2) throws AmazonServiceException, AmazonClientException
    {
        return null;
    }

    @Override
    public GetShardIteratorResult getShardIterator(String s, String s1, String s2, String s3) throws AmazonServiceException, AmazonClientException
    {
        return null;
    }

    @Override
    public void splitShard(String s, String s1, String s2) throws AmazonServiceException, AmazonClientException
    {
        throw new UnsupportedOperationException("MockKinesisClient doesn't support this.");
    }

    @Override
    public ListStreamsResult listStreams(String s) throws AmazonServiceException, AmazonClientException
    {
        return null;
    }

    @Override
    public ListStreamsResult listStreams(Integer integer, String s) throws AmazonServiceException, AmazonClientException
    {
        return null;
    }

    @Override
    public void shutdown()
    {
        return; // Nothing to shutdown here
    }

    @Override
    public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest amazonWebServiceRequest)
    {
        return null;
    }
}
