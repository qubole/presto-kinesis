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
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.RemoveTagsFromStreamRequest;
import com.amazonaws.services.kinesis.model.SplitShardRequest;

import java.nio.ByteBuffer;

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

    public MockKinesisClient()
    {
        super();
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
        throw new UnsupportedOperationException("MockKinesisClient doesn't support this.");
    }

    @Override
    public void createStream(CreateStreamRequest createStreamRequest) throws AmazonServiceException, AmazonClientException
    {
        throw new UnsupportedOperationException("MockKinesisClient doesn't support this.");
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
        throw new UnsupportedOperationException("MockKinesisClient doesn't support this.");
    }

    @Override
    public DescribeStreamResult describeStream(DescribeStreamRequest describeStreamRequest) throws AmazonServiceException, AmazonClientException
    {
        return null;
    }

    @Override
    public GetShardIteratorResult getShardIterator(GetShardIteratorRequest getShardIteratorRequest) throws AmazonServiceException, AmazonClientException
    {
        return null;
    }

    @Override
    public ListTagsForStreamResult listTagsForStream(ListTagsForStreamRequest listTagsForStreamRequest) throws AmazonServiceException, AmazonClientException
    {
        return null;
    }

    @Override
    public GetRecordsResult getRecords(GetRecordsRequest getRecordsRequest) throws AmazonServiceException, AmazonClientException
    {
        return null;
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
        throw new UnsupportedOperationException("MockKinesisClient doesn't support this.");
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
