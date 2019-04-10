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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.s3.AmazonS3Client;

/**
 * Interface to a client manager that provides the AWS clients needed.
 *
 * Created by derekbennett on 6/20/16.
 */
public interface KinesisClientProvider
{
    AmazonKinesisClient getClient();

    AmazonDynamoDBClient getDynamoDBClient();

    AmazonS3Client getS3Client();

    DescribeStreamRequest getDescribeStreamRequest();
}
