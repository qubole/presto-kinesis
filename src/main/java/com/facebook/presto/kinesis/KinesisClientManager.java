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

import javax.inject.Named;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.google.inject.Inject;

import io.airlift.log.Logger;

/**
 *
 * Creates and manages clients for consumer
 *
 */
public class KinesisClientManager
{
    private static final Logger log = Logger.get(KinesisClientManager.class);
    private final AmazonKinesisClient client;
    private final KinesisAwsCredentials kinesisAwsCredentials;
    private final AmazonDynamoDBClient dynamoDBClient;              // for Checkpointing

    @Inject
    KinesisClientManager(@Named("connectorId") String connectorId,
            KinesisConnectorConfig kinesisConnectorConfig)
    {
        log.info("Creating new client for Consuner");
        if (kinesisConnectorConfig.getAccessKey() == null || kinesisConnectorConfig.getSecretKey() == null) {
            this.kinesisAwsCredentials = null;
            this.client = new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain());
            this.dynamoDBClient = new AmazonDynamoDBClient(new DefaultAWSCredentialsProviderChain());
        }
        else {
            this.kinesisAwsCredentials = new KinesisAwsCredentials(kinesisConnectorConfig.getAccessKey(), kinesisConnectorConfig.getSecretKey());
            this.client = new AmazonKinesisClient(this.kinesisAwsCredentials);
            this.dynamoDBClient = new AmazonDynamoDBClient(this.kinesisAwsCredentials);
        }

        this.client.setEndpoint("kinesis." + kinesisConnectorConfig.getAwsRegion() + ".amazonaws.com");
        this.dynamoDBClient.setEndpoint("dynamodb." + kinesisConnectorConfig.getAwsRegion() + ".amazonaws.com");
    }

    public AmazonKinesisClient getClient()
    {
        return client;
    }

    public AmazonDynamoDBClient getDynamoDBClient()
    {
        return dynamoDBClient;
    }

    public DescribeStreamRequest getDescribeStreamRequest()
    {
        return new DescribeStreamRequest();
    }
}
