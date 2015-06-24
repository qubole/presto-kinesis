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

import io.airlift.configuration.Config;
import io.airlift.units.Duration;

import java.io.File;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.validation.constraints.NotNull;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;

/**
 * This Class handles all the configuration settings that is stored in /etc/catalog/kinesis.properties file
 *
 */
public class KinesisConnectorConfig
{
    /**
     * Set of tables known to this connector. For each table, a description file may be present in the catalog folder which describes columns for the given topic.
     */
    private Set<String> tableNames = ImmutableSet.of();

    /**
     * The schema name to use in the connector.
     */
    private String defaultSchema = "default";

    /**
     * Folder holding the JSON description files for Kafka topics.
     */
    private File tableDescriptionDir = new File("etc/kinesis/");

    /**
     * Whether internal columns are shown in table metadata or not. Default is no.
     */
    private boolean hideInternalColumns = true;

    /**
     * Region to be used to read stream from.
     */
    private String awsRegion = "us-east-1";

    /**
     * Defines maximum number of records to return in one call
     */
    private int batchSize = 10000;

    /**
     * Defines number of attempts to fetch records from stream until received non-empty
     */
    private int fetchAttmepts = 3;

    /**
     *  Defines sleep time (in milliseconds) for the thread which is trying to fetch the records from kinesis streams
     */
    private Duration sleepTime = new Duration(1000, TimeUnit.MILLISECONDS);

    private String accessKey = null;

    private String secretKey = null;

    private boolean checkpointEnabled = false;

    private long dynamoReadCapacity = 50L;

    private long dyanamoWriteCapacity = 10L;

    private Duration checkpointIntervalMS = new Duration(60000, TimeUnit.MILLISECONDS);

    private String logicalProcessName = "process1";

    private int iterationNumber = 0;

    @NotNull
    public File getTableDescriptionDir()
    {
        return tableDescriptionDir;
    }

    @Config("kinesis.table-description-dir")
    public KinesisConnectorConfig setTableDescriptionDir(File tableDescriptionDir)
    {
        this.tableDescriptionDir = tableDescriptionDir;
        return this;
    }

    public boolean isHideInternalColumns()
    {
        return hideInternalColumns;
    }

    @Config("kinesis.hide-internal-columns")
    public KinesisConnectorConfig setHideInternalColumns(boolean hideInternalColumns)
    {
        this.hideInternalColumns = hideInternalColumns;
        return this;
    }

    @NotNull
    public Set<String> getTableNames()
    {
        return tableNames;
    }

    @Config("kinesis.table-names")
    public KinesisConnectorConfig setTableNames(String tableNames)
    {
        this.tableNames = ImmutableSet.copyOf(Splitter.on(',').omitEmptyStrings().trimResults().split(tableNames));
        return this;
    }

    @NotNull
    public String getDefaultSchema()
    {
        return defaultSchema;
    }

    @Config("kinesis.default-schema")
    public KinesisConnectorConfig setDefaultSchema(String defaultSchema)
    {
        this.defaultSchema = defaultSchema;
        return this;
    }

    @Config("kinesis.access-key")
    public KinesisConnectorConfig setAccessKey(String accessKey)
    {
        this.accessKey = accessKey;
        return this;
    }

    public String getAccessKey()
    {
        return this.accessKey;
    }

    @Config("kinesis.secret-key")
    public KinesisConnectorConfig setSecretKey(String secretKey)
    {
        this.secretKey = secretKey;
        return this;
    }

    public String getSecretKey()
    {
        return this.secretKey;
    }

    @Config("kinesis.checkpoint-enabled")
    public KinesisConnectorConfig setCheckpointEnabled(boolean checkpointEnabled)
    {
        this.checkpointEnabled = checkpointEnabled;
        return this;
    }

    public boolean isCheckpointEnabled()
    {
        return checkpointEnabled;
    }

    @Config("kinesis.dynamo-read-capacity")
    public KinesisConnectorConfig setDynamoReadCapacity(long dynamoReadCapacity)
    {
        this.dynamoReadCapacity = dynamoReadCapacity;
        return this;
    }

    public long getDynamoReadCapacity()
    {
        return dynamoReadCapacity;
    }

    @Config("kinesis.dynamo-write-capacity")
    public KinesisConnectorConfig setDynamoWriteCapacity(long dynamoWriteCapacity)
    {
        this.dyanamoWriteCapacity = dynamoWriteCapacity;
        return this;
    }

    public long getDynamoWriteCapacity()
    {
        return dyanamoWriteCapacity;
    }

    @Config("kinesis.aws-region")
    public KinesisConnectorConfig setAwsRegion(String awsRegion)
    {
        this.awsRegion = awsRegion;
        return this;
    }

    public String getAwsRegion()
    {
        return awsRegion;
    }

    @Config("kinesis.batch-size")
    public KinesisConnectorConfig setBatchSize(int batchSize)
    {
        this.batchSize = batchSize;
        return this;
    }

    public int getBatchSize()
    {
        return this.batchSize;
    }

    @Config("kinesis.fetch-attempts")
    public KinesisConnectorConfig setFetchAttempts(int fetchAttempts)
    {
        this.fetchAttmepts = fetchAttempts;
        return this;
    }

    public int getFetchAttempts()
    {
        return this.fetchAttmepts;
    }

    @Config("kinesis.sleep-time")
    public KinesisConnectorConfig setSleepTime(Duration sleepTime)
    {
        this.sleepTime = sleepTime;
        return this;
    }

    public Duration getSleepTime()
    {
        return this.sleepTime;
    }

    @Config("kinesis.checkpoint-interval-ms")
    public KinesisConnectorConfig setCheckpointIntervalMS(Duration checkpointIntervalMS)
    {
        this.checkpointIntervalMS = checkpointIntervalMS;
        return this;
    }

    public Duration getCheckpointIntervalMS()
    {
        return checkpointIntervalMS;
    }

    @Config("kinesis.checkpoint-logical-name")
    public KinesisConnectorConfig setLogicalProcessName(String logicalPrcessName)
    {
        this.logicalProcessName = logicalPrcessName;
        return this;
    }

    public String getLogicalProcessName()
    {
        return logicalProcessName;
    }

    @Config("kinesis.iteration-number")
    public KinesisConnectorConfig setIterationNumber(int iterationNumber)
    {
        this.iterationNumber = iterationNumber;
        return this;
    }

    public int getIterationNumber()
    {
        return iterationNumber;
    }
}
