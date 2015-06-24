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

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.Duration;

import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestKinesisConnectorConfig
{
    @Parameters({
        "kinesis.awsAccessKey",
        "kinesis.awsSecretKey"
    })
    @Test
    public void testDefaults(String accessKey, String secretKey)
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(KinesisConnectorConfig.class)
                .setDefaultSchema("default")
                .setHideInternalColumns(true)
                .setTableNames("")
                .setTableDescriptionDir(new File("etc/kinesis/"))
                .setAccessKey(null)
                .setSecretKey(null)
                .setAwsRegion("us-east-1")
                .setSleepTime(new Duration(1000, TimeUnit.MILLISECONDS))
                .setFetchAttempts(3)
                .setBatchSize(10000)
                .setCheckpointEnabled(false)
                .setDynamoReadCapacity(50)
                .setDynamoWriteCapacity(10)
                .setCheckpointIntervalMS(new Duration(60000, TimeUnit.MILLISECONDS))
                .setLogicalProcessName("process1")
                .setIterationNumber(0));
    }

    @Parameters({
        "kinesis.awsAccessKey",
        "kinesis.awsSecretKey"
    })
    @Test
    public void testExplicitPropertyMappings(String accessKey, String secretKey)
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("kinesis.table-description-dir", "/var/lib/kinesis")
                .put("kinesis.table-names", "table1, table2, table3")
                .put("kinesis.default-schema", "kinesis")
                .put("kinesis.hide-internal-columns", "false")
                .put("kinesis.access-key", accessKey)
                .put("kinesis.secret-key", secretKey)
                .put("kinesis.fetch-attempts", "5")
                .put("kinesis.aws-region", "us-west-1")
                .put("kinesis.sleep-time", "100ms")
                .put("kinesis.batch-size", "9000")
                .put("kinesis.checkpoint-enabled", "true")
                .put("kinesis.dynamo-read-capacity", "100")
                .put("kinesis.dynamo-write-capacity", "20")
                .put("kinesis.checkpoint-interval-ms", "50000ms")
                .put("kinesis.checkpoint-logical-name", "process")
                .put("kinesis.iteration-number", "1")
                .build();

        KinesisConnectorConfig expected = new KinesisConnectorConfig()
                .setTableDescriptionDir(new File("/var/lib/kinesis"))
                .setTableNames("table1, table2, table3")
                .setDefaultSchema("kinesis")
                .setHideInternalColumns(false)
                .setAccessKey(accessKey)
                .setSecretKey(secretKey)
                .setAwsRegion("us-west-1")
                .setFetchAttempts(5)
                .setSleepTime(new Duration(100, TimeUnit.MILLISECONDS))
                .setBatchSize(9000)
                .setCheckpointEnabled(true)
                .setDynamoReadCapacity(100)
                .setDynamoWriteCapacity(20)
                .setCheckpointIntervalMS(new Duration(50000, TimeUnit.MILLISECONDS))
                .setLogicalProcessName("process")
                .setIterationNumber(1);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
