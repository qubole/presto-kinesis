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

import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.facebook.presto.Session;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.StandaloneQueryRunner;
import com.google.common.collect.ImmutableMap;
import com.qubole.presto.kinesis.util.EmbeddedKinesisStream;
import com.qubole.presto.kinesis.util.TestUtils;
import io.airlift.log.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Note: this is an integration test that connects to AWS Kinesis.
 *
 * Only run if you have an account setup where you can create streams and put/get records.
 * You may incur AWS charges if you run this test.  You probably want to setup an IAM
 * user for your CI server to use.
 */
@Test(singleThreaded = true)
public class TestMinimalFunctionality
{
    private static final Logger log = Logger.get(TestMinimalFunctionality.class);

    private static final Session SESSION = Session.builder(new SessionPropertyManager())
            .setIdentity(new Identity("user", Optional.empty()))
            .setSource("source")
            .setCatalog("kinesis")
            .setSchema("default")
            .setTimeZoneKey(UTC_KEY)
            .setLocale(ENGLISH)
            .build();

    private EmbeddedKinesisStream embeddedKinesisStream;
    private String streamName;
    private StandaloneQueryRunner queryRunner;

    @Parameters({
        "kinesis.awsAccessKey",
        "kinesis.awsSecretKey"
    })
    @BeforeClass
    public void start(String accessKey, String secretKey)
        throws Exception
    {
        embeddedKinesisStream = new EmbeddedKinesisStream(TestUtils.noneToBlank(accessKey), TestUtils.noneToBlank(secretKey));
    }

    @AfterClass
    public void stop()
            throws Exception
    {
        embeddedKinesisStream.close();
    }

    @Parameters({
        "kinesis.awsAccessKey",
        "kinesis.awsSecretKey"
    })
    @BeforeMethod
    public void spinUp(String accessKey, String secretKey)
            throws Exception
    {
        streamName = "test_" + UUID.randomUUID().toString().replaceAll("-", "_");
        embeddedKinesisStream.createStream(2, streamName);
        this.queryRunner = new StandaloneQueryRunner(SESSION);
        TestUtils.installKinesisPlugin(queryRunner,
                ImmutableMap.<SchemaTableName, KinesisStreamDescription>builder().
                put(TestUtils.createEmptyStreamDescription(streamName, new SchemaTableName("default", streamName))).build(),
                TestUtils.noneToBlank(accessKey), TestUtils.noneToBlank(secretKey));
    }

    private void createMessages(String streamName, int count)
            throws Exception
    {
        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
        putRecordsRequest.setStreamName(streamName);
        List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
            putRecordsRequestEntry.setData(ByteBuffer.wrap(UUID.randomUUID().toString().getBytes()));
            putRecordsRequestEntry.setPartitionKey(Long.toString(i));
            putRecordsRequestEntryList.add(putRecordsRequestEntry);
        }

        putRecordsRequest.setRecords(putRecordsRequestEntryList);
        embeddedKinesisStream.getKinesisClient().putRecords(putRecordsRequest);
    }

    @Test
    public void testStreamExists()
            throws Exception
    {
        // TODO: Was QualifiedTableName, is this OK:
        QualifiedObjectName name = new QualifiedObjectName("kinesis", "default", streamName);
        Optional<TableHandle> handle = queryRunner.getServer().getMetadata().getTableHandle(SESSION, name);
        assertTrue(handle.isPresent());
    }

    @Test
    public void testStreamHasData()
            throws Exception
    {
        MaterializedResult result = queryRunner.execute("Select count(1) from " + streamName);

        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(0)
                .build();

        assertEquals(result, expected);

        int count = 500;
        createMessages(streamName, count);

        result = queryRunner.execute("SELECT count(1) from " + streamName);

        expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(count)
                .build();

        assertEquals(result, expected);
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        embeddedKinesisStream.delteStream(streamName);
        queryRunner.close();
    }
}
