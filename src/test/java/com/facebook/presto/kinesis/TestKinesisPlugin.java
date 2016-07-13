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

import java.util.List;

import com.facebook.presto.kinesis.util.TestUtils;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableMap;

import static com.facebook.presto.spi.transaction.IsolationLevel.READ_COMMITTED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Test that the plug in API is satisfied and all of the required objects can be created.
 *
 * This will not make any calls to AWS, it merely checks that all of the Plug in SPI
 * objects are in place.
 */
public class TestKinesisPlugin
{
    @Test
    public ConnectorFactory testConnectorExists()
    {
        KinesisPlugin plugin = TestUtils.createPluginInstance();

        // Create factory manually to double check everything is done right
        List<ConnectorFactory> factories = plugin.getServices(ConnectorFactory.class);
        assertNotNull(factories);
        assertEquals(factories.size(), 1);
        ConnectorFactory factory = factories.get(0);
        assertNotNull(factory);
        return factory;
    }

    @Parameters({
            "kinesis.awsAccessKey",
            "kinesis.awsSecretKey"
    })
    @Test
    public void testSpinUp(String awsAccessKey, String awsSecretKey)
    {
        ConnectorFactory factory = testConnectorExists();
        // Important: this has to be created before we setup the injector in the factory:
        assertNotNull(factory.getHandleResolver());

        Connector c = factory.create("kinesis.test-connector", ImmutableMap.<String, String>builder()
                .put("kinesis.hide-internal-columns", "false")
                .put("kinesis.access-key", TestUtils.noneToBlank(awsAccessKey))
                .put("kinesis.secret-key", TestUtils.noneToBlank(awsSecretKey))
                .build());
        assertNotNull(c);

        // Verify that the key objects have been created on the connector
        assertNotNull(c.getRecordSetProvider());
        assertNotNull(c.getSplitManager());
        ConnectorMetadata md = c.getMetadata(KinesisTransactionHandle.INSTANCE);
        assertNotNull(md);

        ConnectorTransactionHandle handle = c.beginTransaction(READ_COMMITTED, true);
        assertTrue(handle != null && handle instanceof KinesisTransactionHandle);
    }
}
