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

import com.facebook.presto.kinesis.util.InjectorUtils;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNotNull;

/**
 * Unit test for the TableDescriptionSupplier
 */
public class TestKinesisTableDescriptionSupplier
{
    @Test
    public void testTableDefinition()
    {
        // Create dependent objects
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("kinesis.table-description-dir", "etc/kinesis")
                .put("kinesis.table-names", "prod.test_table")
                .put("kinesis.default-schema", "kinesis")
                .put("kinesis.hide-internal-columns", "false")
                .build();

        Injector inj = InjectorUtils.makeInjector(properties);
        assertNotNull(inj);

        // Get the supplier
        KinesisTableDescriptionSupplier supplier = inj.getInstance(KinesisTableDescriptionSupplier.class);
        assertNotNull(inj);

        // Read table definition and verify
        Map<SchemaTableName, KinesisStreamDescription> readMap = supplier.get();
        assertTrue(!readMap.isEmpty());

        SchemaTableName tblName = new SchemaTableName("prod", "test_table");
        KinesisStreamDescription desc = readMap.get(tblName);

        assertNotNull(desc);
        assertEquals(desc.getSchemaName(), "prod");
        assertEquals(desc.getTableName(), "test_table");
        assertEquals(desc.getStreamName(), "test_kinesis_stream");
        assertNotNull(desc.getMessage());
    }
}
