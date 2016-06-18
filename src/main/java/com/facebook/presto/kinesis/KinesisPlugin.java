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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.inject.Inject;

import io.airlift.log.Logger;

import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.annotations.VisibleForTesting;
import java.util.function.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 *
 * Kinesis version of Presto Plugin interface. This class calls getServices method to create KinesisConnectorFactory
 * and KinesisConnector during server start,
 */
public class KinesisPlugin
        implements Plugin
{
    private static final Logger log = Logger.get(KinesisPlugin.class);

    private TypeManager typeManager;
    private Optional<Supplier<Map<SchemaTableName, KinesisStreamDescription>>> tableDescriptionSupplier = Optional.empty();
    private Map<String, String> optionalConfig = ImmutableMap.of();

    @Override
    public synchronized void setOptionalConfig(Map<String, String> optionalConfig)
    {
        this.optionalConfig = ImmutableMap.copyOf(checkNotNull(optionalConfig, "optionalConfig is null"));
    }

    @Inject
    public synchronized void setTypeManager(TypeManager typeManager)
    {
        // Note: this is done by the PluginManager when loading (not the injector of this plug in!)
        log.info("Injecting type manager into KinesisPlugin");
        this.typeManager = checkNotNull(typeManager, "typeManager is null");
    }

    @VisibleForTesting
    public synchronized void setTableDescriptionSupplier(Supplier<Map<SchemaTableName, KinesisStreamDescription>> tableDescriptionSupplier)
    {
        this.tableDescriptionSupplier = Optional.of(checkNotNull(tableDescriptionSupplier, "tableDescriptionSupplier is null"));
    }

    @Override
    public synchronized <T> List<T> getServices(Class<T> type)
    {
        if (type == ConnectorFactory.class) {
            log.info("Creating connector factory.");
            return ImmutableList.of(type.cast(new KinesisConnectorFactory(typeManager, tableDescriptionSupplier, optionalConfig)));
        }
        return ImmutableList.of();
    }
}
