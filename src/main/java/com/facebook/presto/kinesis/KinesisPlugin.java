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

import static java.util.Objects.requireNonNull;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import javax.inject.Inject;

import com.google.inject.Injector;
import io.airlift.log.Logger;

import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Kinesis version of Presto Plugin interface.
 *
 * The connector manager injects the type manager and node manager, and then calls getServices
 * to get the connector factory.
 */
public class KinesisPlugin
        implements Plugin
{
    private static final Logger log = Logger.get(KinesisPlugin.class);

    private TypeManager typeManager;
    private NodeManager nodeManager;
    private Optional<Supplier<Map<SchemaTableName, KinesisStreamDescription>>> tableDescriptionSupplier = Optional.empty();
    private Map<String, String> optionalConfig = ImmutableMap.of();
    private Optional<Class<? extends KinesisClientProvider>> altProviderClass = Optional.empty();

    private KinesisConnectorFactory factory;

    @Override
    public synchronized void setOptionalConfig(Map<String, String> optionalConfig)
    {
        this.optionalConfig = ImmutableMap.copyOf(requireNonNull(optionalConfig, "optionalConfig is null"));
    }

    @Inject
    public synchronized void setTypeManager(TypeManager typeManager)
    {
        // Note: this is done by the PluginManager when loading (not the injector of this plug in!)
        log.info("Injecting type manager into KinesisPlugin");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Inject
    public synchronized void setNodeManager(NodeManager nodeManager)
    {
        log.info("Injecting node manager into KinesisPlugin");
        this.nodeManager = requireNonNull(nodeManager, "node is null");
    }

    @Override
    public synchronized <T> List<T> getServices(Class<T> type)
    {
        if (type == ConnectorFactory.class) {
            if (this.factory == null) {
                log.info("Creating connector factory.");
                this.factory = new KinesisConnectorFactory(typeManager, nodeManager, tableDescriptionSupplier, optionalConfig, altProviderClass);
            }
            return ImmutableList.of(type.cast(this.factory));
        }
        return ImmutableList.of();
    }

    @VisibleForTesting
    public synchronized void setTableDescriptionSupplier(Supplier<Map<SchemaTableName, KinesisStreamDescription>> tableDescriptionSupplier)
    {
        this.tableDescriptionSupplier = Optional.of(requireNonNull(tableDescriptionSupplier, "tableDescriptionSupplier is null"));
    }

    @VisibleForTesting
    public <T extends KinesisClientProvider> void setAltProviderClass(Class<T> aType)
    {
        // Note: this can be used for other cases besides testing but that was the original motivation
        altProviderClass = Optional.of(requireNonNull(aType, "Provider class type is null"));
    }

    @VisibleForTesting
    public synchronized Injector getInjector()
    {
        if (this.factory != null) {
            return this.factory.getInjector();
        }
        else {
            return null;
        }
    }
}
