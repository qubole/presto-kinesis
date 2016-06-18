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

import com.facebook.presto.spi.ConnectorHandleResolver;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.airlift.log.Logger;

import java.util.Map;
import java.util.Optional;

import javax.inject.Inject;

import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.TypeManager;
import java.util.function.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.Module;
import com.google.inject.name.Names;

/**
 *
 * This factory class creates the KinesisConnector during server start and binds all the dependency
 * by calling create() method.
 */
public class KinesisConnectorFactory
        implements ConnectorFactory
{
    private static final Logger log = Logger.get(KinesisConnectorFactory.class);

    private TypeManager typeManager;
    private Optional<Supplier<Map<SchemaTableName, KinesisStreamDescription>>> tableDescriptionSupplier = Optional.empty();
    private Map<String, String> optionalConfig = ImmutableMap.of();
    private KinesisHandleResolver handleResolver;

    KinesisConnectorFactory(TypeManager typeManager,
            Optional<Supplier<Map<SchemaTableName, KinesisStreamDescription>>> tableDescriptionSupplier,
            Map<String, String> optionalConfig)
    {
        this.typeManager = checkNotNull(typeManager, "typeManager is null");
        this.tableDescriptionSupplier = checkNotNull(tableDescriptionSupplier, "tableDescriptionSupplier is null");
        this.optionalConfig = checkNotNull(optionalConfig, "optionalConfig is null");
    }

    @Override
    public String getName()
    {
        return "kinesis";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        // TODO note: this was moved here from KinesisConnector in prior version
        return this.handleResolver;
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config)
    {
        log.info("In connector factory create method.");
        checkNotNull(connectorId, "connectorId is null");
        checkNotNull(config, "config is null");

        try {
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new KinesisConnectorModule(),
                    new Module()
                    {
                        @Override
                        public void configure(Binder binder)
                        {
                            binder.bindConstant().annotatedWith(Names.named("connectorId")).to(connectorId);
                            binder.bind(TypeManager.class).toInstance(typeManager);

                            if (tableDescriptionSupplier.isPresent()) {
                                binder.bind(new TypeLiteral<Supplier<Map<SchemaTableName, KinesisStreamDescription>>>() {}).toInstance(tableDescriptionSupplier.get());
                            }
                            else {
                                binder.bind(new TypeLiteral<Supplier<Map<SchemaTableName, KinesisStreamDescription>>>() {}).to(KinesisTableDescriptionSupplier.class).in(Scopes.SINGLETON);
                            }
                        }
                    }
                );

            Injector injector = app.strictConfig()
                        .doNotInitializeLogging()
                        .setRequiredConfigurationProperties(config)
                        .setOptionalConfigurationProperties(optionalConfig)
                        .initialize();

            injector.injectMembers(this);

            log.info("Done with injector.  Returning the connector itself.");
            return injector.getInstance(KinesisConnector.class);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Inject
    public synchronized void setHandleResolver(KinesisHandleResolver handleResolver)
    {
        // Should be injected here upon create call above
        log.info("Injecting handle resolver into connector factory, baby!");
        this.handleResolver = checkNotNull(handleResolver, "handleResolver is null");
    }
}
