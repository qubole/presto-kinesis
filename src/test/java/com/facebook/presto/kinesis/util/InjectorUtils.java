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
package com.facebook.presto.kinesis.util;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.s3.AmazonS3Client;
import com.facebook.presto.kinesis.KinesisClientProvider;
import com.facebook.presto.kinesis.KinesisConnector;
import com.facebook.presto.kinesis.KinesisConnectorModule;
import com.facebook.presto.kinesis.KinesisHandleResolver;
import com.facebook.presto.kinesis.KinesisStreamDescription;
import com.facebook.presto.kinesis.KinesisTableDescriptionSupplier;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.base.Throwables;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Duplicate some of the injector logic so that we can create
 * objects to test with.
 *
 * Created by derekbennett on 6/17/16.
 */
public class InjectorUtils
{
    public static final String connectorName = "kinesis";

    private InjectorUtils() {}

    /**
     * Test implementation of KinesisClientProvider that incorporates a mock Kinesis client.
     */
    public static class KinesisTestClientManager implements KinesisClientProvider
    {
        private final AmazonKinesisClient client;
        private final AmazonDynamoDBClient dynamoDBClient;
        private final AmazonS3Client amazonS3Client;

        public KinesisTestClientManager()
        {
            this.client = new MockKinesisClient();
            this.dynamoDBClient = new AmazonDynamoDBClient();
            this.amazonS3Client = new AmazonS3Client();
        }

        @Override
        public AmazonKinesisClient getClient()
        {
            return this.client;
        }

        @Override
        public AmazonDynamoDBClient getDynamoDBClient()
        {
            return this.dynamoDBClient;
        }

        @Override
        public AmazonS3Client getS3Client()
        {
            return amazonS3Client;
        }

        @Override
        public DescribeStreamRequest getDescribeStreamRequest()
        {
            return new DescribeStreamRequest();
        }
    }

     /**
     * Make an injector in a way similar to the connection factory, so it's possible to
     * get an instance of any of the beans.
     *
     * Use the method to keep the usual KinesisTableDescriptionSupplier.
     *
     * @param config
     * @return
     */
    public static Injector makeInjector(Map<String, String> config)
    {
        return makeInjector(config, Optional.empty());
    }

    /**
     * Make an injector in a way similar to the connection factory, so it's possible to
     * get an instance of any of the beans.
     *
     * This returns an injector that sets up all of the dependent classes.  Then
     * just access them from the injector when needed.
     *
     * Use the optional streamDescriptionsOpt to replace the usual KinesisTableDescriptionSupplier
     * with a particular list of mappings in your test.
     *
     * @param config
     * @param streamDescriptionsOpt
     * @return
     */
    public static Injector makeInjector(Map<String, String> config, Optional<Map<SchemaTableName, KinesisStreamDescription>> streamDescriptionsOpt)
    {
        try {
            KinesisTestClientManager clientManager = new KinesisTestClientManager();
            KinesisConnectorModule.setAltProvider(clientManager);
            KinesisConnectorModule mainModule = new KinesisConnectorModule();

            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    mainModule, // Future: make an option to limit this to fewer objects
                    new Module()
                    {
                        @Override
                        public void configure(Binder binder)
                        {
                            binder.bindConstant().annotatedWith(Names.named("connectorId")).to("kinesis");
                            binder.bind(TypeManager.class).toInstance(new TypeRegistry());
                            binder.bind(NodeManager.class).toInstance(new InMemoryNodeManager());
                            binder.bind(KinesisHandleResolver.class).toInstance(new KinesisHandleResolver(connectorName));

                            if (streamDescriptionsOpt.isPresent()) {
                                Supplier<Map<SchemaTableName, KinesisStreamDescription>> supplier = () -> streamDescriptionsOpt.get();
                                binder.bind(new TypeLiteral<Supplier<Map<SchemaTableName, KinesisStreamDescription>>>() {}).toInstance(supplier);
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
                    .initialize();

            // Register objects for shutdown
            KinesisConnector connector = injector.getInstance(KinesisConnector.class);
            if (!streamDescriptionsOpt.isPresent()) {
                // This will shutdown related dependent objects as well:
                KinesisTableDescriptionSupplier supp = getTableDescSupplier(injector);
                connector.registerShutdownObject(supp);
            }

            return injector;
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Convenience method to get the table description supplier.
     *
     * @param inj
     * @return
     */
    public static KinesisTableDescriptionSupplier getTableDescSupplier(Injector inj)
    {
        requireNonNull(inj, "Injector is missing in getTableDescSupplier");
        Supplier<Map<SchemaTableName, KinesisStreamDescription>> supplier =
                inj.getInstance(Key.get(new TypeLiteral<Supplier<Map<SchemaTableName, KinesisStreamDescription>>>() {}));
        requireNonNull(inj, "Injector cannot find any table description supplier");
        return (KinesisTableDescriptionSupplier) supplier;
    }
}
