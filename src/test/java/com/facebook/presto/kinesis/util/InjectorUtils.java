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

import com.facebook.presto.kinesis.KinesisConnectorModule;
import com.facebook.presto.kinesis.KinesisStreamDescription;
import com.facebook.presto.kinesis.KinesisTableDescriptionSupplier;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.base.Throwables;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Duplicate some of the injector logic so that we can create
 * objects to test with.
 *
 * Created by derekbennett on 6/17/16.
 */
public class InjectorUtils
{
    private InjectorUtils() {}

    /**
     * Make an injector in a way similar to the connection factory, so it's possible to
     * get an instance of any of the beans.
     *
     * @param config
     * @return
     */
    public static Injector makeInjector(Map<String, String> config)
    {
        try {
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new KinesisConnectorModule(), // Try to narrow this down!
                    new Module()
                    {
                        @Override
                        public void configure(Binder binder)
                        {
                            binder.bindConstant().annotatedWith(Names.named("connectorId")).to("kinesis-connector");
                            binder.bind(TypeManager.class).toInstance(new TypeRegistry());
                            binder.bind(KinesisTableDescriptionSupplier.class).in(Scopes.SINGLETON);
                            binder.bind(new TypeLiteral<Supplier<Map<SchemaTableName, KinesisStreamDescription>>>() {}).to(KinesisTableDescriptionSupplier.class).in(Scopes.SINGLETON);
                        }
                    }
            );

            Injector injector = app.strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            return injector;
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
