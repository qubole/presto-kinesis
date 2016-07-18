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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import com.qubole.presto.kinesis.decoder.KinesisDecoderRegistry;
import com.qubole.presto.kinesis.decoder.KinesisFieldDecoder;
import com.qubole.presto.kinesis.decoder.KinesisRowDecoder;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

public class KinesisRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final KinesisHandleResolver handleResolver;
    private final KinesisClientProvider clientManager;
    private final KinesisDecoderRegistry registry;
    private final KinesisConnectorConfig kinesisConnectorConfig;

    @Inject
    public  KinesisRecordSetProvider(KinesisDecoderRegistry registry,
            KinesisHandleResolver handleResolver,
            KinesisClientProvider clientManager,
            KinesisConnectorConfig kinesisConnectorConfig)
    {
        this.registry = checkNotNull(registry, "registry is null");
        this.handleResolver = checkNotNull(handleResolver, "handleResolver is null");
        this.clientManager = checkNotNull(clientManager, "clientManager is null");
        this.kinesisConnectorConfig = checkNotNull(kinesisConnectorConfig, "kinesisConnectorConfig is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
                                  ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        KinesisSplit kinesisSplit = handleResolver.convertSplit(split);

        ImmutableList.Builder<KinesisColumnHandle> handleBuilder = ImmutableList.builder();
        ImmutableMap.Builder<KinesisColumnHandle, KinesisFieldDecoder<?>> messageFieldDecoderBuilder = ImmutableMap.builder();

        KinesisRowDecoder messageDecoder = registry.getRowDecoder(kinesisSplit.getMessageDataFormat());

        for (ColumnHandle handle : columns) {
            KinesisColumnHandle columnHandle = handleResolver.convertColumnHandle(handle);
            handleBuilder.add(columnHandle);

            if (!columnHandle.isInternal()) {
                KinesisFieldDecoder<?> fieldDecoder = registry.getFieldDecoder(kinesisSplit.getMessageDataFormat(),
                        columnHandle.getType().getJavaType(),
                        columnHandle.getDataFormat());

                messageFieldDecoderBuilder.put(columnHandle, fieldDecoder);
            }
        }

        ImmutableList<KinesisColumnHandle> handles = handleBuilder.build();
        ImmutableMap<KinesisColumnHandle, KinesisFieldDecoder<?>> messageFieldDecoders = messageFieldDecoderBuilder.build();

        return new KinesisRecordSet(kinesisSplit, session, clientManager, handles, messageDecoder, messageFieldDecoders, kinesisConnectorConfig);
    }
}
