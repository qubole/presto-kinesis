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

import java.util.ArrayList;
import java.util.List;

import javax.inject.Named;

import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.Shard;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

/**
 *
 * Split data chunk from kinesis Stream to multiple small chunks for parallelization and distribution to multiple Presto workers.
 * By default, each shard of Kinesis Stream forms one Kinesis Split
 *
 */
public class KinesisSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final KinesisHandleResolver handleResolver;
    private final KinesisClientManager clientManager;

    @Inject
    public  KinesisSplitManager(@Named("connectorId") String connectorId,
            KinesisHandleResolver handleResolver,
            KinesisClientManager clientManager)
    {
        this.connectorId = connectorId;
        this.handleResolver = handleResolver;
        this.clientManager = clientManager;
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout)
    {
        KinesisTableLayoutHandle kinesislayout = handleResolver.convertLayout(layout);
        KinesisTableHandle kinesisTableHandle = kinesislayout.getTable();

        DescribeStreamRequest describeStreamRequest = clientManager.getDescribeStreamRequest();
        describeStreamRequest.setStreamName(kinesisTableHandle.getStreamName());

        String exclusiveStartShardId = null;
        describeStreamRequest.setExclusiveStartShardId(exclusiveStartShardId);
        DescribeStreamResult describeStreamResult = clientManager.getClient().describeStream(describeStreamRequest);

        String streamStatus = describeStreamResult.getStreamDescription().getStreamStatus();
        if (!streamStatus.equals("ACTIVE") && !streamStatus.equals("UPDATING")) {
            throw new ResourceNotFoundException("Stream not Active");
        }

        // Collect shards from Kinesis
        List<Shard> shards = new ArrayList<>();
        do {
            shards.addAll(describeStreamResult.getStreamDescription().getShards());

            if (describeStreamResult.getStreamDescription().getHasMoreShards() && (shards.size() > 0)) {
                exclusiveStartShardId = shards.get(shards.size() - 1).getShardId();
            }
            else {
                exclusiveStartShardId = null;
            }

        } while (exclusiveStartShardId != null);

        // TODO: verify shard structure and use of connector session
        ImmutableList.Builder<ConnectorSplit> builder = ImmutableList.builder();
        for (Shard shard : shards) {
            KinesisSplit split = new KinesisSplit(connectorId,
                    session,
                    kinesisTableHandle.getStreamName(),
                    kinesisTableHandle.getMessageDataFormat(),
                    shard.getShardId(),
                    shard.getSequenceNumberRange().getStartingSequenceNumber(),
                    shard.getSequenceNumberRange().getEndingSequenceNumber());
            builder.add(split);
        }

        return new FixedSplitSource(connectorId, builder.build());
    }
}
