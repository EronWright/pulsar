/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.impl;

import com.google.common.base.Preconditions;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.WatermarkBuilder;
import org.apache.pulsar.client.api.WatermarkId;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.api.proto.PulsarMarkers;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.pulsar.client.util.TypeCheckUtil.checkType;

public class WatermarkBuilderImpl implements WatermarkBuilder {

    private static final long serialVersionUID = 0L;

    private final ProducerBase<?> producer;
    private final MessageMetadata.Builder msgMetadataBuilder = MessageMetadata.newBuilder();
    private final TransactionImpl txn;

    public WatermarkBuilderImpl(ProducerBase<?> producer) {
        this(producer, null);
    }

    public WatermarkBuilderImpl(ProducerBase<?> producer,
                                TransactionImpl txn) {
        this.producer = producer;
        this.txn = txn;
    }

    private long beforeSend() {
        msgMetadataBuilder.setMarkerType(PulsarMarkers.MarkerType.W_UPDATE_VALUE);
        if (txn == null) {
            return -1L;
        }
        msgMetadataBuilder.setTxnidLeastBits(txn.getTxnIdLeastBits());
        msgMetadataBuilder.setTxnidMostBits(txn.getTxnIdMostBits());
        return -1L;
    }

    @Override
    public WatermarkId send() throws PulsarClientException {
        try {
            // enqueue the watermark to the buffer
            CompletableFuture<WatermarkId> sendFuture = sendAsync();

            if (!sendFuture.isDone()) {
                // the send request wasn't completed yet (e.g. not failing at enqueuing), then attempt to triggerFlush it out
                producer.triggerFlush();
            }

            return sendFuture.get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<WatermarkId> sendAsync() {
        beforeSend();
        WatermarkImpl watermark = WatermarkImpl.create(msgMetadataBuilder);
        return producer.internalWatermarkWithTxnAsync(watermark, txn);
    }

    @Override
    public WatermarkBuilder eventTime(long timestamp) {
        checkArgument(timestamp > 0, "Invalid timestamp : '%s'", timestamp);
        msgMetadataBuilder.setEventTime(timestamp);
        return this;
    }

    @Override
    public WatermarkBuilder sequenceId(long sequenceId) {
        checkArgument(sequenceId >= 0);
        msgMetadataBuilder.setSequenceId(sequenceId);
        return this;
    }

    @Override
    public WatermarkBuilder replicationClusters(List<String> clusters) {
        Preconditions.checkNotNull(clusters);
        msgMetadataBuilder.clearReplicateTo();
        msgMetadataBuilder.addAllReplicateTo(clusters);
        return this;
    }

    @Override
    public WatermarkBuilder disableReplication() {
        msgMetadataBuilder.clearReplicateTo();
        msgMetadataBuilder.addReplicateTo("__local__");
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public WatermarkBuilder loadConf(Map<String, Object> config) {
        config.forEach((key, value) -> {
            switch (key) {
                case CONF_EVENT_TIME:
                    this.eventTime(checkType(value, Long.class));
                    break;
                case CONF_SEQUENCE_ID:
                    this.sequenceId(checkType(value, Long.class));
                    break;
                case CONF_REPLICATION_CLUSTERS:
                    this.replicationClusters(checkType(value, List.class));
                    break;
                case CONF_DISABLE_REPLICATION:
                    boolean disableReplication = checkType(value, Boolean.class);
                    if (disableReplication) {
                        this.disableReplication();
                    }
                    break;
                default:
                    throw new RuntimeException("Invalid watermark config key '" + key + "'");
            }
        });
        return this;
    }

    MessageMetadata.Builder getMetadataBuilder() {
        return msgMetadataBuilder;
    }

    public long getPublishTime() {
        return msgMetadataBuilder.getPublishTime();
    }

}
