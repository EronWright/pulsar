/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.service.eventtime;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Generate watermarks based on persistent watermark information.
 */
public class WatermarkGeneratorImpl implements WatermarkGenerator {

    private final Lock lock = new ReentrantLock();
    private final PulsarService pulsar;
    private final PersistentTopic topic;
    private final ManagedCursor managedCursor;
    private final WatermarkGeneratorListener listener;

    // Max number of producers for which to track the watermark
    private final int maxNumberOfProducers;

    // Map that contains the highest watermark that has been sent by each producer.
    @VisibleForTesting
    final ConcurrentOpenHashMap<String, Long> watermarkPerProducer = new ConcurrentOpenHashMap<>(16, 1);

    @VisibleForTesting
    final ConcurrentOpenHashMap<String, Long> watermarkPerProducerTxn = new ConcurrentOpenHashMap<>(16, 1);

    private Long watermark;

    /**
     * The reference position for the current clock time.
     */
    private PositionImpl position;

    public WatermarkGeneratorImpl(PulsarService pulsar, PersistentTopic topic, ManagedCursor watermarkCursor, WatermarkGeneratorListener listener) {
        this.pulsar = pulsar;
        this.topic = topic;
        this.managedCursor = watermarkCursor;
        this.listener = listener;
        this.maxNumberOfProducers = pulsar.getConfiguration().getBrokerWatermarkingMaxNumberOfProducers();
    }

    @VisibleForTesting
    ManagedCursor getManagedCursor() {
        return managedCursor;
    }

    @Override
    public String getTopic() {
        return this.topic.getName();
    }

    @Override
    public synchronized Long getWatermark() {
        return this.watermark;
    }

    private void updateWatermark() {
        Long newWatermark = watermarkPerProducer.values().stream().min(Long::compareTo).orElse(null);
        if (newWatermark != null && (this.watermark == null || this.watermark < newWatermark)) {
            this.watermark = newWatermark;
            listener.watermarkUpdated(this, newWatermark);
        }
    }

    private synchronized CompletableFuture<Void> recover(PositionImpl position) {
        // Load the watermarks from the snapshot in the cursor properties
        managedCursor.getProperties().forEach((key, value) -> {
            if (txnKeyPattern.matcher(key).matches()) {
                watermarkPerProducerTxn.put(key, value);
            } else {
                watermarkPerProducer.put(key, value);
            }
        });

        // Replay all the entries and apply all the watermark information
        this.position = position;
        log.info("[{}] Replaying {} entries for watermarking", topic.getName(), managedCursor.getNumberOfEntries());
        managedCursor.rewind();
        CompletableFuture<Void> future = new CompletableFuture<>();
        advanceToPosition(future);
        return future;
    }

    @Override
    public synchronized CompletableFuture<Void> seek(Position position) {
        PositionImpl positionImpl = (PositionImpl) position;

        // validate that the clock position is advancing
        PositionImpl readPosition = (PositionImpl) managedCursor.getReadPosition();
        if (readPosition.compareTo(positionImpl) > 0) {
            throw new IllegalArgumentException("clock position may not move backward");
        }

        this.position = positionImpl;
        CompletableFuture<Void> future = new CompletableFuture<>();
        pulsar.getExecutor().execute(() -> advanceToPosition(future));
        return future;
    }

    /**
     * Recover the clock information
     * @param future
     *            future to trigger when the replay is complete
     */
    private void advanceToPosition(CompletableFuture<Void> future) {
        managedCursor.asyncReadEntries(100, new AsyncCallbacks.ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                synchronized (WatermarkGeneratorImpl.this) {
                    PositionImpl setPosition = WatermarkGeneratorImpl.this.position;
                    for (Entry entry : entries) {
                        PositionImpl position = (PositionImpl) entry.getPosition();
                        if (position.compareTo(setPosition) < 0) {
                            handleEntry(entry);
                        }
                        entry.release();
                    }

                    PositionImpl readPosition = (PositionImpl) managedCursor.getReadPosition();
                    if (readPosition.compareTo(setPosition) > 0) {
                        // the cursor has read too far ahead, bring it back
                        managedCursor.seek(setPosition);
                    }

                    if (managedCursor.hasMoreEntries() && shouldRead()) {
                        // Read next batch of entries
                        pulsar.getExecutor().execute(() -> advanceToPosition(future));
                    } else {
                        // Done replaying

                        future.complete(null);
                    }
                }
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null);
    }

    private void handleEntry(Entry entry) {
        ByteBuf messageMetadataAndPayload = entry.getDataBuffer();
        PulsarApi.MessageMetadata md = Commands.parseMessageMetadata(messageMetadataAndPayload);
        if (Markers.isWatermarkUpdateMarker(md)) {
            handleWatermarkUpdateMarker(md);
        } else if (Markers.isTxnCommitMarker(md)) {
            handleTxnCommitMarker(entry, md);
        } else if (Markers.isTxnAbortMarker(md)) {
            handleTxnAbortMarker(entry, md);
        }
        md.recycle();
    }

    private void handleWatermarkUpdateMarker(PulsarApi.MessageMetadata md) {
        String producerName = md.getProducerName();
        if (md.hasTxnidMostBits() && md.hasTxnidLeastBits()) {
            // the watermark is in a transaction; apply it later.
            String key = keyForProducerAndTxn(producerName, md.getTxnidMostBits(), md.getTxnidLeastBits());
            Long txnWatermark = watermarkPerProducerTxn.get(key);
            if (txnWatermark == null || txnWatermark < md.getEventTime()) {
                watermarkPerProducerTxn.put(key, md.getEventTime());
            }
        } else {
            // apply the watermark to the clock state.
            Long watermark = watermarkPerProducer.get(producerName);
            if (watermark == null || watermark < md.getEventTime()) {
                watermarkPerProducer.put(producerName, md.getEventTime());
                updateWatermark();
            }
        }
    }

    private void processTxnWatermarks(long txnidMostBits, long txnidLeastBits, BiConsumer<String, Long> callback) {
        watermarkPerProducerTxn.keys().forEach(key -> {
            Matcher match = txnKeyPattern.matcher(key);
            if (match.matches()) {
                String producerName = match.group("producerName");
                long m = Long.parseLong(match.group("txnidMostBits"));
                long l = Long.parseLong(match.group("txnidLeastBits"));
                if (m == txnidMostBits && l == txnidLeastBits) {
                    callback.accept(producerName, watermarkPerProducerTxn.get(key));
                    watermarkPerProducerTxn.remove(key);
                }
            }
        });
    }

    private void handleTxnCommitMarker(Entry entry, PulsarApi.MessageMetadata md) {
        processTxnWatermarks(md.getTxnidMostBits(), md.getTxnidLeastBits(), (producerName, txnWatermark) -> {
            Long watermark = watermarkPerProducer.get(producerName);
            if (watermark == null || watermark < txnWatermark) {
                watermarkPerProducer.put(producerName, txnWatermark);
                updateWatermark();
            }
        });
    }

    private void handleTxnAbortMarker(Entry entry, PulsarApi.MessageMetadata md) {
        processTxnWatermarks(md.getTxnidMostBits(), md.getTxnidLeastBits(), (producerName, txnWatermark) -> {
            // do nothing
        });
    }

    private static final Pattern txnKeyPattern = Pattern.compile("^txn:(?<producerName>[^:]+):(?<txnidMostBits>\\d+):(?<txnidLeastBits>\\d+)$");

    private static String keyForProducerAndTxn(String producerName, long txnidMostBits, long txnidLeastBits) {
        return "txn:" + producerName + ":" + txnidMostBits + ":" + txnidLeastBits;
    }

    private boolean shouldRead() {
        // read until the set position
        PositionImpl readPosition = (PositionImpl) managedCursor.getReadPosition();
        return readPosition.compareTo(this.position) <= 0;
    }

    private static final Logger log = LoggerFactory.getLogger(WatermarkGeneratorImpl.class);

}
