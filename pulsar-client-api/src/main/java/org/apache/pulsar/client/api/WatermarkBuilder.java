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
package org.apache.pulsar.client.api;

import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Watermark builder that constructs a watermark to be published through a producer.
 *
 * <p>Usage example:
 * <pre><code>
 * producer.newWatermark().eventTime(timestamp).send();
 * </code></pre>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface WatermarkBuilder extends Serializable {

    /**
     * Send a watermark synchronously.
     *
     * <p>This method will block until the watermark is successfully published and returns the
     * {@link WatermarkId} consisting of the message ids assigned by the broker(s) to the published message(s).
     *
     * <p>Example:
     *
     * <pre>{@code
     * WatermarkId watermarkId = producer.newWatermark()
     *                  .eventTime(timestamp)
     *                  .send();
     * System.out.println("Published watermark: " + watermarkId);
     * }</pre>
     *
     * @return the {@link WatermarkId}.
     */
    WatermarkId send() throws PulsarClientException;

    /**
     * Send a watermark asynchronously.
     *
     * <p>This method returns a future that can be used to track the completion of the send operation and yields the
     * {@link WatermarkId} consisting of the message ids assigned by the broker(s) to the published message(s).
     *
     * <p>Example:
     *
     * <pre>
     * <code>producer.newWatermark()
     *                  .eventTime(timestamp)
     *                  .sendAsync().thenAccept(watermarkId -> {
     *    System.out.println("Published watermark: " + watermarkId);
     * }).exceptionally(e -> {
     *    System.out.println("Failed to publish " + e);
     *    return null;
     * });</code>
     * </pre>
     *
     * <p>When the producer queue is full, by default this method will complete the future with an exception
     * {@link PulsarClientException.ProducerQueueIsFullError}
     *
     * <p>See {@link ProducerBuilder#maxPendingMessages(int)} to configure the producer queue size and
     * {@link ProducerBuilder#blockIfQueueFull(boolean)} to change the blocking behavior.
     *
     * @return a future that can be used to track when the watermark will have been safely persisted.
     */
    CompletableFuture<WatermarkId> sendAsync();

    /**
     * Set the event time for a given watermark.
     *
     * @return the watermark builder instance
     */
    WatermarkBuilder eventTime(long timestamp);

    /**
     * Specify a custom sequence id for the watermark being published.
     *
     * <p>The sequence id can be used for deduplication purposes and it needs to follow these rules:
     * <ol>
     * <li><code>sequenceId >= 0</code>
     * <li>Sequence id for a watermark needs to be greater than sequence id for earlier watermarks:
     * <code>sequenceId(N+1) > sequenceId(N)</code>
     * <li>It's not necessary for sequence ids to be consecutive. There can be holes between watermarks. Eg. the
     * <code>sequenceId</code> could represent an offset or a cumulative size.
     * </ol>
     *
     * @param sequenceId
     *            the sequence id to assign to the current watermark
     * @return the watermark builder instance
     */
    WatermarkBuilder sequenceId(long sequenceId);

    /**
     * Override the geo-replication clusters for this watermark.
     *
     * @param clusters the list of clusters.
     * @return the watermark builder instance
     */
    WatermarkBuilder replicationClusters(List<String> clusters);

    /**
     * Disable geo-replication for this watermark.
     *
     * @return the watermark builder instance
     */
    WatermarkBuilder disableReplication();

    /**
     * Configure the {@link WatermarkBuilder} from a config map, as an alternative compared
     * to call the individual builder methods.
     *
     * <p>Example:
     *
     * <pre>{@code
     * Map<String, Object> conf = new HashMap<>();
     * conf.put("eventTime", System.currentTimeMillis());
     *
     * producer.newWatermark()
     *             .loadConf(conf)
     *             .send();
     * }</pre>
     *
     * <p>The available options are:
     * <table border="1">
     *  <tr>
     *    <th>Constant</th>
     *    <th>Name</th>
     *    <th>Type</th>
     *    <th>Doc</th>
     *  </tr>
     *  <tr>
     *    <td>{@link #CONF_EVENT_TIME}</td>
     *    <td>{@code eventTime}</td>
     *    <td>{@code long}</td>
     *    <td>{@link #eventTime(long)}</td>
     *  </tr>
     *  <tr>
     *    <td>{@link #CONF_SEQUENCE_ID}</td>
     *    <td>{@code sequenceId}</td>
     *    <td>{@code long}</td>
     *    <td>{@link #sequenceId(long)}</td>
     *  </tr>
     *  <tr>
     *    <td>{@link #CONF_REPLICATION_CLUSTERS}</td>
     *    <td>{@code replicationClusters}</td>
     *    <td>{@code List<String>}</td>
     *    <td>{@link #replicationClusters(List)}</td>
     *  </tr>
     *  <tr>
     *    <td>{@link #CONF_DISABLE_REPLICATION}</td>
     *    <td>{@code disableReplication}</td>
     *    <td>{@code boolean}</td>
     *    <td>{@link #disableReplication()}</td>
     *  </tr>
     * </table>
     *
     * @param config a map with the configuration options for the watermark
     * @return the watermark builder instance
     */
    WatermarkBuilder loadConf(Map<String, Object> config);

    String CONF_EVENT_TIME = "eventTime";
    String CONF_SEQUENCE_ID = "sequenceId";
    String CONF_REPLICATION_CLUSTERS = "replicationClusters";
    String CONF_DISABLE_REPLICATION = "disableReplication";
}
