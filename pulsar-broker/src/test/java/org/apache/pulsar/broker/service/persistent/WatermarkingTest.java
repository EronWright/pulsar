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
package org.apache.pulsar.broker.service.persistent;

import lombok.Cleanup;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class WatermarkingTest extends ProducerConsumerBase {

    @Override
    @BeforeClass
    public void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    @AfterClass(alwaysRun = true)
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    private static final int NUM_MESSAGES = 10;

    @Test()
    public void testWatermarking()
            throws Exception {
        String topic = "testWatermarking-" + System.nanoTime();

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("consumer-1")
                .subscriptionType(SubscriptionType.Failover)
                .subscriptionMode(SubscriptionMode.NonDurable)
                .enableWatermarking(true)
                .subscribe();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .producerName("producer-1")
                .create();

        for (int i = 0; i < NUM_MESSAGES; i++) {
            producer.newMessage()
                    .value("msg-" + i)
                    .eventTime(System.currentTimeMillis())
                    .sendAsync();
        }
        producer.newWatermark().eventTime(System.currentTimeMillis()).sendAsync();
        producer.flush();

        List<Message> unacked = new ArrayList<>();
        for (int i = 0; i < NUM_MESSAGES; i++) {
            Message<String> msg = consumer.receive();
            assertNotNull(msg);
            assertEquals(msg.getValue(), "msg-" + i);

            // assert that no watermark has been received until after the messages are acked
            assertNull(consumer.getLastWatermark());
            unacked.add(msg);
        }

        // wait for the expected watermark (which should not arrive until we ack the messages)
        assertNull(consumer.getLastWatermark());
        CompletableFuture<Message<String>> watermark = consumer.receiveAsync();

        for (Message msg : unacked) {
            consumer.acknowledge(msg);
        }

        Message<String> marker = watermark.get(60, TimeUnit.SECONDS);
        assertNull(marker);
        assertNotNull(consumer.getLastWatermark(), "expected a watermark");
    }
}
