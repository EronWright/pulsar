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
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.*;

public class WatermarkingTest extends ProducerConsumerBase {

    @Override
    @BeforeClass
    public void setup() throws Exception {
        conf.setSystemTopicEnabled(true);
        conf.setTopicLevelPoliciesEnabled(true);
        conf.setDelayedDeliveryTickTimeMillis(1024);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    @AfterClass(alwaysRun = true)
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 60000)
    public void testWatermarking()
            throws Exception {
        String topic = "testWatermarking-" + System.nanoTime();

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("consumer")
                .subscriptionType(SubscriptionType.Failover)
                .subscribe();

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        for (int i = 0; i < 10; i++) {
            producer.newMessage()
                    .value("msg-" + i)
                    .eventTime(System.currentTimeMillis())
                    .sendAsync();
        }
        producer.newWatermark().eventTime(System.currentTimeMillis()).sendAsync();
        producer.flush();

        Message<String> msg;

        for (int i = 0; i < 10; i++) {
            msg = consumer.receive();
            assertNotNull(msg);
            System.err.printf("%s: %s\n", msg.getMessageId(), msg.getValue());
            assertEquals(msg.getValue(), "msg-" + i);
            if (i < 9) {
                // watermark could be received any time after the last message
                assertNull(consumer.getLastWatermark());
            }
        }

        // wait for the expected watermark
        msg = consumer.receiveAsync().join();
        if (msg != null) {
            System.err.printf("%s: %s\n", msg.getMessageId(), msg.getValue());
        }
        assertNull(msg, "did not expect a message: " + msg.getValue());
        assertNotNull(consumer.getLastWatermark(), "expected a watermark");
    }
}
