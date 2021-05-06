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
package org.apache.pulsar.client.impl;

import org.apache.pulsar.common.api.proto.PulsarMarkers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

/**
 * Unit test of {@link WatermarkBuilderImpl}.
 */
public class WatermarkBuilderImplTest {

    @Mock
    protected ProducerBase producerBase;

    @Test
    public void testSendAsync() {
        producerBase = mock(ProducerBase.class);
        ArgumentCaptor<WatermarkImpl> watermark = ArgumentCaptor.forClass(WatermarkImpl.class);
        CompletableFuture send = new CompletableFuture();
        when(producerBase.internalWatermarkWithTxnAsync(watermark.capture(), isNull())).thenReturn(send);

        WatermarkBuilderImpl watermarkBuilder = new WatermarkBuilderImpl(producerBase);
        watermarkBuilder.eventTime(1L);
        watermarkBuilder.sendAsync();

        WatermarkImpl actual = watermark.getValue();
        assertEquals(actual.msgMetadataBuilder.getMarkerType(), PulsarMarkers.MarkerType.W_UPDATE_VALUE);
    }
}
