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

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.Watermark;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;

import static com.google.common.base.Preconditions.checkState;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Objects;

public class WatermarkImpl implements Watermark {
    private static final ByteBuffer EMPTY_CONTENT = ByteBuffer.allocate(0);

    final MessageMetadata.Builder msgMetadataBuilder;
    final long eventTime;

    private WatermarkImpl(MessageMetadata.Builder msgMetadataBuilder, long eventTime) {
        this.msgMetadataBuilder = msgMetadataBuilder;
        this.eventTime = eventTime;
    }

    @Override
    public long getEventTime() {
        return this.eventTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WatermarkImpl watermark = (WatermarkImpl) o;
        return eventTime == watermark.eventTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventTime);
    }

    // Constructor for outgoing message
    public static WatermarkImpl create(MessageMetadata.Builder msgMetadataBuilder) {
        return new WatermarkImpl(msgMetadataBuilder, msgMetadataBuilder.getEventTime());
    }

    // Constructor for incoming message
    public static WatermarkImpl create(long eventTime) {
        return new WatermarkImpl(null, eventTime);
    }

    MessageImpl<?> createMessage(Producer<?> producer) {
        checkState(this.msgMetadataBuilder != null,
                "outgoing watermark should contain msgMetadataBuilder");
        return MessageImpl.create(msgMetadataBuilder.clone(), EMPTY_CONTENT, Schema.BYTES);
    }

    @Override
    public int compareTo(Watermark o) {
        WatermarkImpl other = (WatermarkImpl) o;
        return Long.compare(this.eventTime, other.eventTime);
    }
}

class WatermarkImplComparator implements Comparator<WatermarkImpl> {

    private static final WatermarkImplComparator INSTANCE = new WatermarkImplComparator();

    static WatermarkImplComparator getInstance() {
        return INSTANCE;
    };

    @Override
    public int compare(WatermarkImpl o1, WatermarkImpl o2) {
        if (o1 == null && o2 == null) return 0;
        if (o1 == null) return 1;
        if (o2 == null) return -1;
        return Long.compare(o1.eventTime, o2.eventTime);
    }
}
