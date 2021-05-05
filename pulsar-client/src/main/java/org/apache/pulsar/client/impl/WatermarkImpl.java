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
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;

import java.nio.ByteBuffer;

public class WatermarkImpl {
    private static final ByteBuffer EMPTY_CONTENT = ByteBuffer.allocate(0);

    private final MessageMetadata.Builder msgMetadataBuilder;

    private WatermarkImpl(MessageMetadata.Builder msgMetadataBuilder) {
        this.msgMetadataBuilder = msgMetadataBuilder;
    }

    // Constructor for out-going message
    public static WatermarkImpl create(MessageMetadata.Builder msgMetadataBuilder) {
        return new WatermarkImpl(msgMetadataBuilder);
    }

    MessageImpl<?> createMessage(Producer<?> producer) {
        return MessageImpl.create(msgMetadataBuilder.clone(), EMPTY_CONTENT, Schema.BYTES);
    }
}
