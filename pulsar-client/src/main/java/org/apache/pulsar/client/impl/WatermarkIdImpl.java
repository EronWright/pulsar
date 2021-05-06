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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.WatermarkId;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageIdData;
import org.apache.pulsar.common.api.proto.PulsarApi.WatermarkIdData;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedInputStream;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedOutputStream;
import org.apache.pulsar.shaded.com.google.protobuf.v241.UninitializedMessageException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class WatermarkIdImpl implements WatermarkId {
    protected final MessageId[] messageIds;

    // Private constructor used only for json deserialization
    @SuppressWarnings("unused")
    private WatermarkIdImpl() {
        this(null);
    }

    public WatermarkIdImpl(MessageId[] messageIds) {
        this.messageIds = messageIds;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(messageIds);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof WatermarkIdImpl) {
            WatermarkIdImpl other = (WatermarkIdImpl) obj;
            return Objects.deepEquals(messageIds, other.messageIds);
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(256);
        for (MessageId mid : messageIds) {
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(mid.toString());
        }
        sb.insert(0, '[').append(']');
        return sb.toString();
    }

    // / Serialization

    public static WatermarkId fromByteArray(byte[] data) throws IOException {
        checkNotNull(data);
        ByteBufCodedInputStream inputStream = ByteBufCodedInputStream.get(Unpooled.wrappedBuffer(data, 0, data.length));
        WatermarkIdData.Builder builder = WatermarkIdData.newBuilder();

        WatermarkIdData idData;
        try {
            idData = builder.mergeFrom(inputStream, null).build();
        } catch (UninitializedMessageException e) {
            throw new IOException(e);
        }

        MessageId[] messageIds = new MessageId[idData.getMessageCount()];
        for (int i = 0; i < idData.getMessageCount(); i++) {
            MessageIdData msgIdData = idData.getMessage(i);
            MessageIdImpl messageId;
            if (msgIdData.hasBatchIndex()) {
                messageId = new BatchMessageIdImpl(msgIdData.getLedgerId(), msgIdData.getEntryId(), msgIdData.getPartition(),
                        msgIdData.getBatchIndex());
            } else {
                messageId = new MessageIdImpl(msgIdData.getLedgerId(), msgIdData.getEntryId(), msgIdData.getPartition());
            }
            messageIds[i] = messageId;
        }
        WatermarkIdImpl watermarkId = new WatermarkIdImpl(messageIds);

        inputStream.recycle();
        builder.recycle();
        idData.recycle();
        return watermarkId;
    }

    public static WatermarkIdImpl convertToWatermarkIdImpl(WatermarkId watermarkId) {
        if (watermarkId instanceof WatermarkIdImpl) {
            return (WatermarkIdImpl) watermarkId;
        }
        return null;
    }

    @Override
    public byte[] toByteArray() {
        WatermarkIdData.Builder builder = WatermarkIdData.newBuilder();
        MessageIdData.Builder msgBuilder = MessageIdData.newBuilder();
        for (MessageId msgId : messageIds) {
            MessageIdImpl msgImpl = (MessageIdImpl) msgId;
            msgBuilder.setLedgerId(msgImpl.getLedgerId());
            msgBuilder.setEntryId(msgImpl.getEntryId());
            if (msgImpl.getPartitionIndex() >= 0) {
                msgBuilder.setPartition(msgImpl.getPartitionIndex());
            }
            if (msgId instanceof BatchMessageIdImpl) {
                BatchMessageIdImpl batchMsgImpl = (BatchMessageIdImpl) msgId;
                if (batchMsgImpl.getBatchIndex() != -1) {
                    msgBuilder.setBatchIndex(batchMsgImpl.getBatchIndex());
                }
            }
            builder.addMessage(msgBuilder.build());
            msgBuilder.clear();
        }
        WatermarkIdData msgId = builder.build();

        int size = msgId.getSerializedSize();
        ByteBuf serialized = Unpooled.buffer(size, size);
        ByteBufCodedOutputStream stream = ByteBufCodedOutputStream.get(serialized);
        try {
            msgId.writeTo(stream);
        } catch (IOException e) {
            // This is in-memory serialization, should not fail
            throw new RuntimeException(e);
        }

        msgId.recycle();
        msgBuilder.recycle();
        builder.recycle();
        stream.recycle();
        return serialized.array();
    }
}
