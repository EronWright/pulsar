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
package org.apache.pulsar.client.api;

import org.apache.pulsar.client.impl.*;
import org.testng.annotations.*;

import static org.testng.Assert.assertEquals;

public class WatermarkIdTest {

    private static final MessageId M1 = new MessageIdImpl(1, 2, 3);
    private static final MessageId M2 = new BatchMessageIdImpl(0, 2, 3, 4);

    @Test
    public void toStringTest() {
        WatermarkId wId;
        wId = new WatermarkIdImpl(new MessageId[]{});
        assertEquals(wId.toString(), "[]");
        wId = new WatermarkIdImpl(new MessageId[]{M1});
        assertEquals(wId.toString(), "[1:2:3]");
        wId = new WatermarkIdImpl(new MessageId[]{M1, M2});
        assertEquals(wId.toString(), "[1:2:3,0:2:3:4]");
    }

    @Test
    public void serializationTest() throws Exception {

        WatermarkId expected = new WatermarkIdImpl(new MessageId[]{M1, M2});
        byte[] data = expected.toByteArray();
        WatermarkId actual = WatermarkIdImpl.fromByteArray(data);
        assertEquals(actual, expected);
    }
}
