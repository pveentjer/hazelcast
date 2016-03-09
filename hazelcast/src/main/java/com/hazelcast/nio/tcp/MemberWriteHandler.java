/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.nio.tcp;

import com.hazelcast.nio.Packet;

import java.nio.ByteBuffer;

/**
 * A {@link WriteHandler} that for member to member communication.
 *
 * It writes {@link Packet} instances to the {@link ByteBuffer}.
 *
 * @see MemberReadHandler
 */
public class MemberWriteHandler implements WriteHandler {

    @Override
    public int onWrite(byte[] src, int offset, ByteBuffer dst) {
        int bufferRemaining = dst.remaining();
        int bytesPending = src.length - offset;

        if (bytesPending <= bufferRemaining) {
            // there is enough space in the buffer, we can write everything
            dst.put(src, offset, bytesPending);
            // we return -1 to indicate we are done.
            return -1;
        } else {
            // there is not enough space in the buffer, so lets write as much bytes as we can
            dst.put(src, offset, bufferRemaining);
            // and return the new offset so we can continue where we stopped.
            return offset + bufferRemaining;
        }
    }
}
