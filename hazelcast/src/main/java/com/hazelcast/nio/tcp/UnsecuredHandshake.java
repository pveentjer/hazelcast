/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static com.hazelcast.util.StringUtil.bytesToString;

/**
 * The handshake for an unsecured TcpIpConnection. It expects 3 bytes with the protocol information.
 */
public class UnsecuredHandshake implements Handshake {

    private final ByteBuffer protocolBuffer = ByteBuffer.allocate(3);
    private String protocol;

    @Override
    public boolean onRead(SocketChannel channel) throws IOException {
        int readBytes = channel.read(protocolBuffer);

        if (readBytes == -1) {
            throw new EOFException("Could not read protocol type!");
        }

        if (protocolBuffer.hasRemaining()) {
            return false;
        }

        // buffer bytes are complete.
        protocolBuffer.flip();
        this.protocol = bytesToString(protocolBuffer.array());
        return true;
    }

    @Override
    public String getProtocol() {
        return protocol;
    }
}
