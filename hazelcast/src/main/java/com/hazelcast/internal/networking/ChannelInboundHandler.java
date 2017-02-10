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

package com.hazelcast.internal.networking;

import com.hazelcast.nio.tcp.PacketDecoder;
import com.hazelcast.nio.tcp.TcpIpConnection;

import java.nio.ByteBuffer;

/**
 * Reads content from a {@link ByteBuffer} and processes it. The ChannelInboundHandler is invoked by the
 * {@link ChannelReader} after it has read data from the socket.
 *
 * A typical example is that Packet instances are created from the buffered data and handing them over the the
 * {@link com.hazelcast.spi.impl.packetdispatcher.PacketDispatcher}. See {@link PacketDecoder}
 * for more information.
 *
 * Each {@link ChannelReader} will have its own {@link ChannelInboundHandler} instance. Therefor it doesn't need to be thread-safe.
 *
 * @see ChannelOutboundHandler
 * @see ChannelReader
 * @see TcpIpConnection
 * @see IOThreadingModel
 */
public interface ChannelInboundHandler {

    /**
     * A callback to indicate that data is available in the ByteBuffer to be processed.
     *
     * @param src the ByteBuffer containing the data to read. The ByteBuffer is already in reading mode and when completed,
     *            should not be converted to write-mode using clear/compact. That is a task of the {@link ChannelReader}.
     * @throws Exception if something fails while reading data from the ByteBuffer or processing the data
     *                   (e.g. when a Packet fails to get processed). When an exception is thrown, the TcpIpConnection
     *                   is closed. There is no point continuing with a potentially corrupted stream.
     */
    void read(ByteBuffer src) throws Exception;
}
