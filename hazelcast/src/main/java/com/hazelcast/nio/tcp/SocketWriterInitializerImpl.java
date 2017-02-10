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

import com.hazelcast.internal.networking.SocketWriterInitializer;

public class SocketWriterInitializerImpl implements SocketWriterInitializer<TcpIpConnection> {

//    private final ILogger logger;
//
//    public SocketWriterInitializerImpl(ILogger logger) {
//        this.logger = logger;
//    }
//
//    @Override
//    public void init(TcpIpConnection connection, ChannelWriter writer, String protocol) {
//        logger.fine("Initializing ChannelWriter ChannelOutboundHandler with " + Protocols.toUserFriendlyString(protocol));
//
//        initHandler(connection, writer, protocol);
//        initOutputBuffer(connection, writer, protocol);
//    }
//
//    private void initHandler(TcpIpConnection connection, ChannelWriter writer, String protocol) {
//        ChannelOutboundHandler handler;
//        if (CLUSTER.equals(protocol)) {
//            IOService ioService = connection.getConnectionManager().getIoService();
//            handler = ioService.createOutboundHandler(connection);
//        } else if (CLIENT_BINARY_NEW.equals(protocol)) {
//            handler = new ClientMessageEncoder();
//        } else {
//            handler = new TextChannelOutboundHandler(connection);
//        }
//        writer.initWriteHandler(handler);
//    }
//
//    private void initOutputBuffer(TcpIpConnection connection, ChannelWriter writer, String protocol) {
//        IOService ioService = connection.getConnectionManager().getIoService();
//        int sizeKb = CLUSTER.equals(protocol)
//                ? ioService.getSocketSendBufferSize()
//                : ioService.getSocketClientSendBufferSize();
//        int size = KILO_BYTE * sizeKb;
//
//        ByteBuffer outputBuffer = newByteBuffer(size, ioService.isSocketBufferDirect());
//        if (CLUSTER.equals(protocol)) {
//            outputBuffer.put(stringToBytes(CLUSTER));
//        }
//
//        writer.initOutputBuffer(outputBuffer);
//
//        try {
//            connection.setSendBufferSize(size);
//        } catch (SocketException e) {
//            logger.finest("Failed to adjust TCP send buffer of " + connection + " to " + size + " B.", e);
//        }
//    }
}
