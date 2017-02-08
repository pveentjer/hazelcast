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

import com.hazelcast.internal.networking.SocketReaderInitializer;

public class SocketReaderInitializerImpl implements SocketReaderInitializer<TcpIpConnection> {

//    private final ILogger logger;
//
//    public SocketReaderInitializerImpl(ILogger logger) {
//        this.logger = logger;
//    }
//
//    @Override
//    public void init(TcpIpConnection connection, SocketReader reader, String protocol) throws IOException {
//        TcpIpConnectionManager connectionManager = connection.getConnectionManager();
//        IOService ioService = connectionManager.getIoService();
//
//        ReadHandler readHandler;
//        SocketWriter socketWriter = connection.getSocketWriter();
//        if (CLUSTER.equals(protocol)) {
//            initInputBuffer(connection, reader, ioService.getSocketReceiveBufferSize());
//            connection.setType(MEMBER);
//            socketWriter.setProtocol(CLUSTER);
//            readHandler = ioService.createReadHandler(connection);
//        } else if (CLIENT_BINARY_NEW.equals(protocol)) {
//            initInputBuffer(connection, reader, ioService.getSocketClientReceiveBufferSize());
//            socketWriter.setProtocol(CLIENT_BINARY_NEW);
//            readHandler = new ClientReadHandler(reader.getNormalFramesReadCounter(), connection, ioService);
//        } else {
//            ByteBuffer inputBuffer = initInputBuffer(connection, reader, ioService.getSocketReceiveBufferSize());
//            socketWriter.setProtocol(TEXT);
//           // inputBuffer.put(protocolBuffer.array());
//            readHandler = new TextReadHandler(connection);
//            connectionManager.incrementTextConnections();
//        }
//
//        if (readHandler == null) {
//            throw new IOException("Could not initialize ReadHandler!");
//        }
//
//        reader.initReadHandler(readHandler);
//    }
//
//    private ByteBuffer initInputBuffer(TcpIpConnection connection, SocketReader reader, int sizeKb) {
//        boolean directBuffer = connection.getConnectionManager().getIoService().isSocketBufferDirect();
//        int sizeBytes = sizeKb * KILO_BYTE;
//
//        ByteBuffer inputBuffer = newByteBuffer(sizeBytes, directBuffer);
//        reader.initInputBuffer(inputBuffer);
//
//        try {
//            connection.setReceiveBufferSize(sizeBytes);
//        } catch (SocketException e) {
//            logger.finest("Failed to adjust TCP receive buffer of " + connection + " to " + sizeBytes + " B.", e);
//        }
//
//        return inputBuffer;
//    }
}
