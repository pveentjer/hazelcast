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

package com.hazelcast.instance;

import com.hazelcast.cluster.Joiner;
import com.hazelcast.internal.networking.BufferingOutboundHandlerImpl;
import com.hazelcast.internal.networking.ChannelInboundHandler;
import com.hazelcast.internal.networking.ChannelOutboundHandler;
import com.hazelcast.internal.networking.IOThreadingModel;
import com.hazelcast.internal.networking.ProtocolBasedFactory;
import com.hazelcast.internal.networking.SocketConnection;
import com.hazelcast.internal.networking.nonblocking.NonBlockingIOThreadingModel;
import com.hazelcast.logging.LoggingServiceImpl;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.NodeIOService;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.nio.tcp.TcpIpConnectionManager;
import com.hazelcast.spi.annotation.PrivateApi;

import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;

@PrivateApi
public class DefaultNodeContext implements NodeContext {

    @Override
    public NodeExtension createNodeExtension(Node node) {
        return NodeExtensionFactory.create(node);
    }

    @Override
    public AddressPicker createAddressPicker(Node node) {
        return new DefaultAddressPicker(node);
    }

    @Override
    public Joiner createJoiner(Node node) {
        return node.createJoiner();
    }

    @Override
    public ConnectionManager createConnectionManager(Node node, ServerSocketChannel serverSocketChannel) {
        NodeIOService ioService = new NodeIOService(node, node.nodeEngine);
        IOThreadingModel ioThreadingModel = createTcpIpConnectionThreadingModel(node, ioService);

        return new TcpIpConnectionManager(
                ioService,
                serverSocketChannel,
                node.loggingService,
                node.nodeEngine.getMetricsRegistry(),
                ioThreadingModel);
    }

    private IOThreadingModel createTcpIpConnectionThreadingModel(Node node, final NodeIOService ioService) {
        boolean spinning = Boolean.getBoolean("hazelcast.io.spinning");
        LoggingServiceImpl loggingService = node.loggingService;

//        SocketWriterInitializerImpl socketWriterInitializer
//                = new SocketWriterInitializerImpl(loggingService.getLogger(SocketWriterInitializerImpl.class));
//        SocketReaderInitializerImpl socketReaderInitializer
//                = new SocketReaderInitializerImpl(loggingService.getLogger(SocketReaderInitializerImpl.class));
//
//        if (spinning) {
//            return new SpinningIOThreadingModel(
//                    loggingService,
//                    node.getHazelcastThreadGroup(),
//                    ioService.getIoOutOfMemoryHandler(),
//                    socketWriterInitializer,
//                    socketReaderInitializer);
//        } else {
        return new NonBlockingIOThreadingModel(
                loggingService,
                node.nodeEngine.getMetricsRegistry(),
                node.getHazelcastThreadGroup(),
                ioService.getIoOutOfMemoryHandler(),
                ioService.getInputThreadCount(),
                ioService.getOutputThreadCount(),
                ioService.getBalancerIntervalSeconds(),
                new ProtocolBasedFactory<ByteBuffer>() {
                    @Override
                    public ByteBuffer create(SocketConnection connection) {
                        return ByteBuffer.allocate(ioService.getSocketReceiveBufferSize());
                    }
                },
                new ProtocolBasedFactory<ChannelInboundHandler>() {
                    @Override
                    public ChannelInboundHandler create(SocketConnection connection) {
                        return ioService.createReadHandler((TcpIpConnection) connection);
                    }
                },
                new ProtocolBasedFactory<ByteBuffer>() {
                    @Override
                    public ByteBuffer create(SocketConnection connection) {
                        return ByteBuffer.allocate(ioService.getSocketSendBufferSize());
                    }
                },
                new ProtocolBasedFactory<ChannelOutboundHandler>() {
                    @Override
                    public ChannelOutboundHandler create(SocketConnection connection) {
                        ChannelOutboundHandler outboundHandler =  ioService.createOutboundHandler((TcpIpConnection) connection);
                        return new BufferingOutboundHandlerImpl(outboundHandler);
                    }
                });
        // );
        //}
    }
}
