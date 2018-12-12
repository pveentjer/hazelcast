/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.connection.nio;

import com.hazelcast.client.config.SocketOptions;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.util.ClientMessageEncoder;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.internal.networking.InitDstBuffer;
import com.hazelcast.internal.networking.InitReceiveBuffer;
import com.hazelcast.util.function.Consumer;
import com.hazelcast.util.function.Supplier;

import static com.hazelcast.client.config.SocketOptions.KILO_BYTE;
import static com.hazelcast.internal.networking.ChannelOption.DIRECT_BUF;
import static com.hazelcast.internal.networking.ChannelOption.SO_KEEPALIVE;
import static com.hazelcast.internal.networking.ChannelOption.SO_LINGER;
import static com.hazelcast.internal.networking.ChannelOption.SO_RCVBUF;
import static com.hazelcast.internal.networking.ChannelOption.SO_REUSEADDR;
import static com.hazelcast.internal.networking.ChannelOption.SO_SNDBUF;
import static com.hazelcast.internal.networking.ChannelOption.SO_TIMEOUT;
import static com.hazelcast.internal.networking.ChannelOption.TCP_NODELAY;


/**
 * Client side ChannelInitializer for connections without SSL/TLS. Client in this
 * case is a real client using client protocol etc.
 * <p>
 * It will automatically send the Client Protocol to the server and configure the
 * correct buffers/handlers.
 */
public class ClientPlainChannelInitializer implements ChannelInitializer {
    private final boolean directBuffer;
    private final SocketOptions socketOptions;
    private final Supplier<ClientAuthenticationRequestEncoder> authRequestEncoderSupplier;

    public ClientPlainChannelInitializer(SocketOptions socketOptions,
                                         boolean directBuffer,
                                         Supplier<ClientAuthenticationRequestEncoder> authRequestEncoderSupplier) {
        this.socketOptions = socketOptions;
        this.directBuffer = directBuffer;
        this.authRequestEncoderSupplier = authRequestEncoderSupplier;
    }

    @Override
    public void initChannel(Channel channel) {
        channel.options()
                .setOption(SO_SNDBUF, KILO_BYTE * socketOptions.getBufferSize())
                .setOption(SO_RCVBUF, KILO_BYTE * socketOptions.getBufferSize())
                .setOption(SO_REUSEADDR, socketOptions.isReuseAddress())
                .setOption(SO_KEEPALIVE, socketOptions.isKeepAlive())
                .setOption(SO_LINGER, socketOptions.getLingerSeconds())
                .setOption(SO_TIMEOUT, 0)
                .setOption(TCP_NODELAY, socketOptions.isTcpNoDelay())
                .setOption(DIRECT_BUF, directBuffer);

        final ClientConnection connection = (ClientConnection) channel.attributeMap().get(ClientConnection.class);

        ClientMessageDecoder decoder = new ClientMessageDecoder(connection, new Consumer<ClientMessage>() {
            @Override
            public void accept(ClientMessage message) {
                connection.handleClientMessage(message);
            }
        });
        channel.inboundPipeline().addLast(new InitReceiveBuffer(), decoder);


        try {
            channel.outboundPipeline().addLast(new InitDstBuffer());
            
            // the first thing the client does is send the client protocol bytes; so this encoder is
            // first in line. Once the bytes have been written to the buffer, this handler will remove itself.
            channel.outboundPipeline().addLast(new ClientProtocolEncoder());

            // after the client protocol bytes have been put in the buffer, the authentication request needs
            // to be put in the buffer. Once the bytes have been written, this handler will remove itself.
            channel.outboundPipeline().addLast(authRequestEncoderSupplier.get());

            // writes the actual client message.
            channel.outboundPipeline().addLast(new ClientMessageEncoder());
        }catch (RuntimeException e){
            e.printStackTrace();
        }
    }
}
