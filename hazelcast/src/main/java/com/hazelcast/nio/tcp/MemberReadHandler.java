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

import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.impl.operationservice.impl.AsyncResponseHandler;
import com.hazelcast.spi.impl.packetdispatcher.PacketDispatcher;
import com.hazelcast.spi.impl.packetdispatcher.impl.PacketDispatcherImpl;

import java.nio.ByteBuffer;

import static com.hazelcast.nio.Packet.FLAG_OP;
import static com.hazelcast.nio.Packet.FLAG_URGENT;

/**
 * The {@link ReadHandler} for member to member communication.
 * <p>
 * It reads as many packets from the src ByteBuffer as possible, and each of the Packets is send to the {@link PacketDispatcher}.
 *
 * @see PacketDispatcher
 * @see MemberWriteHandler
 */
public class MemberReadHandler implements ReadHandler {

    protected final TcpIpConnection connection;
    private final AsyncResponseHandler asyncResponseHandler;
    protected Packet packet;

    private final PacketDispatcherImpl packetDispatcher;
    private final Counter normalPacketsRead;
    private final Counter priorityPacketsRead;
    private final Packet[] responses = new Packet[Integer.getInteger("batchlength",10)];
    private int packetsRead;

    public MemberReadHandler(TcpIpConnection connection, PacketDispatcher packetDispatcher) {
        this.connection = connection;
        this.packetDispatcher = (PacketDispatcherImpl) packetDispatcher;
        SocketReader socketReader = connection.getSocketReader();
        this.normalPacketsRead = socketReader.getNormalFramesReadCounter();
        this.priorityPacketsRead = socketReader.getPriorityFramesReadCounter();
        this.asyncResponseHandler = (AsyncResponseHandler) ((PacketDispatcherImpl) packetDispatcher).responseHandler;
    }

    @Override
    public void onRead(ByteBuffer src) throws Exception {
        int responsesLength = 0;

        while (src.hasRemaining()) {
            if (packet == null) {
                packet = new Packet();
                packet.setConn(connection);
            }

            boolean complete = packet.readFrom(src);
            if (!complete) {
                break;
            }

            packetsRead++;
            if (packet.isFlagSet(FLAG_URGENT)) {
                priorityPacketsRead.inc();
            } else {
                normalPacketsRead.inc();
            }

            if (packetsRead > 100 && packet.isFlagSet(FLAG_OP)) {
                responses[responsesLength] = packet;
                if (responsesLength == responses.length) {
                    asyncResponseHandler.handle(responses, responsesLength+1);
                    responsesLength = 0;
                } else {
                    responsesLength++;
                }
            } else {
                packetDispatcher.dispatch(packet);
            }
            packet = null;
        }

        if (responsesLength > 0) {
            asyncResponseHandler.handle(responses, responsesLength);

//            if (packetsRead > 10100) {
//                packetsRead = 101;
//                System.out.println("responses.length:" + responsesLength);
//            }
        }
    }

//
//    @Override
//    public void onRead(ByteBuffer src) throws Exception {
//        while (src.hasRemaining()) {
//            if (packet == null) {
//                packet = new Packet();
//            }
//            boolean complete = packet.readFrom(src);
//            if (!complete) {
//                break;
//            }
//
//            if (packet.isFlagSet(FLAG_URGENT)) {
//                priorityPacketsRead.inc();
//            } else {
//                normalPacketsRead.inc();
//            }
//
//            packet.setConn(connection);
//
//            packetDispatcher.dispatch(packet);
//
//            packet = null;
//        }
//    }
}
