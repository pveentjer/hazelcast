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
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.impl.packetdispatcher.PacketDispatcher;

import java.nio.ByteBuffer;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.SHORT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Packet.OFFSET_FLAGS;
import static com.hazelcast.nio.Packet.OFFSET_SIZE;
import static com.hazelcast.nio.Packet.PACKET_HEADER_SIZE;

/**
 * The {@link ReadHandler} for member to member communication.
 *
 * It reads as many packets from the src ByteBuffer as possible, and each of the Packets is send to the {@link PacketDispatcher}.
 *
 * @see PacketDispatcher
 * @see WriteHandlerImpl
 */
public class MemberReadHandler implements ReadHandler {

    public static final boolean BIG_ENDIAN = true;

    protected final TcpIpConnection connection;
    protected byte[] packet;

    private final byte[] header = new byte[PACKET_HEADER_SIZE];
    private final PacketDispatcher packetDispatcher;
    private final Counter normalPacketsRead;
    private final Counter priorityPacketsRead;

    public MemberReadHandler(TcpIpConnection connection, PacketDispatcher packetDispatcher) {
        this.connection = connection;
        this.packetDispatcher = packetDispatcher;
        SocketReader socketReader = connection.getSocketReader();
        this.normalPacketsRead = socketReader.getNormalFramesReadCounter();
        this.priorityPacketsRead = socketReader.getPriorityFramesReadCounter();
    }

    @Override
    public void onRead(ByteBuffer src) throws Exception {
        int offset = 0;
        int size;
        while (src.hasRemaining()) {
            if (packet == null) {
                if (src.remaining() < PACKET_HEADER_SIZE) {
                    return;
                }

                // we read the header
                src.get(header);

                // we extract the size
                size = Bits.readInt(header, OFFSET_SIZE, BIG_ENDIAN);
                offset = header.length;

                // we create the packet: the length of the array is the size of the payload, the packet-header and the id
                // of the connection is written at the end so we can look it up.
                packet = new byte[size + PACKET_HEADER_SIZE + INT_SIZE_IN_BYTES];
                System.arraycopy(header, 0, packet, 0, header.length);

                // at the end we write the id of the connection
                Bits.writeInt(packet, packet.length - 1 - Bits.INT_SIZE_IN_BYTES, connection.getConnectionId(), BIG_ENDIAN);
            }

            src.get(packet);

            boolean complete = false;//packet.readFrom(src);
            if (complete) {
                handlePacket(packet);
                packet = null;
            } else {
                break;
            }
        }
    }

    protected void handlePacket(byte[] packet) {
        short flags = Bits.readShort(packet, OFFSET_FLAGS, BIG_ENDIAN);

        if ((flags & Packet.FLAG_URGENT) != 0) {
            priorityPacketsRead.inc();
        } else {
            normalPacketsRead.inc();
        }

        packetDispatcher.dispatch(packet);
    }
}
