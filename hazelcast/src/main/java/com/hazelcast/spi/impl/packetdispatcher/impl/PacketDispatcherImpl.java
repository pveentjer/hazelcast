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

package com.hazelcast.spi.impl.packetdispatcher.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.impl.ByteArrayPacketHandler;
import com.hazelcast.spi.impl.PacketHandler;
import com.hazelcast.spi.impl.packetdispatcher.PacketDispatcher;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.inspectOutputMemoryError;
import static com.hazelcast.nio.Packet.FLAG_BIND;
import static com.hazelcast.nio.Packet.FLAG_EVENT;
import static com.hazelcast.nio.Packet.FLAG_OP;
import static com.hazelcast.nio.Packet.FLAG_RESPONSE;
import static com.hazelcast.nio.Packet.FLAG_WAN_REPLICATION;
import static com.hazelcast.nio.Packet.OFFSET_FLAGS;

/**
 * Default {@link PacketDispatcher} implementation.
 */
public class PacketDispatcherImpl implements PacketDispatcher {

    private final ILogger logger;
    private final PacketHandler eventPacketHandler;
    private final PacketHandler wanReplicationPacketHandler;
    private final ByteArrayPacketHandler operationPacketHandler;
    private final ByteArrayPacketHandler responsePacketHandler;
    private final PacketHandler connectionPacketHandler;

    public PacketDispatcherImpl(ILogger logger,
                                ByteArrayPacketHandler operationPacketHandler,
                                ByteArrayPacketHandler responsePacketHandler,
                                PacketHandler eventPacketHandler,
                                PacketHandler wanReplicationPacketHandler,
                                PacketHandler connectionPacketHandler) {
        this.logger = logger;
        this.operationPacketHandler = operationPacketHandler;
        this.responsePacketHandler = responsePacketHandler;
        this.eventPacketHandler = eventPacketHandler;
        this.wanReplicationPacketHandler = wanReplicationPacketHandler;
        this.connectionPacketHandler = connectionPacketHandler;
    }

    @Override
    public void dispatch(byte[] packet) {
        short flags = Bits.readShort(packet, OFFSET_FLAGS, true);

        try {
            if ((flags & FLAG_OP) != 0) {
                if ((flags & FLAG_RESPONSE) != 0) {
                    responsePacketHandler.handle(packet);
                } else {
                    operationPacketHandler.handle(packet);
                }
            } else if ((flags & FLAG_EVENT) != 0) {
                Packet p = new Packet();
                p.readFrom(packet);
                eventPacketHandler.handle(p);
            } else if ((flags & FLAG_WAN_REPLICATION) != 0) {
                Packet p = new Packet();
                p.readFrom(packet);
                wanReplicationPacketHandler.handle(p);
            } else if ((flags & FLAG_BIND) != 0) {
                Packet p = new Packet();
                p.readFrom(packet);
                connectionPacketHandler.handle(p);
            } else {
                logger.severe("Unknown packet type! Header: " + flags);
            }
        } catch (Throwable t) {
            inspectOutputMemoryError(t);
            logger.severe("Failed to process packet:" + packet, t);
        }
    }
}
