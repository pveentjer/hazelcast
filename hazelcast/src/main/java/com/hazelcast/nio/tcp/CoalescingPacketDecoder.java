package com.hazelcast.nio.tcp;

import com.hazelcast.internal.networking.nio.ChannelInboundHandlerWithCounters;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.PacketIOHelper;
import com.hazelcast.spi.impl.PacketHandler;

import java.nio.ByteBuffer;

import static com.hazelcast.nio.Packet.FLAG_URGENT;

/**
 * A packet decoder that coalesces all incoming packets and in 1 go hands over
 */
public class CoalescingPacketDecoder extends ChannelInboundHandlerWithCounters {

    protected final TcpIpConnection connection;
    private final PacketHandler handler;
    private final PacketIOHelper packetReader = new PacketIOHelper();

    public CoalescingPacketDecoder(TcpIpConnection connection, PacketHandler handler) {
        this.connection = connection;
        this.handler = handler;
    }

    @Override
    public void onRead(ByteBuffer src) throws Exception {
        Packet head = null;
        while (src.hasRemaining()) {
            Packet packet = packetReader.readFrom(src);
            if (packet == null) {
                break;
            }

            packet.next = head;
            head = packet;
            packet.setConn(connection);
            if (packet.isFlagRaised(FLAG_URGENT)) {
                priorityPacketsRead.inc();
            } else {
                normalPacketsRead.inc();
            }
        }

        if (head != null) {
            handler.handle(head);
        }
    }
}
