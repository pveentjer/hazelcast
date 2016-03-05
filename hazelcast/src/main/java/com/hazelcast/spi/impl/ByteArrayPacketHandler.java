package com.hazelcast.spi.impl;

public interface ByteArrayPacketHandler {

    /**
     * Signals the PacketHandler that there is a packet to be handled.
     *
     * @param packet the response packet to handle
     */
    void handle(byte[] packet) throws Exception;
}
