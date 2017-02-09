package com.hazelcast.nio.tcp;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static com.hazelcast.util.StringUtil.bytesToString;


class UnsecuredTcpIpConnectionHandshake implements TcpIpConnectionHandshake {

    private final ByteBuffer protocolBuffer = ByteBuffer.allocate(3);
    private String protocol;

    @Override
    public boolean complete(SocketChannel channel) throws IOException {
        int readBytes = channel.read(protocolBuffer);

        if (readBytes == -1) {
            throw new EOFException("Could not read protocol type!");
        }

        if (protocolBuffer.hasRemaining()) {
            return false;
        }

        // buffer bytes are complete.
        protocolBuffer.flip();
        this.protocol = bytesToString(protocolBuffer.array());
        return true;
    }

    @Override
    public String getProtocol() {
        return protocol;
    }
}
