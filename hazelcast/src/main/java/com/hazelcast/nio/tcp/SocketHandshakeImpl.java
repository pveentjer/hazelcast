package com.hazelcast.nio.tcp;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static com.hazelcast.util.StringUtil.bytesToString;

/**
 * Responsible for handling the handshake for a connection. During the handshake the protocol is determined and based on that
 * the connection will be initialized.
 *
 *
 * todo:
 * - integrate ssl
 * - set the buffer size on the socket
 * - clients
 * - closing
 * - member socket reader and the counters
 *
 * done:
 * - pulled out interface
 */
class SocketHandshakeImpl implements SocketHandshake {

    private final ByteBuffer protocolBuffer = ByteBuffer.allocate(3);
    private String protocol;

    /**
     * Tries to complete the handshake by reading the bytes from the SocketChannel. If all the protocol information is retrieved,
     * the handshake is complete.
     *
     * @param channel the SocketChannel to read from
     * @return
     * @throws IOException
     */
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

    /**
     * @return the protocol. If no protocol has yet been read, null is returned.
     */
    @Override
    public String getProtocol() {
        return protocol;
    }
}
