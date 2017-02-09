package com.hazelcast.nio.tcp;

import java.io.IOException;
import java.nio.channels.SocketChannel;

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
 * - pulled out TcpIpConnectionHandshakeFactory
 * - integrated the factories in node.
 */
public interface TcpIpConnectionHandshake {

    /**
     * Tries to complete the handshake by reading the bytes from the SocketChannel. If all the protocol information is retrieved,
     * the handshake is complete.
     *
     * @param channel the SocketChannel to read from
     * @return
     * @throws IOException
     */
    boolean complete(SocketChannel channel) throws IOException;

    /**
     * @return the protocol. If no protocol has yet been read, null is returned.
     */
    String getProtocol();
}
