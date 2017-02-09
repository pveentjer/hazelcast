package com.hazelcast.nio.tcp;

import java.io.IOException;
import java.nio.channels.SocketChannel;

/**
 * Created by alarmnummer on 2/9/17.
 */
public interface SocketHandshake {
    boolean complete(SocketChannel channel) throws IOException;

    String getProtocol();
}
