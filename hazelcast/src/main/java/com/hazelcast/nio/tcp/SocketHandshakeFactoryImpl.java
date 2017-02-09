package com.hazelcast.nio.tcp;

public class SocketHandshakeFactoryImpl implements SocketHandshakeFactory {

    @Override
    public SocketHandshake create() {
        return new SocketHandshakeImpl();
    }
}
