package com.hazelcast.nio.tcp;

public class UnsecuredTcpIpConnectionHandshakeFactory implements TcpIpConnectionHandshakeFactory {

    @Override
    public TcpIpConnectionHandshake create() {
        return new UnsecuredTcpIpConnectionHandshake();
    }
}
