package com.hazelcast.internal.networking;

public interface ProtocolBasedFactory<E> {

    E create(SocketConnection connection);
}
