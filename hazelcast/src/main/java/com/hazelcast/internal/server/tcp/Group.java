package com.hazelcast.internal.server.tcp;

import java.util.concurrent.atomic.AtomicReferenceArray;

import static java.lang.Math.abs;

class Group {
    final AtomicReferenceArray<TcpServerConnection> connections;

    public Group(int count) {
        this.connections = new AtomicReferenceArray<>(count);
    }

    public TcpServerConnection get(int streamId) {
        return connections.get(connectionIndex(streamId));
    }

    public int connectionIndex(int streamId) {
        int connectionIndex;
        if (streamId == -1 || streamId == Integer.MIN_VALUE) {
            connectionIndex = 0;
        } else {
            connectionIndex = abs(streamId) % connections.length();
        }
        return connectionIndex;
    }
}