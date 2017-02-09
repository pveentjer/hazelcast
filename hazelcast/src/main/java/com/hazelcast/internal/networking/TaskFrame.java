package com.hazelcast.internal.networking;

import com.hazelcast.nio.OutboundFrame;

public class TaskFrame implements OutboundFrame {
    private final Runnable task;

    public TaskFrame(Runnable task) {
        this.task = task;
    }

    public Runnable getTask() {
        return task;
    }

    @Override
    public boolean isUrgent() {
        return true;
    }
}
