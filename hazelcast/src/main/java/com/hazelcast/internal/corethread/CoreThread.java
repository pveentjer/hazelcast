package com.hazelcast.internal.corethread;

import com.hazelcast.internal.util.executor.HazelcastManagedThread;

/**
 * A CoreThread reads requests from the socket, processes them and then sends the responses over the socket.
 *
 * It is the combination of an input-thread, operation-thread and output-thread.
 */
public class CoreThread extends HazelcastManagedThread {
    private final Runnable task;

    public CoreThread(String name, Runnable task) {
        super(name);
        this.task = task;
    }

    @Override
    protected void executeRun() {
        task.run();
    }
}
