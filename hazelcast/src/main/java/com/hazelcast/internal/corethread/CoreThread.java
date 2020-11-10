package com.hazelcast.internal.corethread;

import com.hazelcast.internal.util.executor.HazelcastManagedThread;

/**
 * A CoreThread reads requests from the socket, processes them and then sends the responses over the socket.
 *
 * It is the combination of an input-thread, operation-thread and output-thread.
 */
public class CoreThread extends HazelcastManagedThread {
    private final Runnable task;
    private final int id;

    public CoreThread(String name, Runnable task, int id) {
        super(name);
        this.task = task;
        this.id = id;
    }

    public int getThreadId(){
        return id;
    }

    @Override
    protected void executeRun() {
        task.run();
    }
}
