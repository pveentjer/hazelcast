package com.hazelcast.internal.corethread;

import com.hazelcast.internal.util.executor.HazelcastManagedThread;

class CoreThread extends HazelcastManagedThread {
    private final Runnable task;

    CoreThread(String name, Runnable task) {
        super(name);
        this.task = task;
    }

    @Override
    protected void executeRun() {
        task.run();
    }
}
