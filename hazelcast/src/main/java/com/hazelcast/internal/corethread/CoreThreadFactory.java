package com.hazelcast.internal.corethread;

import com.hazelcast.internal.util.ThreadAffinity;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class CoreThreadFactory implements ThreadFactory {
    private final AtomicInteger counter = new AtomicInteger();
    private final ThreadAffinity threadAffinity;

    public CoreThreadFactory(ThreadAffinity threadAffinity) {
        this.threadAffinity = threadAffinity;
    }

    @Override
    public Thread newThread(@NotNull Runnable r) {
        CoreThread thread = new CoreThread("CoreThread/" + counter.getAndIncrement(), r);
        thread.setThreadAffinity(threadAffinity);
        return thread;
    }
}
