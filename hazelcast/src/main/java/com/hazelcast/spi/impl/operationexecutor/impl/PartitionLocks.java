package com.hazelcast.spi.impl.operationexecutor.impl;

import com.hazelcast.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.util.concurrent.IdleStrategy;

import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.hazelcast.nio.Bits.CACHE_LINE_LENGTH;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * The locks for the partitions.
 *
 * The locks are stored in an array where padding is applied to prevent false sharing.
 */
public class PartitionLocks {
    private static final long IDLE_MAX_SPINS = 20;
    private static final long IDLE_MAX_YIELDS = 50;
    private static final long IDLE_MIN_PARK_NS = NANOSECONDS.toNanos(1);
    private static final long IDLE_MAX_PARK_NS = MICROSECONDS.toNanos(100);

    // Strategy used when the partition is locked.
    private static final IdleStrategy IDLE_STRATEGY
            = new BackoffIdleStrategy(IDLE_MAX_SPINS, IDLE_MAX_YIELDS, IDLE_MIN_PARK_NS, IDLE_MAX_PARK_NS);

    private final AtomicReferenceArray<Thread> locks;

    public PartitionLocks(int size) {
        locks = new AtomicReferenceArray<Thread>(size * CACHE_LINE_LENGTH);
    }

    public void unlock(int partitionId) {
        locks.set(partitionId * CACHE_LINE_LENGTH, null);
    }

    public boolean tryLock(int partitionId, Thread owner) {
        return locks.compareAndSet(partitionId * CACHE_LINE_LENGTH, null, owner);
    }

    public Thread getOwner(int partitionId) {
        return locks.get(partitionId * CACHE_LINE_LENGTH);
    }

    public void lock(int partitionId, Thread owner) {
        long iteration = 0;
        long startMs = System.currentTimeMillis();
        for (; ; ) {
            if (getOwner(partitionId) == owner) {
                throw new RuntimeException("Reentrant lock acquire detected by: " + owner);
            } else if (tryLock(partitionId, owner)) {
                return;
            }

            iteration++;
            IDLE_STRATEGY.idle(iteration);

            // hack
            if (System.currentTimeMillis() > startMs + SECONDS.toMillis(30)) {
                throw new RuntimeException("Waiting too long for lock");
            }
        }
    }
}
