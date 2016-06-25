package com.hazelcast.internal.util;

import com.hazelcast.util.QuickMath;
import com.hazelcast.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.util.concurrent.IdleStrategy;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.hazelcast.util.QuickMath.modPowerOfTwo;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class Ringbuffer<E> extends AbstractQueue<E> implements BlockingQueue<E> {
    private static final long IDLE_MAX_SPINS = 20;
    private static final long IDLE_MAX_YIELDS = 50;
    private static final long IDLE_MIN_PARK_NS = NANOSECONDS.toNanos(1);
    private static final long IDLE_MAX_PARK_NS = MICROSECONDS.toNanos(100);

    private static final IdleStrategy WAIT_FOR_COMMIT_IDLE_STRATEGY
            = new BackoffIdleStrategy(IDLE_MAX_SPINS, IDLE_MAX_YIELDS, IDLE_MIN_PARK_NS, IDLE_MAX_PARK_NS);

    private final static int HEAD_INDEX = 8;
    private final static int TAIL_INDEX = 16;
    public static final int PADDING = 16;

    private final AtomicReferenceArray<E> buffer;
    private final AtomicLongArray sequenceArray = new AtomicLongArray(32);
    private final int bufferLength;
    private final IdleStrategy idleStrategy;
    private Thread consumerThread;

    public Ringbuffer(int bufferLength) {
        this(null, bufferLength, null);
    }

    public Ringbuffer(Thread consumerThread, int bufferLength, IdleStrategy idleStrategy) {
        this.bufferLength = QuickMath.nextPowerOfTwo(bufferLength);
        this.buffer = new AtomicReferenceArray<E>(bufferLength * PADDING);
        this.idleStrategy = idleStrategy;
        this.consumerThread = consumerThread;
    }

    public void setConsumerThread(Thread consumerThread) {
        this.consumerThread = consumerThread;
    }

    @Override
    public boolean offer(E item) {
        long newTail = sequenceArray.incrementAndGet(TAIL_INDEX) - 1;
        int index = index(newTail);
        buffer.lazySet(index, item);
        return true;
    }

    public boolean offer(E[] items) {
        long seq = sequenceArray.getAndAdd(TAIL_INDEX, items.length);
        for (int k = 0; k < items.length; k++) {
            E item = items[k];
            if (item == null) {
                break;
            }

            items[k] = null;
            buffer.lazySet(index(seq), item);
            seq ++;
        }

        return true;
    }

    private int index(long seq) {
        return (int) modPowerOfTwo(seq, bufferLength) * PADDING;
    }

    @Override
    public E poll() {
        long currentHead = sequenceArray.get(HEAD_INDEX);
        if (currentHead == sequenceArray.get(TAIL_INDEX)) {
            return null;
        }

        return removeItem(currentHead);
    }

    @Override
    public E take() throws InterruptedException {
        long iteration = 0;

        for (; ; ) {
            long currentHead = sequenceArray.get(HEAD_INDEX);
            if (currentHead == sequenceArray.get(TAIL_INDEX)) {
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }

                iteration++;
                idleStrategy.idle(iteration);
                continue;
            }

            return removeItem(currentHead);
        }
    }

    private E removeItem(long currentHead) {
        int index = index(currentHead);

        long n = 0;
        E item;
        for (; ; ) {
            item = buffer.get(index);
            if (item != null) {
                buffer.lazySet(index, null);
                sequenceArray.lazySet(HEAD_INDEX, currentHead + 1);
                break;
            }
            n++;
            WAIT_FOR_COMMIT_IDLE_STRATEGY.idle(n);
        }

        return item;
    }

    @Override
    public void clear() {
        for (int k = 0; k < buffer.length(); k++) {
            buffer.set(k, null);
        }
    }

    @Override
    public int size() {
        return (int) (sequenceArray.get(TAIL_INDEX) - sequenceArray.get(HEAD_INDEX));
    }

    @Override
    public boolean add(E item) {
        return offer(item);
    }

    @Override
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void put(E e) throws InterruptedException {
        add(e);
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        add(e);
        return true;
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int remainingCapacity() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int drainTo(Collection<? super E> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        throw new UnsupportedOperationException();
    }

    @Override
    public E peek() {
        throw new UnsupportedOperationException();
    }
}
