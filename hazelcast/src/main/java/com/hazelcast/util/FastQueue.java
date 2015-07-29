package com.hazelcast.util;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * Single consumer, multi producer variable length queue implementation.
 * <p/>
 * The fast queue is a blocking implementation.
 *
 * @param <E>
 */
public final class FastQueue<E> extends AbstractQueue<E> implements BlockingQueue<E> {
    private final static Node BLOCKED = new Node();

    private final Thread owningThread;
    private final AtomicReference<Node> head = new AtomicReference<Node>();
    private Object[] array;
    private int index = -1;

    public FastQueue(Thread owningThread) {
        if (owningThread == null) {
            throw new IllegalArgumentException("owningThread can't be null");
        }
        this.owningThread = owningThread;
        this.array = new Object[512];
    }

    @Override
    public void put(E e) throws InterruptedException {
        throw new UnsupportedOperationException();

    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException();

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
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public E peek() {
        throw new UnsupportedOperationException();
    }

    public void clear() {
        head.set(null);
    }

    public boolean offer(E value) {
        if (value == null) {
            throw new IllegalArgumentException("value can't be null");
        }

        AtomicReference<Node> head = this.head;
        Node newHead = new Node();
        newHead.value = value;

        for (; ; ) {
            Node oldHead = head.get();
            if (oldHead == null || oldHead == BLOCKED) {
                newHead.next = null;
                newHead.size = 1;
            } else {
                newHead.next = oldHead;
                newHead.size = oldHead.size + 1;
            }

            if (!head.compareAndSet(oldHead, newHead)) {
                continue;
            }

            if (oldHead == BLOCKED) {
                LockSupport.unpark(owningThread);
            }

            return true;
        }
    }

    public E take() throws InterruptedException {
        E item = removeLocal();
        if (item == null) {
            takeAll();
            index = 0;
        }

        return removeLocal();
    }

    public E poll() {
        E item = removeLocal();

        if (item == null) {
            if (!pollAll()) {
                return null;
            }

            index = 0;
        }

        return removeLocal();
    }

    private E removeLocal() {
        if (index == -1) {
            return null;
        }

        E item = (E) array[index];
        if (item == null) {
            index = 1;
            return null;
        }
        index++;
        return item;
    }

    public void takeAll() throws InterruptedException {
        AtomicReference<Node> head = this.head;
        for (; ; ) {
            Node currentHead = head.get();

            if (currentHead == null) {
                // there is nothing to be take, so lets block.
                if (!head.compareAndSet(null, BLOCKED)) {
                    continue;
                }
                LockSupport.park();

                if (owningThread.isInterrupted()) {
                    head.compareAndSet(BLOCKED, null);
                    throw new InterruptedException();
                }

            } else {
                if (!head.compareAndSet(currentHead, null)) {
                    continue;
                }

                copyToDrain(currentHead);
            }
        }
    }

    public boolean pollAll() {
        AtomicReference<Node> head = this.head;
        for (; ; ) {
            Node headNode = head.get();
            if (headNode == null) {
                return false;
            }

            if (head.compareAndSet(headNode, null)) {
                copyToDrain(headNode);
                return true;
            }
        }
    }

    private Object[] copyToDrain(Node head) {
        int size = head.size;

        Object[] drain = this.array;
        if (size > drain.length) {
            drain = new Object[head.size * 2];
            this.array = drain;
        }

        for (int index = size - 1; index >= 0; index--) {
            drain[index] = head.value;
            head = head.next;
        }
        return drain;
    }

    public int size() {
        Node h = head.get();
        return h == null ? 0 : h.size;
    }

    public boolean isEmpty() {
        return head.get() == null;
    }

    private static final class Node<E> {
        Node next;
        E value;
        int size;
    }
}