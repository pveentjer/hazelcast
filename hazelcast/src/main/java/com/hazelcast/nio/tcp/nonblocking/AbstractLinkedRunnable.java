package com.hazelcast.nio.tcp.nonblocking;

public abstract class AbstractLinkedRunnable implements LinkedRunnable {
    private LinkedRunnable next;

    @Override
    public LinkedRunnable getNext() {
        return next;
    }

    @Override
    public void setNext(LinkedRunnable next) {
        this.next = next;
    }

}
