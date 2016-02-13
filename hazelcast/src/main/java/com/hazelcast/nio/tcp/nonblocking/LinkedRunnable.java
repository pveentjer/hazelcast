package com.hazelcast.nio.tcp.nonblocking;

public interface LinkedRunnable extends Runnable {

    LinkedRunnable getNext();

    void setNext(LinkedRunnable next);
}
