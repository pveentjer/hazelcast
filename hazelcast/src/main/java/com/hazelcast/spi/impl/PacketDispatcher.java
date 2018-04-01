/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.spi.impl;

import com.hazelcast.internal.util.concurrent.MPSCQueue;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.impl.operationexecutor.OperationHostileThread;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.util.MutableInteger;
import sun.rmi.server.Dispatcher;

import java.util.concurrent.BlockingQueue;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.inspectOutOfMemoryError;
import static com.hazelcast.nio.Packet.FLAG_OP_CONTROL;
import static com.hazelcast.nio.Packet.FLAG_OP_RESPONSE;
import static com.hazelcast.spi.properties.GroupProperty.DISPATCH_THREAD_COUNT;
import static com.hazelcast.spi.properties.GroupProperty.RESPONSE_THREAD_COUNT;
import static com.hazelcast.util.EmptyStatement.ignore;
import static com.hazelcast.util.ThreadUtil.createThreadName;

/**
 * A {@link PacketHandler} that dispatches the {@link Packet} to the right service. So operations are send to the
 * {@link com.hazelcast.spi.OperationService}, events are send to the {@link com.hazelcast.spi.EventService} etc.
 */
public final class PacketDispatcher implements PacketHandler {
    private static final ThreadLocal<MutableInteger> INT_HOLDER = new ThreadLocal<MutableInteger>() {
        @Override
        protected MutableInteger initialValue() {
            return new MutableInteger();
        }
    };

    private final ILogger logger;
    private final PacketHandler eventService;
    private final PacketHandler operationExecutor;
    private final PacketHandler jetService;
    private final PacketHandler connectionManager;
    private final PacketHandler responseHandler;
    private final PacketHandler invocationMonitor;
    private final DispatchThread[] threads;

    public PacketDispatcher(ILogger logger,
                            HazelcastProperties properties,
                            PacketHandler operationExecutor,
                            PacketHandler responseHandler,
                            PacketHandler invocationMonitor,
                            PacketHandler eventService,
                            PacketHandler connectionManager,
                            PacketHandler jetService) {
        this.logger = logger;
        this.responseHandler = responseHandler;
        this.eventService = eventService;
        this.invocationMonitor = invocationMonitor;
        this.connectionManager = connectionManager;
        this.operationExecutor = operationExecutor;
        this.jetService = jetService;

        int responseThreadCount = properties.getInteger(DISPATCH_THREAD_COUNT);
        if (responseThreadCount < 0) {
            throw new IllegalArgumentException(DISPATCH_THREAD_COUNT.getName() + " can't be smaller than 0");
        }

        threads = new DispatchThread[responseThreadCount];
        for(int k=0;k<threads.length;k++){
            threads[k]=new DispatchThread();
        }
    }

    @Override
    public void handle(Packet packet) throws Exception {
        try {
            switch (packet.getPacketType()) {
                case OPERATION:
                    if (packet.isFlagRaised(FLAG_OP_RESPONSE)) {
                        responseHandler.handle(packet);
                    } else if (packet.isFlagRaised(FLAG_OP_CONTROL)) {
                        invocationMonitor.handle(packet);
                    } else {
                        operationExecutor.handle(packet);
                    }
                    break;
                case EVENT:
                    eventService.handle(packet);
                    break;
                case BIND:
                    connectionManager.handle(packet);
                    break;
                case JET:
                    jetService.handle(packet);
                    break;
                default:
                    logger.severe("Header flags [" + Integer.toBinaryString(packet.getFlags())
                            + "] specify an undefined packet type " + packet.getPacketType().name());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Throwable t) {
            inspectOutOfMemoryError(t);
            logger.severe("Failed to process: " + packet, t);
        }
    }

    public void start() {
        for (DispatchThread thread : threads) {
            thread.start();
        }
    }

    public void shutdown() {
        for (DispatchThread thread : threads) {
            thread.shutdown();
        }
    }

    @Override
    public void handle(Packet packet) {
        int threadIndex = INT_HOLDER.get().getAndInc() % threads.length;
        threads[threadIndex].dispatchQueue.add(packet);
    }

    private final class DispatchThread extends Thread implements OperationHostileThread {

        private final BlockingQueue<Packet> dispatchQueue;
        private volatile boolean shutdown;

        private DispatchThread(String hzName, int threadIndex) {
            super(createThreadName(hzName, "dispatch-" + threadIndex));
            this.dispatchQueue = new MPSCQueue<Packet>(this, null);
        }

        @Override
        public void run() {
            try {
                run0();
            } catch (InterruptedException e) {
                ignore(e);
            } catch (Throwable t) {
                inspectOutOfMemoryError(t);
                logger.severe(t);
            }
        }

        private void run0() throws InterruptedException {
            while (!shutdown) {
                Packet packet = dispatchQueue.take();
                Packet next;
                do {
                    next = packet.next;
                    packet.next = null;
                    process(packet);
                } while (next != null);
            }
        }

        private void process(Packet packet) {
            try {
                packetHandler.handle(packet);
            } catch (Throwable e) {
                inspectOutOfMemoryError(e);
                logger.severe("Failed to process packet: " + packet + " on:" + getName(), e);
            }
        }

        private void shutdown() {
            shutdown = true;
            interrupt();
        }
    }

}
