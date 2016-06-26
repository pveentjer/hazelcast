/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.internal.metrics.MetricsProvider;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.Ringbuffer;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.impl.PacketHandler;
import com.hazelcast.spi.impl.operationexecutor.OperationHostileThread;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.util.concurrent.BusySpinIdleStrategy;
import com.hazelcast.util.concurrent.IdleStrategy;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.inspectOutOfMemoryError;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.nio.Packet.FLAG_OP;
import static com.hazelcast.nio.Packet.FLAG_OP_CONTROL;
import static com.hazelcast.nio.Packet.FLAG_RESPONSE;
import static com.hazelcast.util.EmptyStatement.ignore;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkTrue;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * The AsyncResponsePacketHandler is a PacketHandler that asynchronously process operation-response packets.
 * <p>
 * So when a response is received from a remote system, it is put in the responseQueue of the ResponseThread.
 * Then the ResponseThread takes it from this responseQueue and calls the {@link PacketHandler} for the
 * actual processing.
 * <p>
 * The reason that the IO thread doesn't immediately deals with the response is that deserializing the
 * {@link com.hazelcast.spi.impl.operationservice.impl.responses.Response} and let the invocation-future
 * deal with the response can be rather expensive currently.
 */
public class AsyncResponseHandler implements PacketHandler, MetricsProvider {

    public static final HazelcastProperty IDLE_STRATEGY
            = new HazelcastProperty("hazelcast.operation.responsequeue.idlestrategy", "block");

    private static final long IDLE_MAX_SPINS = 20;
    private static final long IDLE_MAX_YIELDS = 50;
    private static final long IDLE_MIN_PARK_NS = NANOSECONDS.toNanos(1);
    private static final long IDLE_MAX_PARK_NS = MICROSECONDS.toNanos(100);

    final ResponseThread responseThread;
    private final ILogger logger;
    public final Ringbuffer<Packet> queue;
    private final PacketHandler operationExecutor;
    private final PacketHandler responseHandler;
    private final PacketHandler invocationMonitor;

    AsyncResponseHandler(HazelcastThreadGroup threadGroup,
                         ILogger logger,
                         PacketHandler responseHandler,
                         PacketHandler operationExecutor,
                         PacketHandler invocationMonitor,
                         HazelcastProperties properties) {
        this.operationExecutor = operationExecutor;
        this.responseHandler = responseHandler;
        this.invocationMonitor = invocationMonitor;
        this.logger = logger;
        this.responseThread = new ResponseThread(threadGroup, properties);
        this.queue = responseThread.responseQueue;
    }

    @Probe(name = "responseQueueSize", level = MANDATORY)
    public int getQueueSize() {
        return responseThread.responseQueue.size();
    }

    @Override
    public void handle(Packet packet) {
        checkNotNull(packet, "packet can't be null");
        checkTrue(packet.isFlagSet(FLAG_OP), "FLAG_OP should be set");
        checkTrue(packet.isFlagSet(FLAG_RESPONSE), "FLAG_RESPONSE should be set");

        responseThread.responseQueue.add(packet);
    }


    public void handle(Packet[] packets, int length) {
//        checkNotNull(packet, "packet can't be null");
//        checkTrue(packet.isFlagSet(FLAG_OP), "FLAG_OP should be set");
//        checkTrue(packet.isFlagSet(FLAG_RESPONSE), "FLAG_RESPONSE should be set");

        queue.offerAll(packets, length);
    }

    @Override
    public void provideMetrics(MetricsRegistry metricsRegistry) {
        metricsRegistry.scanAndRegister(this, "operation");
    }

    public void start() {
        responseThread.start();
    }

    public void shutdown() {
        responseThread.shutdown();
    }

    public static IdleStrategy getIdleStrategy(HazelcastProperties properties, HazelcastProperty property) {
        String idleStrategyString = properties.getString(property);
        if ("block".equals(idleStrategyString)) {
            return null;
        } else if ("backoff".equals(idleStrategyString)) {
            return new BackoffIdleStrategy(IDLE_MAX_SPINS, IDLE_MAX_YIELDS, IDLE_MIN_PARK_NS, IDLE_MAX_PARK_NS);
        } else if ("busyspin".equals(idleStrategyString)) {
            return new BusySpinIdleStrategy();
        } else {
            throw new IllegalStateException("Unrecognized " + property.getName() + " value=" + idleStrategyString);
        }
    }

    /**
     * The ResponseThread needs to implement the OperationHostileThread interface to make sure that the OperationExecutor
     * is not going to schedule any operations on this task due to retry.
     */
    final class ResponseThread extends Thread implements OperationHostileThread {

        private final Ringbuffer<Packet> responseQueue;
        private volatile boolean shutdown;

        private ResponseThread(HazelcastThreadGroup threadGroup,
                               HazelcastProperties properties) {
            super(threadGroup.getInternalThreadGroup(), threadGroup.getThreadNamePrefix("response"));
            setContextClassLoader(threadGroup.getClassLoader());
            this.responseQueue = new Ringbuffer<Packet>(this, 16384, getIdleStrategy(properties, IDLE_STRATEGY));
            //this.responseQueue = new MPSCQueue<Packet>(this, getIdleStrategy(properties, IDLE_STRATEGY));
        }

        @Override
        public void run() {
            try {
                doRun();
            } catch (InterruptedException e) {
                ignore(e);
            } catch (Throwable t) {
                inspectOutOfMemoryError(t);
                logger.severe(t);
            }
        }

        private void doRun() throws InterruptedException {
            while (!shutdown) {
                Packet response = responseQueue.take();

                try {
                    dispatch(response);
                } catch (Throwable e) {
                    inspectOutOfMemoryError(e);
                    logger.severe("Failed to process response: " + response + " on:" + getName(), e);
                }
            }
        }

        public void dispatch(Packet packet) {
            try {
                if (packet.isFlagSet(FLAG_RESPONSE)) {
                    responseHandler.handle(packet);
                } else if (packet.isFlagSet(FLAG_OP_CONTROL)) {
                    invocationMonitor.handle(packet);
                } else {
                    operationExecutor.handle(packet);
                }
            } catch (Throwable t) {
                inspectOutOfMemoryError(t);
                logger.severe("Failed to process:" + packet, t);
            }
        }

        private void shutdown() {
            shutdown = true;
            interrupt();
        }
    }
}
