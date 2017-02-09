/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.networking.nonblocking;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.networking.BufferingOutboundHandler;
import com.hazelcast.internal.networking.ChannelWriter;
import com.hazelcast.internal.networking.SocketConnection;
import com.hazelcast.internal.networking.TaskFrame;
import com.hazelcast.internal.networking.nonblocking.iobalancer.IOBalancer;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.OutboundFrame;
import com.hazelcast.nio.tcp.TcpIpConnection;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static com.hazelcast.util.EmptyStatement.ignore;
import static java.lang.Math.max;
import static java.lang.System.currentTimeMillis;
import static java.nio.channels.SelectionKey.OP_WRITE;

/**
 * The writing side of the {@link TcpIpConnection}.
 */
public final class NonBlockingChannelWriter
        extends AbstractHandler
        implements Runnable, ChannelWriter {

    private static final long TIMEOUT = 3;

    private final ByteBuffer outputBuffer;

    private final AtomicBoolean scheduled = new AtomicBoolean(false);
    @Probe(name = "bytesWritten")
    private final SwCounter bytesWritten = newSwCounter();
    private final BufferingOutboundHandler outboundHandler;

    private volatile long lastWriteTime;

    // this field will be accessed by the NonBlockingIOThread or
    // it is accessed by any other thread but only that thread managed to cas the scheduled flag to true.
    // This prevents running into an NonBlockingIOThread that is migrating.
    private NonBlockingIOThread newOwner;

    public NonBlockingChannelWriter(SocketConnection connection,
                                    NonBlockingIOThread ioThread,
                                    ILogger logger,
                                    IOBalancer balancer,
                                    BufferingOutboundHandler outboundHandler,
                                    ByteBuffer outputBuffer) {
        super(connection, ioThread, OP_WRITE, logger, balancer);
        this.outboundHandler = outboundHandler;
        this.outputBuffer = outputBuffer;
    }

    @Override
    public long lastWriteMillis() {
        return lastWriteTime;
    }

    @Probe(name = "idleTimeMs")
    private long idleTimeMs() {
        return max(currentTimeMillis() - lastWriteTime, 0);
    }

    @Probe(name = "isScheduled", level = DEBUG)
    private long isScheduled() {
        return scheduled.get() ? 1 : 0;
    }

    // accessed from ChannelInboundHandler and SocketConnector
    @Override
    public void handshake() {
        final CountDownLatch latch = new CountDownLatch(1);
        ioThread.addTaskAndWakeup(new Runnable() {
            @Override
            public void run() {
                try {
                    String protocol = connection.getProtocol();
                    ByteBuffer bb = ByteBuffer.wrap(protocol.getBytes());
                    socketChannel.write(bb);
                    // registerOp(OP_WRITE);
//                    if (outboundHandler == null) {
//                        initializer.init(connection, NonBlockingChannelWriter.this, protocol);
//                    }

                    System.out.println("protocol " + protocol + " written");
                } catch (Throwable t) {
                    onFailure(t);
                } finally {
                    latch.countDown();
                }
            }
        });
        try {
            latch.await(TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.finest("CountDownLatch::await interrupted", e);
        }
    }

    @Override
    public void write(OutboundFrame frame) {
        outboundHandler.offer(frame.isUrgent(), frame);
        schedule();
    }

    /**
     * Makes sure this ChannelOutboundHandler is scheduled to be executed by the IO thread.
     * <p/>
     * This call is made by 'outside' threads that interact with the connection. For example when a frame is placed
     * on the connection to be written. It will never be made by an IO thread.
     * <p/>
     * If the ChannelOutboundHandler already is scheduled, the call is ignored.
     */
    private void schedule() {
        if (scheduled.get()) {
            // So this ChannelOutboundHandler is still scheduled, we don't need to schedule it again
            return;
        }

        if (!scheduled.compareAndSet(false, true)) {
            // Another thread already has scheduled this ChannelOutboundHandler, we are done. It
            // doesn't matter which thread does the scheduling, as long as it happens.
            return;
        }

        // We managed to schedule this ChannelOutboundHandler. This means we need to add a task to
        // the ioThread and give it a kick so that it processes our frames.
        ioThread.addTaskAndWakeup(this);
    }

    /**
     * Tries to unschedule this ChannelOutboundHandler.
     * <p/>
     * It will only be unscheduled if:
     * - the outputBuffer is empty
     * - there are no pending frames.
     * <p/>
     * If the outputBuffer is dirty then it will register itself for an OP_WRITE since we are interested in knowing
     * if there is more space in the socket output buffer.
     * If the outputBuffer is not dirty, then it will unregister itself from an OP_WRITE since it isn't interested
     * in space in the socket outputBuffer.
     * <p/>
     * This call is only made by the IO thread.
     */
    private void unschedule() throws IOException {
        // since everything is written, we are not interested anymore in write-events, so lets unsubscribe
        unregisterOp(OP_WRITE);
        // So the outputBuffer is empty, so we are going to unschedule ourselves.
        scheduled.set(false);

        if (outboundHandler.isEmpty()) {
            // there are no remaining frames, so we are done.
            return;
        }

        // So there are frames, but we just unscheduled ourselves. If we don't try to reschedule, then these
        // Frames are at risk not to be send.
        if (!scheduled.compareAndSet(false, true)) {
            //someone else managed to schedule this ChannelOutboundHandler, so we are done.
            return;
        }

        // We managed to reschedule. So lets add ourselves to the ioThread so we are processed again.
        // We don't need to call wakeup because the current thread is the IO-thread and the selectionQueue will be processed
        // till it is empty. So it will also pick up tasks that are added while it is processing the selectionQueue.
        ioThread.addTask(this);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void handle() throws Exception {
        eventCount.inc();
        lastWriteTime = currentTimeMillis();

        boolean clean = outboundHandler.write(null, outputBuffer);

        if (outputBuffer.position() > 0) {
            // So there is data for writing, so lets prepare the buffer for writing and then write it to the socketChannel.
            outputBuffer.flip();

            int written = socketChannel.write(outputBuffer);

            bytesWritten.inc(written);

            if (outputBuffer.hasRemaining()) {
                outputBuffer.compact();
            } else {
                outputBuffer.clear();
            }
        }

        if (newOwner == null) {
            if (clean) {
                unschedule();
            } else {
                // Because not all data was written to the socket, we need to register for OP_WRITE so we get
                // notified when the socketChannel is ready for more data.
                registerOp(OP_WRITE);
            }
        } else {
            startMigration();
        }
    }

    private void startMigration() throws IOException {
        NonBlockingIOThread newOwner = this.newOwner;
        this.newOwner = null;
        startMigration(newOwner);
    }

    @Override
    public void run() {
        try {
            handle();
        } catch (Throwable t) {
            onFailure(t);
        }
    }

    @Override
    public void close() {
        outboundHandler.clear();
        CloseTask closeTask = new CloseTask();
        write(new TaskFrame(closeTask));
        closeTask.awaitCompletion();
    }

    @Override
    public void requestMigration(NonBlockingIOThread newOwner) {
        write(new TaskFrame(new StartMigrationTask(newOwner)));
    }

    @Override
    public String toString() {
        return connection + ".socketWriter";
    }

    /**
     * Triggers the migration when executed by setting the ChannelWriter.newOwner field. When the handle method completes, it
     * checks if this field if set, if so, the migration starts.
     *
     * If the current ioThread is the same as 'theNewOwner' then the call is ignored.
     */
    private final class StartMigrationTask implements Runnable {
        // field is called 'theNewOwner' to prevent any ambiguity problems with the outboundHandler.newOwner.
        // Else you get a lot of ugly ChannelOutboundHandler.this.newOwner is ...
        private final NonBlockingIOThread theNewOwner;

        StartMigrationTask(NonBlockingIOThread theNewOwner) {
            this.theNewOwner = theNewOwner;
        }

        @Override
        public void run() {
            assert newOwner == null : "No migration can be in progress";

            if (ioThread == theNewOwner) {
                // if there is no change, we are done
                return;
            }

            newOwner = theNewOwner;
        }
    }

    private class CloseTask implements Runnable {
        private final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void run() {
//            try {
//                socketChannel.closeOutbound();
//            } catch (IOException e) {
//                logger.finest("Error while closing outbound", e);
//            } finally {
//                latch.countDown();
//            }
        }

        void awaitCompletion() {
            try {
                latch.await(TIMEOUT, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                ignore(e);
            }
        }
    }
}
