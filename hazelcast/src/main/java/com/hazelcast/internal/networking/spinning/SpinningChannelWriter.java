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

package com.hazelcast.internal.networking.spinning;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.networking.BufferingOutboundHandler;
import com.hazelcast.internal.networking.ChannelWriter;
import com.hazelcast.internal.networking.IOOutOfMemoryHandler;
import com.hazelcast.internal.networking.SocketConnection;
import com.hazelcast.internal.networking.TaskFrame;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.OutboundFrame;
import com.hazelcast.util.EmptyStatement;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;

public class SpinningChannelWriter extends AbstractHandler implements ChannelWriter {

    private static final long TIMEOUT = 3;

    private final ByteBuffer outputBuffer;
    @Probe(name = "bytesWritten")
    private final SwCounter bytesWritten = newSwCounter();
    private volatile long lastWriteTime;
    private final BufferingOutboundHandler outboundHandler;
    private volatile OutboundFrame currentFrame;

    public SpinningChannelWriter(SocketConnection connection,
                                 ILogger logger,
                                 IOOutOfMemoryHandler oomeHandler,
                                 BufferingOutboundHandler outboundHandler,
                                 ByteBuffer outputBuffer) {
        super(connection, logger, oomeHandler);
        this.outboundHandler = outboundHandler;
        this.outputBuffer = outputBuffer;
    }

    @Override
    public void write(OutboundFrame frame) {
        outboundHandler.offer(frame.isUrgent(), frame);
    }

    @Probe(name = "idleTimeMs")
    private long idleTimeMs() {
        return Math.max(currentTimeMillis() - lastWriteTime, 0);
    }

    @Override
    public long lastWriteMillis() {
        return lastWriteTime;
    }

    // accessed from ChannelInboundHandler and SocketConnector
    @Override
    public void handshake() {
        final CountDownLatch latch = new CountDownLatch(1);
        outboundHandler.offer(true, new TaskFrame(new Runnable() {
            @Override
            public void run() {
                logger.info("Setting protocol: " + connection.getProtocol());
//                if (outboundHandler == null) {
//                    initializer.init(connection, SpinningChannelWriter.this, protocol);
//                }
                latch.countDown();
            }
        }));

        try {
            latch.await(TIMEOUT, SECONDS);
        } catch (InterruptedException e) {
            logger.finest("CountDownLatch::await interrupted", e);
        }
    }

    @Override
    public void close() {
        outboundHandler.clear();

        ShutdownTask shutdownTask = new ShutdownTask();
        write(new TaskFrame(shutdownTask));
        shutdownTask.awaitCompletion();
    }

    public void write() throws Exception {
        if (!connection.isAlive()) {
            return;
        }

        outboundHandler.write(null, outputBuffer);

        if (dirtyOutputBuffer()) {
            writeOutputBufferToSocket();
        }
    }

    /**
     * Checks of the outputBuffer is dirty.
     *
     * @return true if dirty, false otherwise.
     */
    private boolean dirtyOutputBuffer() {
        if (outputBuffer == null) {
            return false;
        }
        return outputBuffer.position() > 0;
    }


    /**
     * Writes to content of the outputBuffer to the socket.
     *
     * @throws Exception
     */
    private void writeOutputBufferToSocket() throws Exception {
        // So there is data for writing, so lets prepare the buffer for writing and then write it to the socketChannel.
        outputBuffer.flip();
        int result = socketChannel.write(outputBuffer);
        if (result > 0) {
            lastWriteTime = currentTimeMillis();
            bytesWritten.inc(result);
        }

        if (outputBuffer.hasRemaining()) {
            outputBuffer.compact();
        } else {
            outputBuffer.clear();
        }
    }

    private class ShutdownTask implements Runnable {
        private final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void run() {
//            try {
//             //   socketChannel.closeOutbound();
//            } catch (IOException e) {
//                logger.finest("Error while closing outbound", e);
//            } finally {
//                latch.countDown();
//            }
        }

        void awaitCompletion() {
            try {
                latch.await(TIMEOUT, SECONDS);
            } catch (InterruptedException e) {
                EmptyStatement.ignore(e);
            }
        }
    }
}
