/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.networking.nio;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.networking.ChannelErrorHandler;
import com.hazelcast.internal.networking.ChannelHandler;
import com.hazelcast.internal.networking.HandlerStatus;
import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.internal.networking.OutboundHandler;
import com.hazelcast.internal.networking.OutboundPipeline;
import com.hazelcast.internal.networking.nio.iobalancer.IOBalancer;
import com.hazelcast.internal.util.ConcurrencyDetection;
import com.hazelcast.logging.ILogger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.networking.HandlerStatus.CLEAN;
import static com.hazelcast.internal.networking.HandlerStatus.DIRTY;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.collection.ArrayUtils.append;
import static com.hazelcast.internal.util.collection.ArrayUtils.replaceFirst;
import static java.lang.Long.max;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.currentThread;
import static java.nio.channels.SelectionKey.OP_WRITE;


enum State {
    /*
     * The pipeline isn't scheduled (nothing to do).
     * Only possible next state is scheduled.
     */
    UNSCHEDULED,
    /*
     * The pipeline is scheduled, meaning it is owned by some thread.
     *
     * the next possible states are:
     * - unscheduled (everything got written; we are done)
     * - scheduled: new writes got detected
     * - reschedule: needed if one of the handlers wants to reschedule the pipeline
     */
    SCHEDULED,
    /*
     * One of the handler wants to stop with the pipeline; one of the usages is TLS handshake.
     * Additional writes of frames will not lead to a scheduling of the pipeline. Only a
     * wakeup will schedule the pipeline.
     *
     * Next possible states are:
     * - unscheduled: everything got written
     * - scheduled: new writes got detected
     * - reschedule: pipeline needs to be reprocessed
     * - blocked (one of the handler wants to stop with the pipeline; one of the usages is TLS handshake
     */
    BLOCKED,
    /*
     * state needed for pipeline that was scheduled, but needs to be reprocessed
     * this is needed for wakeup during processing (TLS).
     *
     * Next possible states are:
     * - unscheduled: everything got written
     * - scheduled: new writes got detected
     * - reschedule: pipeline needs to be reprocessed
     * - blocked (one of the handler wants to stop with the pipeline; one of the usages is TLS handshake
     */
    RESCHEDULE
}

abstract class NioOutboundPipelineL1Pad extends NioPipeline {
    long p01, p02, p03, p04, p05, p06, p07;
    long p10, p11, p12, p13, p14, p15, p16, p17;

    NioOutboundPipelineL1Pad(NioChannel channel,
                             NioThread owner,
                             ChannelErrorHandler errorHandler,
                             int initialOps,
                             ILogger logger,
                             IOBalancer ioBalancer) {
        super(channel, owner, errorHandler, initialOps, logger, ioBalancer);
    }
}

abstract class NioOutboundPipelineL1Fields extends NioOutboundPipelineL1Pad {
    final static AtomicLongFieldUpdater<NioOutboundPipelineL1Fields> NORMAL_FRAMES_WRITTEN
            = AtomicLongFieldUpdater.newUpdater(NioOutboundPipelineL1Fields.class, "normalFramesWritten");
    final static AtomicLongFieldUpdater<NioOutboundPipelineL1Fields> PRIORITY_FRAMES_WRITTEN
            = AtomicLongFieldUpdater.newUpdater(NioOutboundPipelineL1Fields.class, "priorityFramesWritten");
    final static AtomicLongFieldUpdater<NioOutboundPipelineL1Fields> PROCESS_COUNT
            = AtomicLongFieldUpdater.newUpdater(NioOutboundPipelineL1Fields.class, "processCount");
    final static AtomicLongFieldUpdater<NioOutboundPipelineL1Fields> LAST_WRITE_TIME
            = AtomicLongFieldUpdater.newUpdater(NioOutboundPipelineL1Fields.class, "lastWriteTime");
    final static AtomicLongFieldUpdater<NioOutboundPipelineL1Fields> BYTES_WRITTEN
            = AtomicLongFieldUpdater.newUpdater(NioOutboundPipelineL1Fields.class, "bytesWritten");

    OutboundHandler[] handlers = new OutboundHandler[0];
    ByteBuffer sendBuffer;
    @Probe(name = "bytesWritten")
    public volatile long bytesWritten;
    @Probe(name = "normalFramesWritten")
    public volatile long normalFramesWritten;
    @Probe(name = "priorityFramesWritten")
    public volatile long priorityFramesWritten;
    @Probe
    public volatile long processCount;
    long bytesWrittenLastPublish;
    long normalFramesWrittenLastPublish;
    long priorityFramesWrittenLastPublish;
    long processCountLastPublish;
    public volatile long lastWriteTime;

    NioOutboundPipelineL1Fields(NioChannel channel,
                                NioThread owner,
                                ChannelErrorHandler errorHandler,
                                int initialOps,
                                ILogger logger,
                                IOBalancer ioBalancer) {
        super(channel, owner, errorHandler, initialOps, logger, ioBalancer);
    }
}

abstract class NioOutboundPipelineL2Pad extends NioOutboundPipelineL1Fields {
    long p01, p02, p03, p04, p05, p06, p07;
    long p10, p11, p12, p13, p14, p15, p16, p17;

    NioOutboundPipelineL2Pad(NioChannel channel,
                             NioThread owner,
                             ChannelErrorHandler errorHandler,
                             int initialOps,
                             ILogger logger,
                             IOBalancer ioBalancer) {
        super(channel, owner, errorHandler, initialOps, logger, ioBalancer);
    }
}

abstract class NioOutboundPipelineL2Fields extends NioOutboundPipelineL2Pad {

    @SuppressWarnings("checkstyle:visibilitymodifier")
    //@Probe(name = "writeQueueSize")
    public final ConcurrentLinkedQueue<OutboundFrame> writeQueue = new ConcurrentLinkedQueue<OutboundFrame>();
    @SuppressWarnings("checkstyle:visibilitymodifier")
    @Probe(name = "priorityWriteQueueSize")
    public final Queue<OutboundFrame> priorityWriteQueue = new ConcurrentLinkedQueue<>();
    final AtomicReference<State> scheduled = new AtomicReference<>(State.SCHEDULED);

    ConcurrencyDetection concurrencyDetection;
    boolean writeThroughEnabled;
    boolean selectionKeyWakeupEnabled;

    NioOutboundPipelineL2Fields(NioChannel channel,
                                NioThread owner,
                                ChannelErrorHandler errorHandler,
                                int initialOps,
                                ILogger logger,
                                IOBalancer ioBalancer) {
        super(channel, owner, errorHandler, initialOps, logger, ioBalancer);
    }
}

abstract class NioOutboundPipelineL3Pad extends NioOutboundPipelineL2Fields {
    long p01, p02, p03, p04, p05, p06, p07;
    long p10, p11, p12, p13, p14, p15, p16, p17;

    NioOutboundPipelineL3Pad(NioChannel channel,
                             NioThread owner,
                             ChannelErrorHandler errorHandler,
                             int initialOps,
                             ILogger logger,
                             IOBalancer ioBalancer) {
        super(channel, owner, errorHandler, initialOps, logger, ioBalancer);
    }
}

public final class NioOutboundPipeline
        extends NioOutboundPipelineL3Pad
        implements Supplier<OutboundFrame>, OutboundPipeline {

    NioOutboundPipeline(NioChannel channel,
                        NioThread owner,
                        ChannelErrorHandler errorHandler,
                        ILogger logger,
                        IOBalancer balancer,
                        ConcurrencyDetection concurrencyDetection,
                        boolean writeThroughEnabled,
                        boolean selectionKeyWakeupEnabled) {
        super(channel, owner, errorHandler, OP_WRITE, logger, balancer);
        this.concurrencyDetection = concurrencyDetection;
        this.writeThroughEnabled = writeThroughEnabled;
        this.selectionKeyWakeupEnabled = selectionKeyWakeupEnabled;
    }

    @Override
    public long load() {
        switch (loadType) {
            case LOAD_BALANCING_HANDLE:
                return processCount;
            case LOAD_BALANCING_BYTE:
                return bytesWritten;
            case LOAD_BALANCING_FRAME:
                return normalFramesWritten + priorityFramesWritten;
            default:
                throw new RuntimeException();
        }
    }

    public int totalFramesPending() {
        return writeQueue.size() + priorityWriteQueue.size();
    }

    public long lastWriteTimeMillis() {
        return lastWriteTime;
    }

    @Probe(name = "writeQueuePendingBytes", level = DEBUG)
    public long bytesPending() {
        return 0;//return bytesPending(writeQueue);
    }

    @Probe(name = "priorityWriteQueuePendingBytes", level = DEBUG)
    public long priorityBytesPending() {
        return bytesPending(priorityWriteQueue);
    }

    private long bytesPending(Queue<OutboundFrame> writeQueue) {
        long bytesPending = 0;
        for (OutboundFrame frame : writeQueue) {
            bytesPending += frame.getFrameLength();
        }
        return bytesPending;
    }

    @Probe
    private long idleTimeMs() {
        return max(currentTimeMillis() - lastWriteTime, 0);
    }

    @Probe
    private long scheduled() {
        return scheduled.get().ordinal();
    }

    public void write(OutboundFrame frame) {
        if (frame.isUrgent()) {
            priorityWriteQueue.offer(frame);
        } else {
            writeQueue.offer(frame);
        }

        // take care of the scheduling.
        for (; ; ) {
            State state = scheduled.get();
            if (state == State.UNSCHEDULED) {
                // pipeline isn't scheduled, so we need to schedule it.
                if (!scheduled.compareAndSet(State.UNSCHEDULED, State.SCHEDULED)) {
                    // try again
                    continue;
                }

                executePipeline();
                return;
            } else if (state == State.SCHEDULED || state == State.RESCHEDULE) {
                // already scheduled, so we are done
                if (writeThroughEnabled) {
                    concurrencyDetection.onDetected();
                }
                return;
            } else if (state == State.BLOCKED) {
                // pipeline is blocked, so we don't need to schedule
                return;
            } else {
                throw new IllegalStateException("Unexpected state:" + state);
            }
        }
    }

    // executes the pipeline. Either on the calling thread or on th owning NIO thread.
    private void executePipeline() {
        if (writeThroughEnabled && !concurrencyDetection.isDetected()) {
            // we are allowed to do a write through, so lets process the request on the calling thread
            try {
                process();
            } catch (Throwable t) {
                onError(t);
            }
        } else {
            if (selectionKeyWakeupEnabled) {
                registerOp(OP_WRITE);
                selectionKey.selector().wakeup();
            } else {
                owner.addTaskAndWakeup(this);
            }
        }
    }

    @Override
    public OutboundPipeline wakeup() {
        for (; ; ) {
            State prevState = scheduled.get();
            if (prevState == State.RESCHEDULE) {
                break;
            } else {
                if (scheduled.compareAndSet(prevState, State.RESCHEDULE)) {
                    if (prevState == State.UNSCHEDULED || prevState == State.BLOCKED) {
                        ownerAddTaskAndWakeup(this);
                    }
                    break;
                }
            }
        }

        return this;
    }


    @Override
    public OutboundFrame get() {
        OutboundFrame frame = priorityWriteQueue.poll();
        if (frame == null) {
            frame = writeQueue.poll();

            if (frame == null) {
                return null;
            }
            NORMAL_FRAMES_WRITTEN.lazySet(this, normalFramesWritten + 1);
        } else {
            PRIORITY_FRAMES_WRITTEN.lazySet(this, priorityFramesWritten + 1);
        }

        return frame;
    }

    // is never called concurrently!
    @Override
    @SuppressWarnings("unchecked")
    public void process() throws Exception {
        PROCESS_COUNT.lazySet(this, processCount + 1);

        OutboundHandler[] localHandlers = handlers;
        HandlerStatus pipelineStatus = CLEAN;
        for (int handlerIndex = 0; handlerIndex < localHandlers.length; handlerIndex++) {
            OutboundHandler handler = localHandlers[handlerIndex];

            HandlerStatus handlerStatus = handler.onWrite();

            if (localHandlers != handlers) {
                // change in the pipeline detected, therefor the loop is restarted.
                localHandlers = handlers;
                pipelineStatus = CLEAN;
                handlerIndex = -1;
            } else if (handlerStatus != CLEAN) {
                pipelineStatus = handlerStatus;
            }
        }

        flushToSocket();

        if (migrationRequested()) {
            startMigration();
            // we leave this method and the NioOutboundPipeline remains scheduled.
            // So we don't need to worry about write-through
            return;
        }

        if (sendBuffer.remaining() > 0) {
            pipelineStatus = DIRTY;
        }

        switch (pipelineStatus) {
            case CLEAN:
                postProcessClean();
                break;
            case DIRTY:
                postProcessDirty();
                break;
            case BLOCKED:
                postProcessBlocked();
                break;
            default:
                throw new IllegalStateException();
        }
    }

    private void postProcessBlocked() throws IOException {
        // pipeline is blocked; no point in receiving OP_WRITE events.
        unregisterOp(OP_WRITE);

        // we try to set the state to blocked.
        for (; ; ) {
            State state = scheduled.get();
            if (state == State.SCHEDULED) {
                // if it is still scheduled, we'll just try to put it to blocked.
                if (scheduled.compareAndSet(State.SCHEDULED, State.BLOCKED)) {
                    break;
                }
            } else if (state == State.BLOCKED) {
                // it is already blocked, so we are done.
                break;
            } else if (state == State.RESCHEDULE) {
                // rescheduling is requested, so lets do that. Once put to RESCHEDULE,
                // only the thread running the process method will change the state, so we can safely call a set.
                scheduled.set(State.SCHEDULED);
                // this will cause the pipeline to be rescheduled.
                owner().addTaskAndWakeup(this);
                break;
            } else {
                throw new IllegalStateException("unexpected state:" + state);
            }
        }
    }

    private void postProcessDirty() throws IOException {
        // pipeline is dirty, so register for an OP_WRITE to write more data.
        registerOp(OP_WRITE);

        if (writeThroughEnabled && !(Thread.currentThread() instanceof NioThread)) {
            // there was a write through. Changing the interested set of the selection key
            // after the IO thread did a select, will not lead to the selector waking up. So
            // if we don't wake up the selector explicitly, only after the selector.select(timeout)
            // has expired the selectionKey will be seen. For more info see:
            // https://stackoverflow.com/questions/11523471/java-selectionkey-interestopsint-not-thread-safe
            owner.getSelector().wakeup();
            concurrencyDetection.onDetected();
        }
    }

    private void postProcessClean() throws IOException {
        // There is nothing left to be done; so lets unschedule this pipeline
        // since everything is written, we are not interested anymore in write-events, so lets unsubscribe
        unregisterOp(OP_WRITE);

        for (; ; ) {
            State state = scheduled.get();
            if (state == State.RESCHEDULE) {
                // the pipeline needs to be rescheduled. The current thread is still owner of the pipeline,
                // so lets remove the reschedule flag and return it to schedule and lets reprocess the pipeline.
                scheduled.set(State.SCHEDULED);
                owner().addTaskAndWakeup(this);
                return;
            }

            if (!scheduled.compareAndSet(state, State.UNSCHEDULED)) {
                // we didn't manage to set it to unscheduled, lets retry the loop and see what needs to be done
                continue;
            }

            // we manage to unschedule the pipeline. From this point on we have released ownership of the pipeline
            // and another thread could call the process method.
            if (writeQueue.isEmpty() && priorityWriteQueue.isEmpty()) {
                //pipeline is clean, we are done.
                return;
            }

            // there is stuff to write we are going to reclaim ownership of the pipeline to prevent
            // we are going to end up with scheduled pipeline that isn't going to be processed.
            // If we can't reclaim ownership, then it is the concern of the other thread deal with the pipeline.
            if (scheduled.compareAndSet(State.UNSCHEDULED, State.SCHEDULED)) {
                if (Thread.currentThread().getClass() == NioThread.class) {
                    owner().addTask(this);
                } else {
                    owner().addTaskAndWakeup(this);
                }
            }

            return;
        }
    }

    private void flushToSocket() throws IOException {
        LAST_WRITE_TIME.lazySet(this, currentTimeMillis());
        int written = socketChannel.write(sendBuffer);
        BYTES_WRITTEN.lazySet(this, bytesWritten + written);
        //System.out.println(channel + " bytes written:" + written);
    }

    void drainWriteQueues() {
        writeQueue.clear();
        priorityWriteQueue.clear();
    }

    @Override
    protected void publishMetrics() {
        if (currentThread() != owner) {
            return;
        }

        owner.bytesTransceived += bytesWritten - bytesWrittenLastPublish;
        owner.framesTransceived += normalFramesWritten - normalFramesWrittenLastPublish;
        owner.priorityFramesTransceived += priorityFramesWritten - priorityFramesWrittenLastPublish;
        owner.processCount += processCount - processCountLastPublish;

        bytesWrittenLastPublish = bytesWritten;
        normalFramesWrittenLastPublish = normalFramesWritten;
        priorityFramesWrittenLastPublish = priorityFramesWritten;
        processCountLastPublish = processCount;
    }

    @Override
    public String toString() {
        return channel + ".outboundPipeline";
    }

    @Override
    protected Iterable<? extends ChannelHandler> handlers() {
        return Arrays.asList(handlers);
    }

    @Override
    public OutboundPipeline remove(OutboundHandler handler) {
        return replace(handler);
    }

    @Override
    public OutboundPipeline addLast(OutboundHandler... addedHandlers) {
        checkNotNull(addedHandlers, "addedHandlers can't be null");

        for (OutboundHandler addedHandler : addedHandlers) {
            addedHandler.setChannel(channel).handlerAdded();
        }
        updatePipeline(append(handlers, addedHandlers));
        return this;
    }

    @Override
    public OutboundPipeline replace(OutboundHandler oldHandler, OutboundHandler... addedHandlers) {
        checkNotNull(oldHandler, "oldHandler can't be null");
        checkNotNull(addedHandlers, "newHandler can't be null");

        OutboundHandler[] newHandlers = replaceFirst(handlers, oldHandler, addedHandlers);
        if (newHandlers == handlers) {
            throw new IllegalArgumentException("handler " + oldHandler + " isn't part of the pipeline");
        }

        for (OutboundHandler addedHandler : addedHandlers) {
            addedHandler.setChannel(channel).handlerAdded();
        }
        updatePipeline(newHandlers);
        return this;
    }

    private void updatePipeline(OutboundHandler[] newHandlers) {
        this.handlers = newHandlers;
        this.sendBuffer = newHandlers.length == 0 ? null : (ByteBuffer) newHandlers[newHandlers.length - 1].dst();

        OutboundHandler prev = null;
        for (OutboundHandler handler : handlers) {
            if (prev == null) {
                handler.src(this);
            } else {
                Object src = prev.dst();
                if (src instanceof ByteBuffer) {
                    handler.src(src);
                }
            }
            prev = handler;
        }
    }

    // useful for debugging
    private String pipelineToString() {
        StringBuilder sb = new StringBuilder("out-pipeline[");
        OutboundHandler[] handlers = this.handlers;
        for (int k = 0; k < handlers.length; k++) {
            if (k > 0) {
                sb.append("->-");
            }
            sb.append(handlers[k].getClass().getSimpleName());
        }
        sb.append(']');
        return sb.toString();
    }
}
