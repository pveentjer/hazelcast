package com.hazelcast.internal.networking;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.nio.OutboundFrame;
import com.hazelcast.nio.Packet;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;

public class InterleavingOutboundHandler implements BufferingOutboundHandler {

    @SuppressWarnings("checkstyle:visibilitymodifier")
    @Probe(name = "writeQueueSize")
    public final Queue<OutboundFrame> queue = new ConcurrentLinkedQueue<OutboundFrame>();
    @SuppressWarnings("checkstyle:visibilitymodifier")
    @Probe(name = "priorityWriteQueueSize")
    public final Queue<OutboundFrame> priorityQueue = new ConcurrentLinkedQueue<OutboundFrame>();
    @Probe(name = "normalFramesWritten")
    private final SwCounter normalFramesWritten = newSwCounter();
    @Probe(name = "priorityFramesWritten")
    private final SwCounter priorityFramesWritten = newSwCounter();
    private final int maxParallelPackets;
    private final int maxChunkSize;

    private OutboundFrame currentFrame;

    private final Queue[] queues;

    private final ChannelOutboundHandler next;

    public InterleavingOutboundHandler(ChannelOutboundHandler next, int maxParallelPackets, int maxChunkize) {
        this.next = next;
        this.maxParallelPackets = maxParallelPackets;
        this.maxChunkSize = maxChunkize;
    }

    @Override
    public void offer(boolean urgent, OutboundFrame frame) {
        if (urgent) {
            priorityQueue.offer(frame);
        } else {
            queue.offer(frame);
        }
    }

    @Override
    public int pending() {
        return queue.size() + priorityQueue.size();
    }

    @Override
    public void clear() {
        queue.clear();
        priorityQueue.clear();
    }

    @Override
    public boolean isEmpty() {
        return queue.isEmpty() && priorityQueue.isEmpty();
    }

    @Probe(name = "writeQueuePendingBytes", level = DEBUG)
    public long bytesPending() {
        return bytesPending(queue);
    }

    @Probe(name = "priorityWriteQueuePendingBytes", level = DEBUG)
    public long priorityBytesPending() {
        return bytesPending(priorityQueue);
    }

    private long bytesPending(Queue<OutboundFrame> writeQueue) {
        long bytesPending = 0;
        for (OutboundFrame frame : writeQueue) {
            if (frame instanceof Packet) {
                bytesPending += ((Packet) frame).packetSize();
            }
        }
        return bytesPending;
    }

    private OutboundFrame poll() {
        for (; ; ) {
            boolean urgent = true;
            OutboundFrame frame = priorityQueue.poll();

            if (frame == null) {
                urgent = false;
                frame = queue.poll();
            }

            if (frame == null) {
                return null;
            }

            if (frame.getClass() == TaskFrame.class) {
                TaskFrame taskFrame = (TaskFrame) frame;
                taskFrame.getTask().run();
                continue;
            }

            if (urgent) {
                priorityFramesWritten.inc();
            } else {
                normalFramesWritten.inc();
            }

            return frame;
        }
    }

    @Override
    public boolean write(OutboundFrame frame, ByteBuffer dst) throws Exception {
        for (; ; ) {
            // If there currently is not frame sending, lets try to get one.
            if (currentFrame == null) {
                currentFrame = poll();
                if (currentFrame == null) {
                    // There is no frame to write, we are done.
                    return true;
                }
            }

            // Lets write the currentFrame to the dst.
            if (!next.write(currentFrame, dst)) {
                // We are done for this round because not all data of the current frame fits in the dst
                return false;
            }

            // The current frame has been written completely. So lets null it and lets try to write another frame.
            currentFrame = null;
        }
    }
}
