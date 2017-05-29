package com.hazelcast.internal.networking.udpspinning;

import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.nio.Packet;

import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

public class UdpSpinningChannelWriter {

    private static final AtomicLongFieldUpdater WRITER_INDEX = newUpdater(UdpSpinningChannelWriter.class, "writerIndex");

    private final Buffer[] buffers = new Buffer[3];

    private final ConcurrentLinkedQueue<TaskFrame> tasks = new ConcurrentLinkedQueue<>();

    private DatagramChannel datagramChannel;

    private volatile long lastWriteTimeMillis;

    // if writerIndex == readerIndex then all buffers clean
    // if writerIndex == readerIndex + 3, then all buffers are full
    private volatile long writerIndex;

    private volatile long readerIndex;

    public UdpSpinningChannelWriter() {
        for (int k = 0; k < buffers.length; k++) {
            buffers[k] = new Buffer();
        }
    }

    // called by user threads, operation threads etc.
    public void write(OutboundFrame frame) {
        if (frame instanceof Packet) {
            for (; ; ) {
                Packet packet = (Packet) frame;

                long currentWriterIndex = writerIndex;
                long currentReaderIndex = readerIndex;

                Buffer active = null;
                if (active.write(packet)) {

                }
            }
        } else {
            tasks.add((TaskFrame) frame);
        }
    }


    // one thread calling this all the time.
    public void handle() throws Exception {
        runTasks();

        // the reader can take all the buffers as long as it < writer index.

        // todo: martin thompson mentioned spinning on the length field to detect if a write is complete.
        // perhaps he writes to the socket up to that point?
        //

        ByteBuffer dirtyBuffer = null;
        int written = datagramChannel.write(dirtyBuffer);

        lastWriteTimeMillis = System.currentTimeMillis();

        // we can clear the buffer since everything is written.
        dirtyBuffer.clear();

        // the dirty buffer is now fully written, so it has become the clean buffer.
        // now we signal

        // todo: we need to signal that we want to get a dirty buffer.

    }

    private void runTasks() {
        TaskFrame taskFrame = tasks.poll();
        while (taskFrame != null) {
            taskFrame.task.run();
            taskFrame = tasks.poll();
        }
    }

    public long lastWriteTimeMillis() {
        return lastWriteTimeMillis;
    }

    /**
     * The TaskFrame is not really a Frame. It is a way to put a task on one of the frame-queues. Using this approach we
     * can lift on top of the Frame scheduling mechanism and we can prevent having:
     * - multiple NioThread-tasks for a ChannelWriter on multiple NioThread
     * - multiple NioThread-tasks for a ChannelWriter on the same NioThread.
     */
    private static final class TaskFrame implements OutboundFrame {

        private final Runnable task;

        private TaskFrame(Runnable task) {
            this.task = task;
        }

        @Override
        public boolean isUrgent() {
            return true;
        }
    }
}
