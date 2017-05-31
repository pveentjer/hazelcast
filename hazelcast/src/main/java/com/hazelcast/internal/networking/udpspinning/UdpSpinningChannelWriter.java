package com.hazelcast.internal.networking.udpspinning;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.networking.ChannelErrorHandler;
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.internal.networking.ChannelOutboundHandler;
import com.hazelcast.internal.networking.InitResult;
import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Packet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static com.hazelcast.util.QuickMath.modPowerOfTwo;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

public class UdpSpinningChannelWriter extends AbstractHandler {

    private final AtomicReferenceFieldUpdater<UdpSpinningChannelWriter, Buffer> DIRTY_BUFFER
            = newUpdater(UdpSpinningChannelWriter.class, Buffer.class, "dirtyBuffer");

    private final Buffer[] buffers = new Buffer[2];
    private final ChannelInitializer initializer;

    private volatile long lastWriteTimeMillis;

    private volatile Buffer writeBuffer;
    private volatile Buffer readBuffer;

    // the writer index is only modified by the write thread that ran into FULL_FIRST
    private volatile long writerBufferIndex;

    // will only be modified by the io thread.
    private long readBufferIndex;

    @Probe(name = "bytesWritten")
    private final SwCounter bytesWritten = newSwCounter();

    // Contains the buffer that needs to be cleaned
    // is set to a not null value by the io thread, and cas to zero by a writing-thread.
    // So the io thread will set this when a buffer has been written/read and needs to be cleaned.
    // we don't do this on the IO thread; so we'll let a write thread run into this.
    private volatile Buffer dirtyBuffer;
    private Object outboundHandler;


    public UdpSpinningChannelWriter(SpinningUdpChannel channel, ILogger logger, ChannelErrorHandler errorHandler,
                                    ChannelInitializer initializer) {
        super(channel, logger, errorHandler);

        for (int k = 0; k < buffers.length; k++) {
            buffers[k] = new Buffer(32 * 1024);
            buffers[k].channel = channel;
        }

        this.writeBuffer = buffers[0];
        this.readBuffer = buffers[0];
        this.initializer = initializer;
    }

    // called by user threads, operation threads etc.
    public void write(OutboundFrame frame) {
        Packet packet = (Packet) frame;

        for (; ; ) {

            cleanBufferAssist();

            Buffer.WriteResult writeResult = writeBuffer.write(packet);
            switch (writeResult) {
                case OK:
                    return;
                case FULL_FIRST:
                    // the current write buffer is full; and we are the ones making it full; now we need to rotate

                    writerBufferIndex++;
                    Buffer nextWriteBuffer = buffers[(int) modPowerOfTwo(writerBufferIndex, 2)];

                    //System.out.println(channel + ":first full,  switching to write buffer:" + nextWriteBuffer);

                    // we need to wait for it to be cleaned

                    long startMillis = System.currentTimeMillis();

                    for (; ; ) {
                        //System.out.println(channel + ":loop");
                        if (nextWriteBuffer.writePos() == 0) {
                            // the active buffer is ready for use; we are done
                            break;
                        }

                        // the buffer isn't ready to use; but lets do a clean; this could make the buffer ready to use.
                        cleanBufferAssist();

                        if (System.currentTimeMillis() - startMillis > SECONDS.toMillis(10)) {
                            startMillis = System.currentTimeMillis();
                            logger.info("Waiting for buffer to be cleared: "+nextWriteBuffer+" dirtyBuffer:"+dirtyBuffer+ " readBuffer:"+readBuffer);
                        }
                    }

                    // and now we publish the buffer; any subsequent write will see this new buffer
                    writeBuffer = nextWriteBuffer;
                    break;
                case FULL:
                    break;
                default:
                    throw new RuntimeException();
            }
        }
    }

    private void cleanBufferAssist() {
        Buffer dirtyBuffer = this.dirtyBuffer;
        if (dirtyBuffer != null && DIRTY_BUFFER.compareAndSet(this, dirtyBuffer, null)) {
            //System.out.println(channel + ": clearing "+dirtyBuffer);
            dirtyBuffer.clear();
        }
    }


    private boolean switched = false;

    // one thread calling this all the time.
    public void handle() throws Exception {
        //Thread.sleep(1000);

        if (channel.isClosed()) {
            return;
        }

        if (outboundHandler == null && !init()) {
            return;
        }

        ByteBuffer bb = readBuffer.drain();
        if (bb == null) {
            //   Thread.sleep(100);

            switched = true;
            // signals that the buffer should be cleaned by a writing thread; we don't want to do this on the IO thread.
            if (dirtyBuffer != null) {
                throw new RuntimeException("dirtyBuffer already set");
            }

            DIRTY_BUFFER.set(this, readBuffer);

            readBufferIndex++;
            readBuffer = buffers[(int) modPowerOfTwo(readBufferIndex, 2)];

            //System.out.println(channel + ":switching readbuffer to " + readBuffer);

            // retry.
            return;
        } else if (!bb.hasRemaining()) {
//            if (switched) {
//                Buffer other = buffers[(int) modPowerOfTwo(readBufferIndex+1, 2)];
//
//                //todo: UdpNioChannel{/10.8.0.10:5701->/10.8.0.10:43509} nothing to read, rb.writePos 8005 rb.readPos:26767 other.writePos 37668 other.readPos:29962
//                // how can readPos be larger than wrotePos
//                // is the readPos reset?
//
//                System.out.println(channel
//                                + " nothing to read, rb.writePos " + readBuffer.writePos() + " rb.readPos:" + readBuffer.readPos());
////                                + " other.writePos " + other.writePos() + " other.readPos:" + other.readPos());
//
//
            //            Thread.sleep(100);
//            }
            // there is no data to write to the socket.
            return;
        }

        int written = datagramChannel.write(bb);
        //  System.out.println(channel + " writing data to channel:" + written + " bytes");
        bytesWritten.inc(written);
        this.lastWriteTimeMillis = currentTimeMillis();
    }


    /**
     * Tries to initialize.
     *
     * @return true if initialization was a success, false if insufficient data is available.
     * @throws IOException
     */
    private boolean init() throws IOException {
        InitResult<ChannelOutboundHandler> init = initializer.initOutbound(channel);
        if (init == null) {
            // we can't initialize the outbound-handler yet since insufficient data is available.
            return false;
        }

        ByteBuffer bb = init.getByteBuffer();
        bb.flip();
        datagramChannel.write(bb);
        this.outboundHandler = init.getHandler();
        return true;
    }

    public long lastWriteTimeMillis() {
        return lastWriteTimeMillis;
    }

    @Override
    public String toString() {
        return "UdpSpinningChannelWriter{" +
                "channel=" + channel +
                ", writeBuffer=" + writeBuffer +
                ", readBuffer=" + readBuffer +
                ", bytesWritten=" + bytesWritten.get() +
                ", dirtyBuffer=" + dirtyBuffer +
                '}';
    }
}
