package com.hazelcast.internal.networking.udpspinning;

import com.hazelcast.internal.memory.impl.UnsafeUtil;
import com.hazelcast.nio.Packet;
import sun.misc.Unsafe;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.internal.networking.udpspinning.Buffer.WriteResult.FULL;
import static com.hazelcast.internal.networking.udpspinning.Buffer.WriteResult.FULL_FIRST;
import static com.hazelcast.internal.networking.udpspinning.Buffer.WriteResult.OK;
import static com.hazelcast.nio.Bits.CHAR_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;

public class Buffer {

    private final static AtomicLong ID = new AtomicLong();

    static final int OFFSET_FLAGS = 1;
    static final int OFFSET_PARTITION_ID = OFFSET_FLAGS + CHAR_SIZE_IN_BYTES;
    static final int OFFSET_SIZE = OFFSET_PARTITION_ID + INT_SIZE_IN_BYTES;
    static final int OFFSET_PAYLOAD = OFFSET_SIZE + INT_SIZE_IN_BYTES;
    private static final Unsafe UNSAFE = UnsafeUtil.UNSAFE;
    private static final long ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

    private final ByteBuffer byteBuffer;
    private final long baseAddress;
    private final int capacity;

    // an long is needed instead of an int because an int can wrap and then a thread could falsely assume it has claimed space.
    private final AtomicLong writeIndex = new AtomicLong();
    private final int endIndex;
    private volatile int readIndex;
    private final long id = ID.incrementAndGet();
    private byte[] nullArray;

    public Buffer(int capacity) {
        this.nullArray = new byte[capacity];
        this.capacity = capacity;
        this.endIndex = capacity - 1;
        this.byteBuffer = ByteBuffer.allocateDirect(capacity);
        this.baseAddress = ((sun.nio.ch.DirectBuffer) byteBuffer).address();
    }

    public int capacity() {
        return capacity;
    }

    public long writePos() {
        return writeIndex.get();
    }

    public int readPos() {
        return readIndex;
    }

    enum WriteResult {
        OK, FULL_FIRST, FULL
    }

    public WriteResult write(Packet packet) {
        // total size of the packet including the header.
        int packetSize = packet.packetSize();

        long packetStartIndex = writeIndex.getAndAdd(packetSize);
        long packetEndIndex = packetStartIndex + packetSize;
        if (packetEndIndex > endIndex) {
            return onOverflow(packetStartIndex);
        }

        // the address of the packet into the buffer.
        long packetAddress = baseAddress + packetStartIndex;

        // version
        UNSAFE.putByte(packetAddress, Packet.VERSION);
        // flags
        UNSAFE.putChar(packetAddress + OFFSET_FLAGS, Character.reverseBytes(packet.getFlags()));
        // partition-id
        UNSAFE.putInt(packetAddress + OFFSET_PARTITION_ID, Integer.reverseBytes(packet.getPartitionId()));
        // payload
        byte[] src = packet.toByteArray();
        UNSAFE.copyMemory(src, ARRAY_BASE_OFFSET, null, packetAddress + OFFSET_PAYLOAD, src.length);

        // the last one copied is the size of the payload.
        // this signals the write is complete and provides a happens before relation between the writer of the buffer
        // and the reader (io-out-thread)
        UNSAFE.putIntVolatile(null, packetAddress + OFFSET_SIZE, Integer.reverseBytes(src.length));

        return OK;
    }

    int getIntVolatile(int index) {
        return UNSAFE.getIntVolatile(null, baseAddress + index);
    }

    private WriteResult onOverflow(long packetStartIndex) {
        long bytesGranted = endIndex - packetStartIndex;

        if (bytesGranted < 0) {
            // there is no space remaining.
            return FULL;
        }

        if (bytesGranted >= OFFSET_SIZE + INT_SIZE_IN_BYTES) {
           // System.out.println(channel + " " + id + " onOverflow, enough space to write value");
            // there is enough space in the buffer available to write the size
            long packetAddress = baseAddress + packetStartIndex;

            // we need to mark the buffer as full, by setting -1 as size.
            UNSAFE.putIntVolatile(null, packetAddress + OFFSET_SIZE, Integer.reverseBytes(-1));
        } else {
            //System.out.println(channel + " " + id + " onOverflow, not enough space to write value");
        }

        return FULL_FIRST;
    }

    public void clear() {
        if (readIndex != -1) {
            throw new IllegalArgumentException("Cleaning buffer used for reading");
        }

        UNSAFE.copyMemory(nullArray, ARRAY_BASE_OFFSET, null, baseAddress, capacity);

        // only the io thread will be the one setting the buffer again to a value

        // from this point on the buffer can be used again.
        writeIndex.set(0);

        readIndex = 0;
    }

    public SpinningUdpChannel channel;

    /**
     * Reads. The system should keep on reading until the.
     *
     * todo:
     * Imagine a buffer has 100 bytes left and a write of 200 bytes is done; then we'll get an overflow on the buffer. But
     * since it doesnt' fit; the length field isn't set. So currently we don't know the difference between:
     * - maybe a write will follow at some point in time
     * - the buffer has a gap at the end
     * This problem can be solved by letting the write thread place e.g. -1 as size as a marker.
     *
     * If the ByteBuffer is at the end, it will return null.
     * Otherwise it will return a buffer; which can potentially be empty.
     *
     * @return
     */
    public ByteBuffer drain() {
        if (readIndex == -1) {
            byteBuffer.position(0);
            byteBuffer.limit(0);
            return byteBuffer;
        }

        if (readIndex > writePos()) {
            throw new RuntimeException();
        }

        if (readIndex > endIndex - OFFSET_SIZE - INT_SIZE_IN_BYTES) {
            // todo: the problem here is that we are about to clear a buffer, that hasnt been fully written. We know that no
            // sensible write can be done, but we need to wait for the buffer to over commit.

            if (writePos() < endIndex) {
                byteBuffer.position(0);
                byteBuffer.limit(0);
                return byteBuffer;
            }

            // we are the end of the buffer.
           // System.out.println(channel + " " + id + ":drain null because not enough space, readIndex:" + readIndex + " writePos:" + writePos());
            readIndex = -1;
            return null;
        }

        long packetAddress = baseAddress + readIndex;
        long sizeAddress = packetAddress + OFFSET_SIZE;

        int size = Integer.reverseBytes(UNSAFE.getIntVolatile(null, sizeAddress));

        if (size == -1) {
            // the buffer has been closed by the last writer.
            //System.out.println(channel + " " + this + ":drain null because -1 size found, readIndex:" + readIndex + " writePos:" + writePos());
            readIndex = -1;
            return null;
        } else if (size == 0) {
            // no data is available, so lets return the empty bytebuffer.
            byteBuffer.position(0);
            byteBuffer.limit(0);
            return byteBuffer;
        }

        int packetLength = size + OFFSET_SIZE + INT_SIZE_IN_BYTES;

        byteBuffer.limit(readIndex + packetLength);
        byteBuffer.position(readIndex);
        // we increase the readStartIndex for a subsequent read.
        readIndex += packetLength;

        // if the read isn't complete, don't wait for it to complete. Just let go and try again later since spinning anyway
        return byteBuffer;
    }

    @Override
    public String toString() {
        return "Buffer{" +
                "id=" + id +
                ", writeIndex=" + writeIndex +
                ", readIndex=" + readIndex +
                '}';
    }
}
