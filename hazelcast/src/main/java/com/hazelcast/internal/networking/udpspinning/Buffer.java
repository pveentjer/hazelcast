package com.hazelcast.internal.networking.udpspinning;

import com.hazelcast.internal.memory.impl.UnsafeUtil;
import com.hazelcast.nio.Packet;
import sun.misc.Unsafe;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.nio.Bits.CHAR_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;

public class Buffer {

    private static final int OFFSET_FLAGS = 1;
    private static final int OFFSET_PARTITION_ID = OFFSET_FLAGS + CHAR_SIZE_IN_BYTES;
    private static final int OFFSET_SIZE = OFFSET_PARTITION_ID + INT_SIZE_IN_BYTES;
    private static final int OFFSET_PAYLOAD = OFFSET_SIZE + INT_SIZE_IN_BYTES;
    private static final Unsafe UNSAFE = UnsafeUtil.UNSAFE;
    private static final long ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

    private final ByteBuffer byteBuffer;
    private final long baseAddress;
    private final int capacity;

    private final AtomicInteger writePos = new AtomicInteger();
    private int readPos;

    public Buffer(int capacity) {
        this.capacity = capacity;
        this.byteBuffer = ByteBuffer.allocateDirect(capacity);
        this.baseAddress = ((sun.nio.ch.DirectBuffer) byteBuffer).address();
    }

    /**
     * True if the bytes have been written, false if there was no space to write the bytes.
     *
     * This call can be done concurrently.
     *
     * todo: probably we need 3 return values, success, failure first, failure subsequent
     *
     * @param packet
     * @return
     */
    public boolean write(Packet packet) {
        int packetSize = packet.packetSize();

        int lastPos = writePos.addAndGet(packetSize);

        if (lastPos >= capacity) {
            // not enough space in the buffer to write the packet.
            //todo: we need to determine if we are the first one.
            // we can decide that based on the packetIndex. If that is in front; of the buffer.length; we are responsible

            return false;
        }

        // the first byte of the packet in underlying buffer
        long packetIndex = lastPos - packetSize;
        // the address of the packet into the buffer.
        long packetAddress = baseAddress + packetIndex;

        // version
        UNSAFE.putByte(packetAddress, Packet.VERSION);
        // flags
        UNSAFE.putChar(packetAddress + OFFSET_FLAGS, packet.getFlags());
        // partition-id
        UNSAFE.putInt(packetAddress + OFFSET_PARTITION_ID, Integer.reverseBytes(packet.getPartitionId()));
        // payload
        byte[] src = packet.toByteArray();
        UNSAFE.copyMemory(src, ARRAY_BASE_OFFSET, null, packetAddress + OFFSET_PAYLOAD, src.length);

        // the last one copied is the size;
        // this signals the write is complete and provides a happens before relation between the writer of the buffer
        // and the reader (io-out-thread)
        UNSAFE.putIntVolatile(null, packetAddress + OFFSET_SIZE, Integer.reverseBytes(src.length));

        return true;
    }

    public void clear() {

        writePos.lazySet(0);
    }

    /**
     * Reads.
     *
     * The read either finds:
     * - data to read
     * - no more data to read; do we need to make difference between
     *
     * The read can use the byte array; no copying is needed.
     * The read can commit up to some point.
     * The buffer doesn't need to be cleared.
     *
     * @return
     */
    public ByteBuffer fill() {
        if (readPos > capacity) {

        }

        long packetAddress = baseAddress + readPos;
        long sizeAddress = packetAddress + OFFSET_SIZE;

        int size = Integer.reverseBytes(UNSAFE.getIntVolatile(null, sizeAddress));

        if (size == 0) {
            // the write has not yet committed; so we are done.
            return null;
        }

        int packetLength = size + OFFSET_SIZE + INT_SIZE_IN_BYTES;

        byteBuffer.limit(readPos + packetLength);
        byteBuffer.position(readPos);
        // we increase the readPos for a subsequent read.
        readPos += packetLength;

        // how does the byte-buffer know to what point it can read?
        // we do; we can just traverse the length fields...

        // if the read isn't complete, don't wait for it to complete. Just let go and try again later since spinning anyway
        return byteBuffer;
    }
}
