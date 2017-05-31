package com.hazelcast.internal.networking.udpspinning;

import com.hazelcast.nio.Packet;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import static com.hazelcast.internal.networking.udpspinning.Buffer.OFFSET_SIZE;
import static com.hazelcast.internal.networking.udpspinning.Buffer.WriteResult.FULL;
import static com.hazelcast.internal.networking.udpspinning.Buffer.WriteResult.FULL_FIRST;
import static com.hazelcast.internal.networking.udpspinning.Buffer.WriteResult.OK;
import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Packet.Type.OPERATION;
import static com.hazelcast.test.HazelcastTestSupport.generateRandomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class BufferTest {

    private Buffer buffer;

    @Before
    public void setup() {
        buffer = new Buffer(1024 * 32);
    }

    @Test
    public void testWhenFull() {
        Packet p1 = new Packet(generateRandomString(20 * 1000).getBytes(), 43).setPacketType(OPERATION);
        assertEquals(OK, buffer.write(p1));

        Packet p2 = new Packet(generateRandomString(20 * 1000).getBytes(), 43).setPacketType(OPERATION);
        assertEquals(FULL_FIRST, buffer.write(p2));

        Packet p3 = new Packet(generateRandomString(20 * 1000).getBytes(), 43).setPacketType(OPERATION);
        assertEquals(FULL, buffer.write(p3));
    }

    @Test
    public void testWrite_whenOverflow_andEnoughSpaceToWriteSize() {
        buffer.write(new Packet(generateRandomString(30 * 1000).getBytes(), 43));

        long pos = buffer.writePos();
        Packet packet = new Packet(generateRandomString(20 * 1000).getBytes(), 43);

        Buffer.WriteResult result = buffer.write(packet);
        assertEquals(FULL_FIRST, result);
        assertEquals(pos + packet.packetSize(), buffer.writePos());
        assertEquals(-1, buffer.getIntVolatile((int)pos + OFFSET_SIZE));
    }

    @Test
    public void testWrite_whenOverflow_andNotEnoughSpaceToWriteSize() {
        int bufferCapacity = buffer.capacity();
        int packetLength = bufferCapacity - 1;
        int size = packetLength - OFFSET_SIZE - INT_SIZE_IN_BYTES;
        assertEquals(OK, buffer.write(new Packet(new byte[size])));
        assertEquals(bufferCapacity-1, buffer.writePos());

        Packet packet = new Packet(new byte[100]);
        long pos = buffer.writePos();
        Buffer.WriteResult result = buffer.write(packet);

        assertEquals(FULL_FIRST, result);
        assertEquals(pos + packet.packetSize(), buffer.writePos());
    }

    @Test
    public void testWrite_whenPacketCompletelyFillsBuffer() {
//        int bufferCapacity = buffer.capacity();
//        int packetLength = bufferCapacity - 1;
//        int size = packetLength - OFFSET_SIZE + INT_SIZE_IN_BYTES;
//        int pos = buffer.writePos();
//        Packet packet = new Packet(new byte[size]);
//
//        Buffer.WriteResult result = buffer.write(packet);
//
//        assertEquals(FULL_FIRST, result);
//        assertEquals(pos + packet.packetSize(), buffer.writePos());
//        assertEquals(-1, buffer.getIntVolatile(pos + OFFSET_SIZE));
    }

    @Test
    public void testWrite_whenOverflow_whenAlreadyOverflowed() {
        buffer.write(new Packet(generateRandomString(20 * 1000).getBytes(), 43));
        buffer.write(new Packet(generateRandomString(20 * 1000).getBytes(), 43));

        long pos = buffer.writePos();
        Packet packet = new Packet(generateRandomString(20 * 1000).getBytes(), 43);

        Buffer.WriteResult writeResult = buffer.write(packet);

        assertEquals(FULL, writeResult);
        assertEquals(pos + packet.packetSize(), buffer.writePos());
    }

    @Test
    public void drainWhenEmpty() {
        ByteBuffer bb = buffer.drain();

        assertNotNull(bb);
        assertEquals(0, bb.remaining());
    }

    @Test
    public void drainWhenEmpty2() {
        Packet p = new Packet(generateRandomString(20 * 1000).getBytes(), 43).setPacketType(OPERATION);
        buffer.write(p);

        // this will empty the buffer.
        buffer.drain();

        // and now we run into an empty buffer
        ByteBuffer bb = buffer.drain();
        assertNotNull(bb);
        assertEquals(0, bb.remaining());
    }

    @Test
    public void fillAndDrain() {
        List<Packet> writtenPackets = new LinkedList<>();
        for (; ; ) {
            Packet p = new Packet(generateRandomString(1000).getBytes(), 43).setPacketType(OPERATION);
            Buffer.WriteResult writeResult = buffer.write(p);
            if (writeResult == OK) {
                writtenPackets.add(p);
            } else if (writeResult == FULL_FIRST) {
                break;
            } else {
                throw new IllegalArgumentException();
            }
        }

        List<Packet> readPackets = new LinkedList<>();
        for (; ; ) {
            ByteBuffer bb = buffer.drain();
            if (bb == null) {
                break;
            }
            Packet packet = new Packet();
            packet.readFrom(bb);
            readPackets.add(packet);
        }

        assertEquals(writtenPackets, readPackets);
    }

    @Test
    public void test() {
        Packet originalPacket = new Packet(generateRandomString(10).getBytes(), 43)
                .setPacketType(OPERATION);
        assertEquals(OK, buffer.write(originalPacket));

        ByteBuffer bb = buffer.drain();
        assertNotNull(bb);

        Packet clonedPacket = new Packet();
        clonedPacket.readFrom(bb);

        assertEquals(originalPacket, clonedPacket);
    }

    @Test
    public void test2() {
        Packet o1 = new Packet(generateRandomString(10).getBytes(), 43);
        Packet o2 = new Packet(generateRandomString(10).getBytes(), 42);
        buffer.write(o1);
        buffer.write(o2);

        ByteBuffer bb = buffer.drain();
        assertNotNull(bb);
        Packet c1 = new Packet();
        c1.readFrom(bb);
        assertEquals(o1, c1);

        bb = buffer.drain();
        assertNotNull(bb);
        Packet c2 = new Packet();
        c2.readFrom(bb);
        assertEquals(o2, c2);
    }

    @Test
    public void test3() {
        Packet o1 = new Packet(generateRandomString(10).getBytes(), 43);
        buffer.write(o1);

        ByteBuffer bb = buffer.drain();
        assertNotNull(bb);
        Packet c1 = new Packet();
        c1.readFrom(bb);
        assertEquals(o1, c1);

        Packet o2 = new Packet(generateRandomString(10).getBytes(), 42);
        buffer.write(o2);
        bb = buffer.drain();
        assertNotNull(bb);
        Packet c2 = new Packet();
        c2.readFrom(bb);
        assertEquals(o2, c2);
    }
}
