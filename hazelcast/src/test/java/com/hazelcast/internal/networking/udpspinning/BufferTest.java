package com.hazelcast.internal.networking.udpspinning;

import com.hazelcast.nio.Packet;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static com.hazelcast.test.HazelcastTestSupport.generateRandomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by alarmnummer on 5/30/17.
 */
public class BufferTest {

    private Buffer buffer;

    @Before
    public void setup() {
        buffer = new Buffer(1024 * 32);
    }

    @Test
    public void test() {
        Packet originalPacket = new Packet(generateRandomString(10).getBytes(), 43);
        buffer.write(originalPacket);
        System.out.println("write complete");

        ByteBuffer bb = buffer.fill();
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
        System.out.println("write complete");

        ByteBuffer bb = buffer.fill();
        assertNotNull(bb);
        Packet c1 = new Packet();
        c1.readFrom(bb);
        assertEquals(o1, c1);

        bb = buffer.fill();
        assertNotNull(bb);
        Packet c2 = new Packet();
        c2.readFrom(bb);
        assertEquals(o2, c2);
    }

    @Test
    public void test3() {
        Packet o1 = new Packet(generateRandomString(10).getBytes(), 43);
        buffer.write(o1);
        System.out.println("write complete");

        ByteBuffer bb = buffer.fill();
        assertNotNull(bb);
        Packet c1 = new Packet();
        c1.readFrom(bb);
        assertEquals(o1, c1);

        System.out.println("------------------------------");

        Packet o2 = new Packet(generateRandomString(10).getBytes(), 42);
        buffer.write(o2);
        bb = buffer.fill();
        assertNotNull(bb);
        Packet c2 = new Packet();
        c2.readFrom(bb);
        assertEquals(o2, c2);
    }
}
