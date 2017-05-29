package com.hazelcast.internal.networking.udpspinning;

import com.hazelcast.internal.memory.impl.UnsafeUtil;
import com.hazelcast.nio.Packet;
import sun.misc.Unsafe;

import java.nio.ByteBuffer;

/**
 * Created by alarmnummer on 5/28/17.
 */
public class Buffer {

    private byte[] bytes = new byte[32 * 1024];
    private Unsafe unsafe = UnsafeUtil.UNSAFE;

    /**
     * True if the bytes have been written, false if there was no space to write the bytes.
     *
     * @param packet
     * @return
     */
    public boolean write(Packet packet) {

    }

    public ByteBuffer byteBuffer(){
        return null;
    }
}
