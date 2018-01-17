package com.hazelcast.internal.networking.aeron;

import com.hazelcast.internal.networking.AbstractChannel;
import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.nio.Packet;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.channels.SocketChannel;

public class AeronChannel extends AbstractChannel {

    public Publication publication;
    public Subscription subscription;

    public final static ThreadLocal<UnsafeBuffer> BUFFER = new ThreadLocal<UnsafeBuffer>() {
        @Override
        protected UnsafeBuffer initialValue() {
            return new UnsafeBuffer(BufferUtil.allocateDirectAligned(512 * 1024, 64));
        }
    };
    public FragmentHandler handler;

    public AeronChannel(SocketChannel channel, boolean clientMode) {
        super(channel, clientMode);
    }

    @Override
    public long lastReadTimeMillis() {
        return 0;
    }

    @Override
    public long lastWriteTimeMillis() {
        return 0;
    }

    @Override
    public boolean write(OutboundFrame frame) {
        Packet packet = (Packet) frame;
        //System.out.println("writing packet:" + packet + " with payload size:" + ((Packet) frame).toByteArray().length);

        UnsafeBuffer buffer = BUFFER.get();
        buffer.putByte(0, Packet.VERSION);
        buffer.putChar(1, packet.getFlags());
        buffer.putInt(3, packet.getPartitionId());
        byte[] payload = packet.toByteArray();
        buffer.putInt(7, payload.length);
        buffer.putBytes(11, payload);


        for (; ; ) {
            // System.out.println("before publication.pos:"+publication.position());
            final long result = publication.offer(buffer, 0, packet.packetSize());
            // System.out.println("result:" + result);
            //  System.out.println("after publication.pos:"+publication.position());


            if (result < 0L) {
                if (result == Publication.BACK_PRESSURED) {
                    System.out.println("Offer failed due to back pressure");
                } else if (result == Publication.NOT_CONNECTED) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println("Offer failed because publisher is not connected to subscriber");
                } else if (result == Publication.ADMIN_ACTION) {
                    System.out.println("Offer failed because of an administration action in the system");
                } else if (result == Publication.CLOSED) {
                    System.out.println("Offer failed publication is closed");
                    //   break;
                } else if (result == Publication.MAX_POSITION_EXCEEDED) {
                    System.out.println("Offer failed due to publication reaching max position");
                    // break;
                } else {
                    System.out.println("Offer failed due to unknown reason");
                }
            }else{
                break;
            }
        }

        return true;
    }

    @Override
    public void flush() {

    }

    @Override
    public String toString() {
        String s = "AeronChannel{" + getLocalSocketAddress() + "->" + getRemoteSocketAddress() + '}';
        if (!isClientMode()) {
            s = "             " + s;
        }
        return s;
    }
}
