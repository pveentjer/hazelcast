package com.hazelcast.internal.networking.aeron;

import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.impl.PacketHandler;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

import static com.hazelcast.nio.Packet.VERSION;
import static com.hazelcast.nio.PacketIOHelper.HEADER_SIZE;

public class MemberFragmentHandler implements FragmentHandler {

    private final Connection connection;
    private final PacketHandler handler;
    private int packetsProcessed;

    private int dstOffset;
    private int dstLength;


    private boolean headerComplete;
    private char flags;
    private int partitionId;
    private byte[] payload;

    public MemberFragmentHandler(Connection connection, PacketHandler handler) {
        System.out.println("connection:" + connection);
        this.connection = connection;
        this.handler = handler;
    }

    @Override
    public void onFragment(DirectBuffer src, int srcOffset, int srcLength, Header header) {
        if (!headerComplete) {
            if (srcLength < HEADER_SIZE) {
                return;
            }

            byte version = src.getByte(srcOffset);
            if (VERSION != version) {
                throw new IllegalArgumentException("Packet versions are not matching! Expected -> "
                        + VERSION + ", Incoming -> " + version);
            }

            flags = src.getChar(srcOffset + 1);
            partitionId = src.getInt(srcOffset + 3);
            dstLength = src.getInt(srcOffset + 7);
            headerComplete = true;
            payload = new byte[dstLength];
            srcOffset += 11;
            srcLength -= 11;
        }

        if (!readValue(src, srcOffset, srcLength)) {
            //System.out.println("value not read");
            return;
        }

        Packet packet = new Packet(payload, partitionId)
                .resetFlagsTo(flags)
                .setConn(connection);

        reset();
        packetsProcessed++;
        try {
        //    System.out.println("Packets processed " + packetsProcessed);
            handler.handle(packet);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void reset() {
        headerComplete = false;
        payload = null;
        dstOffset = 0;
    }

    private boolean readValue(DirectBuffer src, int srcOffset, int srcLength) {
        if (dstLength == 0) {
            return true;
        }

        int bytesNeeded = dstLength - dstOffset;

        boolean done;
        int bytesToRead;
        if (srcLength >= bytesNeeded) {
            bytesToRead = bytesNeeded;
            done = true;
        } else {
            bytesToRead = srcLength;
            done = false;
        }

        // read the data from the byte-buffer into the bytes-array.
        src.getBytes(srcOffset, payload, dstOffset, bytesToRead);
        dstOffset += bytesToRead;
        return done;
    }
}
