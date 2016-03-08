
package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.bufferpool.BufferPool;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.impl.responses.Response;

import java.io.IOException;

import static com.hazelcast.nio.Packet.FLAG_OP;
import static com.hazelcast.nio.Packet.FLAG_RESPONSE;
import static com.hazelcast.nio.Packet.FLAG_URGENT;
import static com.hazelcast.nio.Packet.VERSION;


public class PacketBuilder {

    private final SerializationService serializationService;

    public PacketBuilder(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    public byte[] buildOperationPacket(Operation op, boolean urgent) {
        BufferPool bufferPool = serializationService.getThreadLocalBufferPool();
        BufferObjectDataOutput out = bufferPool.takeOutputBuffer();
        try {
            // version
            out.writeByte(VERSION);

            // flags
            if (urgent) {
                out.writeShort(FLAG_OP | FLAG_URGENT);
            } else {
                out.writeShort(FLAG_OP);
            }

            // partition id
            out.writeInt(op.getPartitionId());

            // size place-holder
            int sizePos = out.position();
            out.writeInt(0);

            // payload
            int dataStartPos = out.position();
            serializationService.write(out, op);

            // updating the size placeholder
            int size = out.position() - dataStartPos;
            out.writeInt(sizePos, size);

            return out.toByteArray();
        } catch (IOException e) {
            throw new HazelcastSerializationException(e);
        } finally {
            bufferPool.returnOutputBuffer(out);
        }
    }

    public byte[] buildResponsePacket(Response response) {
        BufferPool bufferPool = serializationService.getThreadLocalBufferPool();
        BufferObjectDataOutput out = bufferPool.takeOutputBuffer();
        boolean urgent = response.isUrgent();
        try {
            //version
            out.writeByte(VERSION);

            //flags
            if (urgent) {
                out.writeShort(FLAG_OP | FLAG_RESPONSE | FLAG_URGENT);
            } else {
                out.writeShort(FLAG_OP | FLAG_RESPONSE);
            }

            //partition-id
            out.writeInt(0);

            //size
            int sizePos = out.position();
            out.writeInt(0);

            //payload
            int dataStartPos = out.position();
            serializationService.write(out, response);

            // updating the size placeholder
            int size = out.position() - dataStartPos;
            out.writeInt(sizePos, size);

            return out.toByteArray();
        } catch (IOException e) {
            throw new HazelcastSerializationException(e);
        } finally {
            bufferPool.returnOutputBuffer(out);
        }
    }
}
