package com.hazelcast.internal.corethread;

import com.hazelcast.internal.nio.Packet;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import io.netty.channel.ChannelHandlerContext;

/**
 * Synchronizes on a partition locks. There are as many locks as there are partitions.
 *
 * So any thread can access any partition, but before it executes an operation on a partition, it needs to acquire
 * the partition lock.
 *
 * Currently nothing smart is done in case of contention; we just keep waiting for the lock to come available.
 */
public class PartitionLockOperationExecutor extends OperationExecutor {
    private final Object[] partitionLocks;

    public PartitionLockOperationExecutor(OperationService os, boolean batch, Object[] partitionLocks) {
        super(os, batch);
        this.partitionLocks = partitionLocks;
    }

    @Override
    protected void acceptOperation(ChannelHandlerContext ctx, Packet packet) throws Exception {
        Object partitionLock = packet.getPartitionId() == -1 ? null : partitionLocks[packet.getPartitionId()];

        if (partitionLock == null) {
            execute(ctx, packet);
        } else {
            synchronized (partitionLock) {
                execute(ctx, packet);
            }
        }
    }

    private void execute(ChannelHandlerContext ctx, Packet packet) throws Exception {
        OperationRunner runner = getRunner(packet);
        Operation operation = runner.toOperation(packet);
        operation.setOperationResponseHandler((op, response) -> {
            Packet responsePacket = outboundResponseHandler.toResponse(op, response);
            if (batch) {
                ctx.write(responsePacket);
            } else {
                ctx.writeAndFlush(responsePacket);
            }
        });
        runner.run(operation);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        super.channelReadComplete(ctx);

        if (batch) {
            ctx.flush();
        }
    }
}
