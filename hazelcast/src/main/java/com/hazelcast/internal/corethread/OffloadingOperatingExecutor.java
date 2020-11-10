package com.hazelcast.internal.corethread;

import com.hazelcast.internal.nio.Packet;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import io.netty.channel.ChannelHandlerContext;

/**
 * An OperationExecutor that sends the operation to the thread that owns the partition.
 */
public class OffloadingOperatingExecutor extends OperationExecutor {

    private final int partitionCount;

    public OffloadingOperatingExecutor(OperationService os, boolean batch) {
        super(os, batch);
        this.partitionCount = operationService.getNode().partitionService.getPartitionCount();
    }

    protected void acceptOperation(ChannelHandlerContext ctx, Packet packet) throws Exception {

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
