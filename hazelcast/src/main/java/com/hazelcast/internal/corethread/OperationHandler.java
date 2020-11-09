package com.hazelcast.internal.corethread;

import com.hazelcast.internal.nio.Packet;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.impl.operationservice.impl.OutboundResponseHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.function.Consumer;

import static com.hazelcast.internal.nio.Packet.FLAG_OP_CONTROL;
import static com.hazelcast.internal.nio.Packet.FLAG_OP_RESPONSE;
import static com.hazelcast.internal.util.HashUtil.hashToIndex;

/**
 * Responsible for processing an operation.
 *
 * Currently there is no thread-safety within a partition. Every {@link CoreThread} can process any
 * partition without any form of synchronization.
 */
public class OperationHandler extends SimpleChannelInboundHandler<Packet> {

    private final Consumer<Packet> inboundResponseHandler;
    private final Consumer<Packet> invocationMonitor;
    private final OperationRunner[] partitionRunners;
    private final OperationRunner[] genericRunners;
    private final OutboundResponseHandler outboundResponseHandler;
    private final boolean batch;

    public OperationHandler(OperationService os, boolean batch) {
        this.batch = batch;
        OperationServiceImpl operationService = (OperationServiceImpl) os;
        this.inboundResponseHandler = operationService.getInboundResponseHandlerSupplier().get();
        this.outboundResponseHandler = operationService.getOutboundResponseHandler();
        this.invocationMonitor = operationService.getInvocationMonitor();
        this.genericRunners = operationService.getOperationExecutor().getGenericOperationRunners();
        this.partitionRunners = operationService.getOperationExecutor().getPartitionOperationRunners();
    }

    private OperationRunner getRunner(Packet packet) {
        int partitionId = packet.getPartitionId();
        if (partitionId < 0) {
            return genericRunners[0];
        } else {
            return partitionRunners[hashToIndex(partitionId, partitionRunners.length)];
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Packet packet) {
        try {
            if (packet.isFlagRaised(FLAG_OP_RESPONSE)) {
                inboundResponseHandler.accept(packet);
            } else if (packet.isFlagRaised(FLAG_OP_CONTROL)) {
                invocationMonitor.accept(packet);
            } else {
                acceptOperation(ctx, packet);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void acceptOperation(ChannelHandlerContext ctx, Packet packet) throws Exception {
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
