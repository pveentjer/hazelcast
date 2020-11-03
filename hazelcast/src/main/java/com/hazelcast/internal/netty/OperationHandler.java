package com.hazelcast.internal.netty;

import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.util.HashUtil;
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
            return partitionRunners[HashUtil.hashToIndex(partitionId, partitionRunners.length)];
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
                OperationRunner runner = getRunner(packet);
                Operation operation = runner.toOperation(packet);
                operation.setOperationResponseHandler((op, response) -> {
                    Packet responsePacket = outboundResponseHandler.toResponse(operation, response);
                    if (batch) {
                        ctx.write(responsePacket);
                    } else {
                        ctx.writeAndFlush(responsePacket);
                    }

                });
                runner.run(operation);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        super.channelReadComplete(ctx);
        if (batch) {
            ctx.flush();
        }
    }
}
