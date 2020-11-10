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
 *
 * Execution options:
 * 1) synchronize the partition
 * 2) always send the operation to the right thread
 * 3) offload the request to the thread responsible for a given partition.
 *
 *
 * <h1>Synchronize the partition</h1>
 * So you synchronize the partition.
 *
 * Advantage:
 * you don't need many connections.
 *
 * <h1>Send request to the right CPU</h1>
 *
 * Disadvantage:
 * You need as many connections as CPUs.
 *
 * The advantage is that there is no contention and no cache coherence traffic. It is truly a thread per core approach.
 *
 * If a request is send to a CPU where it doesn't belong, a redirect could be send.
 *
 * <h1>Offload</h1>
 * Send a request that doesn't belong, to the CPU where it belongs.
 *
 * Advantage:
 * you don't need many connections.
 *
 * <h1>Send to right NUMA node</h1>
 *
 * So this either requires synchronization or offloading, but you keep the request in the right NUMA node. So you need
 * to have at least 1 connection per NUMA node and if you get a connect to the wrong NUMA node, it should be redirected.
 */
public abstract class OperationExecutor extends SimpleChannelInboundHandler<Packet> {

    protected final Consumer<Packet> inboundResponseHandler;
    protected final Consumer<Packet> invocationMonitor;
    protected final OperationRunner[] partitionRunners;
    protected final OperationRunner[] genericRunners;
    protected final OutboundResponseHandler outboundResponseHandler;
    protected final boolean batch;
    protected final OperationServiceImpl operationService;

    public OperationExecutor(OperationService os, boolean batch) {
        this.batch = batch;
        this.operationService = (OperationServiceImpl) os;
        this.inboundResponseHandler = operationService.getInboundResponseHandlerSupplier().get();
        this.outboundResponseHandler = operationService.getOutboundResponseHandler();
        this.invocationMonitor = operationService.getInvocationMonitor();
        this.genericRunners = operationService.getOperationExecutor().getGenericOperationRunners();
        this.partitionRunners = operationService.getOperationExecutor().getPartitionOperationRunners();
    }

    protected OperationRunner getRunner(Packet packet) {
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

    protected abstract void acceptOperation(ChannelHandlerContext ctx, Packet packet) throws Exception;
}
