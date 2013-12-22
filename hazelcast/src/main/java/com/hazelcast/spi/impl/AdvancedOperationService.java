package com.hazelcast.spi.impl;

import com.hazelcast.cluster.ClusterServiceImpl;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.partition.PartitionView;
import com.hazelcast.spi.*;
import com.hazelcast.spi.exception.CallerNotMemberException;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.util.ValidationUtil.isNotNull;


/**
 * <h1>Disruptor</h1>
 * The PartitionOperationScheduler uses a disruptor to exchange work (operations) with operation threads.
 * <p/>
 * There are 2 differences:
 * <ol>
 * <li>
 * One difference is that the sequenceNumber of each slot, in my implementation is added to the Slot field
 * and in the disruptor implementation, there is a separate array with the same size as the ringbuffer called
 * </li>
 * <li>
 * Another difference is that in the sequence field we also encode if a PartitionOperationScheduler is scheduled to
 * a thread. This is needed for the caller runs optimization. With this optimization you need to be able to
 * atomically either publish the work and potentially set the scheduled flag, or to set the scheduled flag and
 * run the operation in the calling thread.
 * </li>
 * </ol>
 * <p/>
 * <h1>So why does the ForkJoinExecutor not help Hazelcast?</h1>
 * <p/>
 * Because instead of storing the work produced by an actor on the shared queue like a threadpool executor, each
 * worker thread has its private cheap work deque and when an actor sends a message to another actor, that actor is
 * assigned to the workerthread its workqueue. Other worker threads can steal work from private deque so that
 * there is still balancing of the load.
 * <p/>
 * So actors talking to actors, is a difference compared to Hazelcast. Where non hazelcast threads will be interacting
 * with a partition thread, but in most cases a partition thread will not interact with other partition threads. So
 * all the biggest part of the work send to the forkjoinpool, still needs to go through the expensive shared queue.
 * <p/>
 * <p/>
 * <p/>
 * <p/>
 * Documentation of unsafe:
 * http://www.docjar.com/docs/api/sun/misc/Unsafe.html
 * <p/>
 * More info about AtomicLong.lazySet instead of using AtomicLong.set
 * https://groups.google.com/forum/#!searchin/lmax-disruptor/thread/lmax-disruptor/PwnvICvrJQU/PgsxWiQCONQJ
 */
public class AdvancedOperationService extends AbstractOperationService {

    private final PartitionOperationScheduler[] schedulers;
    private final boolean localCallOptimizationEnabled;
    private final OperationThread[] operationThreads;

    //todo: we need to optimize this executor.
    private final Executor responseExecutor = Executors.newFixedThreadPool(10);
    private final Executor defaultExecutor = Executors.newFixedThreadPool(10);
    private final Address thisAddress;
    private final AtomicLong callIdGen = new AtomicLong(0);
    private final ConcurrentMap<Long, Operation> remoteOperations = new ConcurrentHashMap<>();
    private PartitionService partitionService;
    private ClusterServiceImpl clusterService;

    public AdvancedOperationService(NodeEngineImpl nodeEngine) {
        super(nodeEngine);

        this.thisAddress = nodeEngine.getThisAddress();
        if (thisAddress == null) {
            throw new RuntimeException("thisAddress can't be null");
        }
        int partitionCount = node.getGroupProperties().PARTITION_COUNT.getInteger();
        this.schedulers = new PartitionOperationScheduler[partitionCount];
        int ringbufferSize = 8192;
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            schedulers[partitionId] = new PartitionOperationScheduler(partitionId, ringbufferSize);
        }
        this.localCallOptimizationEnabled = false;

        this.operationThreads = new OperationThread[16];
        for (int k = 0; k < operationThreads.length; k++) {
            operationThreads[k] = new OperationThread(partitionCount);
            operationThreads[k].start();
        }
    }

    private void executeOnOperationThread(Runnable runnable) {
        //todo: we need a better mechanism for finding a suitable threadpool.
        operationThreads[0].offer(runnable);
    }

    public <E> InternalCompletableFuture<E> invokeOnPartition(String serviceName, Operation op, int partitionId,
                                                              long callTimeout, int replicaIndex, int tryCount, long tryPauseMillis,
                                                              Callback<Object> callback) {
        isNotNull(op, "operation");

        if (serviceName != null) {
            op.setServiceName(serviceName);
        }
        op.setPartitionId(partitionId);
        if (callback != null) {
            ResponseHandlerFactory.setLocalResponseHandler(op, callback);
        }

        Address target = getAddress(partitionId, replicaIndex);
        if (isLocal(target)) {
            PartitionOperationScheduler scheduler = schedulers[partitionId];
            scheduler.schedule(op);
        } else {
            long callId = callIdGen.incrementAndGet();
            remoteOperations.put(callId, op);

            OperationAccessor.setCallId(op, callId);
            OperationAccessor.setCallerAddress(op, thisAddress);
            OperationAccessor.setCallTimeout(op, callTimeout);
            //todo: we need to do something with return value.
            send(op, partitionId, replicaIndex);
        }

        return op;
    }

    private Address getAddress(int partitionId, int replicaIndex) {
        PartitionView partitionview = partitionService.getPartition(partitionId);
        return partitionview.getReplicaAddress(replicaIndex);
    }

    private boolean isLocal(Address thatAddress) {
        return thatAddress.equals(thisAddress);
    }

    @Override
    public <E> InternalCompletableFuture<E> invokeOnPartition(String serviceName, Operation op, int partitionId) {
        return invokeOnPartition(
                serviceName,
                op,
                partitionId,
                AbstractInvocationBuilder.DEFAULT_CALL_TIMEOUT,
                AbstractInvocationBuilder.DEFAULT_REPLICA_INDEX,
                AbstractInvocationBuilder.DEFAULT_TRY_COUNT,
                AbstractInvocationBuilder.DEFAULT_TRY_PAUSE_MILLIS,
                null);
    }

    public <E> InternalCompletableFuture<E> invokeOnTarget(String serviceName, Operation op, Address target,
                                                           long callTimeout, int tryCount, long tryPauseMillis,
                                                           Callback<Object> callback) {
        isNotNull(op, "operation");
        isNotNull(target, "target");

        if (serviceName != null) {
            op.setServiceName(serviceName);
        }
        if (callback != null) {
            ResponseHandlerFactory.setLocalResponseHandler(op, callback);
        }

        if (isLocal(target)) {
            op.setNodeEngine(nodeEngine);
            //todo: we should offload this call
            doRunOperation(op);
        } else {
            //System.out.println("InvokeOnTarget: " + op);

            long callId = callIdGen.incrementAndGet();
            remoteOperations.put(callId, op);
            OperationAccessor.setCallId(op, callId);
            //todo: we need to do something with return value.
            //todo: is this a blocking call?
            send(op, target);
        }
        return op;
    }

    @Override
    public <E> InternalCompletableFuture<E> invokeOnTarget(final String serviceName, final Operation op, final Address target) {
        return invokeOnTarget(
                serviceName,
                op,
                target,
                AbstractInvocationBuilder.DEFAULT_CALL_TIMEOUT,
                AbstractInvocationBuilder.DEFAULT_TRY_COUNT,
                AbstractInvocationBuilder.DEFAULT_TRY_PAUSE_MILLIS,
                null);
    }

    @Override
    public void handleOperation(final Packet packet) {
        //System.out.println("handleOperation:" + packet);

        try {
            if (packet.isHeaderSet(Packet.HEADER_RESPONSE)) {
                responseExecutor.execute(new RemoteOperationProcessor(packet));
            } else {
                final int partitionId = packet.getPartitionId();
                if (partitionId == -1) {
                    defaultExecutor.execute(new RemoteOperationProcessor(packet));
                } else {
                    schedulers[partitionId].schedule(packet);
                }
            }
        } catch (RejectedExecutionException e) {
            if (nodeEngine.isActive()) {
                throw e;
            }
        }
    }

    private void handleOperationError(Operation op, Exception error) {
        throw new RuntimeException(error);
    }

    @Override
    public void onMemberLeft(MemberImpl member) {
        System.out.println("onMemberLeft:" + member);
        //todo: in the future we need to remove remote operations.
    }

    @Override
    public void shutdown() {
        logger.finest("Stopping AdvancedOperationService...");
    }

    @Override
    public void start() {
        logger.finest("Starting AdvancedOperationService...");
        this.partitionService = node.getPartitionService();
        this.clusterService = node.getClusterService();
    }

    @Override
    public void notifyBackupCall(long callId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void notifyRemoteCall(final long callId, Object response) {
        //todo: we are removing the operation immediately but in the future you only want
        //todo it e.g. when all backups have returned.
        final Operation op = remoteOperations.remove(callId);

        if (op == null) {
            return;
        }

        if (response instanceof Response) {
            response = ((Response) response).response;
        }

        op.set(response, false);

        final ResponseHandler responseHandler = op.getResponseHandler();
        if (responseHandler != null) {
            responseHandler.sendResponse(response);
        }
    }

    @Override
    public boolean isCallTimedOut(Operation op) {
        return false;
    }

    @Override
    public void runOperation(Operation op) {
        op.setNodeEngine(nodeEngine);
        doRunOperation(op);
    }

    private void doRunOperation(Operation op) {
        //System.out.println(op);
        try {
            op.beforeRun();
            op.run();
            op.afterRun();

            Object response = null;
            if (op.returnsResponse()) {
                response = op.getResponse();

                ResponseHandler responseHandler = op.getResponseHandler();
                if (responseHandler != null) {
                    responseHandler.sendResponse(response);
                }
            }
            op.set(response, false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void executeOperation(final Operation op) {
        final int partitionId = getPartitionIdForExecution(op);
        op.setNodeEngine(nodeEngine);
        if (partitionId == -1) {
            defaultExecutor.execute(new Runnable() {
                public void run() {
                    doRunOperation(op);
                }
            });
        } else {
            schedulers[partitionId].schedule(op);
        }
    }

    @Override
    public InvocationBuilder createInvocationBuilder(String serviceName, Operation op, int partitionId) {
        return new AdvancedInvocationBuilder(nodeEngine, serviceName, op, partitionId);
    }

    @Override
    public InvocationBuilder createInvocationBuilder(String serviceName, Operation op, Address target) {
        return new AdvancedInvocationBuilder(nodeEngine, serviceName, op, target);
    }

    @Override
    public Map<Integer, Object> invokeOnAllPartitions(String serviceName, OperationFactory operationFactory) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Integer, Object> invokeOnPartitions(String serviceName, OperationFactory operationFactory, Collection<Integer> partitions) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Integer, Object> invokeOnTargetPartitions(String serviceName, OperationFactory operationFactory, Address target) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getResponseQueueSize() {
        return 0;
    }

    @Override
    public int getOperationExecutorQueueSize() {
        return 0;
    }

    @Override
    public int getRunningOperationsCount() {
        return 0;
    }

    @Override
    public int getRemoteOperationsCount() {
        return 0;
    }

    @Override
    public int getOperationThreadCount() {
        return 0;
    }

    @Override
    public long getExecutedOperationCount() {
        return 0;
    }

    private class RemoteOperationProcessor implements Runnable {
        final Packet packet;

        public RemoteOperationProcessor(Packet packet) {
            this.packet = packet;
        }

        @Override
        public void run() {
            final Connection conn = packet.getConn();
            try {
                final Address caller = conn.getEndPoint();
                final Data data = packet.getData();
                final Operation op = (Operation) nodeEngine.toObject(data);
                op.setNodeEngine(nodeEngine);
                OperationAccessor.setCallerAddress(op, caller);
                OperationAccessor.setConnection(op, conn);
                if (op instanceof ResponseOperation) {
                    processResponse((ResponseOperation) op);
                } else {
                    ResponseHandlerFactory.setRemoteResponseHandler(nodeEngine, op);
                    if (!OperationAccessor.isJoinOperation(op) && clusterService.getMember(op.getCallerAddress()) == null) {
                        final Exception error = new CallerNotMemberException(op.getCallerAddress(), op.getPartitionId(),
                                op.getClass().getName(), op.getServiceName());
                        handleOperationError(op, error);
                    } else {
                        doRunOperation(op);
                    }
                }
            } catch (Throwable e) {
                logger.severe(e);
            }
        }

        void processResponse(ResponseOperation response) {
            try {
                response.beforeRun();
                response.run();
                response.afterRun();
            } catch (Throwable e) {
                logger.severe("While processing response...", e);
            }
        }
    }

    private class AdvancedInvocationBuilder extends AbstractInvocationBuilder {

        private AdvancedInvocationBuilder(NodeEngineImpl nodeEngine, String serviceName, Operation op, int partitionId) {
            super(nodeEngine, serviceName, op, partitionId);
        }

        private AdvancedInvocationBuilder(NodeEngineImpl nodeEngine, String serviceName, Operation op, Address target) {
            super(nodeEngine, serviceName, op, target);
        }

        @Override
        public InternalCompletableFuture invoke() {
            if (target != null) {
                return invokeOnTarget(serviceName, op, target, replicaIndex, tryCount, tryPauseMillis, callback);
            } else {
                return invokeOnPartition(serviceName, op, partitionId, replicaIndex, replicaIndex, tryCount, tryPauseMillis, callback);
            }
        }
    }

    public class OperationThread extends Thread {

        private final Slot[] ringBuffer;
        private final AtomicLong consumerSeq = new AtomicLong();
        private final AtomicLong producerSeq = new AtomicLong();

        public OperationThread(int capacity) {
            ringBuffer = new Slot[capacity];
            for (int k = 0; k < ringBuffer.length; k++) {
                Slot slot = new Slot();
                ringBuffer[k] = slot;
            }
        }

        public void offer(final Runnable task) {
            if (task == null) {
                throw new IllegalArgumentException("task can't be null");
            }

            final long oldProducerSeq = producerSeq.getAndIncrement();
            final long newProducerSeq = oldProducerSeq + 1;
            final int slotIndex = (int) (oldProducerSeq % ringBuffer.length);
            final Slot slot = ringBuffer[slotIndex];
            slot.runnable = task;
            slot.commit(newProducerSeq);

            //todo: now always an unpark is done, but you only want to do it when
            //the buffer is empty.
            if (consumerSeq.get() == oldProducerSeq) {
                LockSupport.unpark(this);
            }
        }

        public void run() {
            for (; ; ) {
                LockSupport.park();

                long oldConsumerSeq = consumerSeq.get();
                for (; ; ) {
                    final long producerSeq = this.producerSeq.get();
                    if (producerSeq == oldConsumerSeq) {
                        break;
                    }

                    final long newConsumerSeq = oldConsumerSeq + 1;
                    final int slotIndex = (int) (oldConsumerSeq % ringBuffer.length);
                    final Slot slot = ringBuffer[slotIndex];
                    slot.awaitCommitted(newConsumerSeq);
                    final Runnable task = slot.runnable;
                    slot.runnable = null;
                    consumerSeq.set(newConsumerSeq);
                    oldConsumerSeq = newConsumerSeq;

                    doRun(task);
                }
            }
        }

        private void doRun(Runnable task) {
            try {
                task.run();
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        //todo: padding needed to prevent false sharing.
        private class Slot {
            private volatile long seq = 0;
            private Runnable runnable;


            public void commit(final long seq) {
                this.seq = seq;
            }

            public void awaitCommitted(final long consumerSequence) {
                for (; ; ) {
                    if (seq >= consumerSequence) {
                        return;
                    }
                }
            }
        }
    }

    /**
     * A Scheduler responsible for scheduling operations for a specific partitions.
     * The PartitionOperationScheduler will guarantee that at any given moment, at
     * most 1 thread will be active in that partition.
     * <p/>
     * todo:
     * - improved thread assignment
     * - add batching for the consumer
     * - add system messages. System messages can be stored on a different regular queue (e.g. concurrentlinkedqueue)
     * <p/>
     * bad things:
     * - contention on the producersequenceref with concurrent producers
     * - when a consumer is finished, it needs to unset the scheduled bit on the producersequence,
     * this will cause contention of the producers with the consumers. This is actually also
     * the case with actors in akka. The nice thing however is that contention between producer
     * and condumer will not happen when there is a lot of work being processed since the scheduler
     * needs to remain 'scheduled'.
     * <p/>
     * workstealing: when a partitionthread is finished with running a partitionoperationscheduler,
     * instead of waiting for more work, it could try to 'steal' another partitionoperationscheduler
     * that has pending work.
     */
    public class PartitionOperationScheduler implements Runnable {
        private final int partitionId;

        private final Slot[] ringbuffer;

        private final AtomicLong producerSeq = new AtomicLong(0);

        //we only have a single consumer
        private final AtomicLong consumerSeq = new AtomicLong(0);

        private final ConcurrentLinkedQueue priorityQueue = new ConcurrentLinkedQueue();

        public PartitionOperationScheduler(final int partitionId, int ringBufferSize) {
            this.partitionId = partitionId;
            this.ringbuffer = new Slot[ringBufferSize];

            for (int k = 0; k < ringbuffer.length; k++) {
                ringbuffer[k] = new Slot();
            }
        }

        public int toIndex(long sequence) {
            if (sequence % 2 == 1) {
                sequence--;
            }

            //todo: can be done more efficient by not using mod but using bitshift
            return (int) ((sequence / 2) % ringbuffer.length);
        }

        private int size(long producerSeq, long consumerSeq) {
            if (producerSeq % 2 == 1) {
                producerSeq--;
            }

            if (producerSeq == consumerSeq) {
                return 0;
            }

            return (int) ((producerSeq - consumerSeq) / 2);
        }

        public void schedule(Packet packet) {
            assert packet != null;
            try {
                long oldProducerSeq = producerSeq.get();

                //this flag indicates if we need to schedule, or if scheduling already is taken care of.
                boolean schedule;
                long newProducerSeq;
                for (; ; ) {
                    newProducerSeq = oldProducerSeq + 2;
                    if (oldProducerSeq % 2 == 0) {
                        //if the scheduled flag is not set, we are going to be responsible for scheduling.
                        schedule = true;
                        newProducerSeq++;
                    } else {
                        //apparently this scheduler already is scheduled, so we don't need to schedule it.
                        schedule = false;
                    }

                    if (producerSeq.compareAndSet(oldProducerSeq, newProducerSeq)) {
                        break;
                    }

                    //we did not manage to claim the slot and potentially set the scheduled but, so we need to try again.
                    oldProducerSeq = producerSeq.get();
                }

                //we claimed a slot.
                int slotIndex = toIndex(newProducerSeq);
                Slot slot = ringbuffer[slotIndex];
                slot.task = packet;
                slot.commit(newProducerSeq);

                if (schedule) {
                    executeOnOperationThread(this);
                }
            } catch (RuntimeException e) {
                e.printStackTrace();
                throw e;
            }
        }

        public void schedule(Operation op) {
            assert op != null;

            try {
                long oldProducerSeq = producerSeq.get();
                long consumerSeq = this.consumerSeq.get();

                if (localCallOptimizationEnabled && oldProducerSeq == consumerSeq) {
                    //there currently is no pending work and the scheduler is not scheduled, so we can try to do a local runs optimization

                    long newProduceSequence = oldProducerSeq + 1;

                    //if we can set the 'uneven' flag, it means that scheduler is not yet running
                    if (producerSeq.compareAndSet(oldProducerSeq, newProduceSequence)) {
                        //we managed to signal other consumers that scheduling should not be done, because we do a local runs optimization

                        runOperation(op, true);

                        if (producerSeq.get() > newProduceSequence) {
                            //work has been produced by another producer, and since we still own the scheduled bit, we can safely
                            //schedule this
                            executeOnOperationThread(this);
                        } else {
                            //work has not yet been produced, so we are going to unset the scheduled bit.
                            if (producerSeq.compareAndSet(newProduceSequence, oldProducerSeq)) {
                                //we successfully managed to set the scheduled bit to false and no new work has been
                                //scheduled by other producers, so we are done.
                                return;
                            }

                            //new work has been scheduled by other producers, but since we still own the scheduled bit,
                            //we can schedule the work.
                            //work has been produced, so we need to offload it.
                            executeOnOperationThread(this);
                        }

                        return;
                    }

                    oldProducerSeq = producerSeq.get();
                } else if (size(oldProducerSeq, consumerSeq) == ringbuffer.length) {
                    //todo: overload
                    System.out.println("Overload");
                    throw new RuntimeException();
                }

                //this flag indicates if we need to schedule, or if scheduling already is taken care of.
                boolean schedule;
                long newProducerSeq;
                for (; ; ) {
                    newProducerSeq = oldProducerSeq + 2;
                    if (oldProducerSeq % 2 == 0) {
                        //if the scheduled flag is not set, we are going to be responsible for scheduling.
                        schedule = true;
                        newProducerSeq++;
                    } else {
                        //apparently this scheduler already is scheduled, so we don't need to schedule it.
                        schedule = false;
                    }

                    if (producerSeq.compareAndSet(oldProducerSeq, newProducerSeq)) {
                        break;
                    }

                    //we did not manage to claim the slot and potentially set the scheduled but, so we need to try again.
                    oldProducerSeq = producerSeq.get();
                }

                //we claimed a slot.
                int slotIndex = toIndex(newProducerSeq);
                Slot slot = ringbuffer[slotIndex];
                slot.task = op;
                slot.commit(newProducerSeq);

                if (schedule) {
                    executeOnOperationThread(this);
                }
            } catch (RuntimeException e) {
                e.printStackTrace();
                throw e;
            }
        }

        @Override
        public void run() {
            try {
                long oldConsumerSeq = consumerSeq.get();
                for (; ; ) {
                    final long oldProducerSeq = producerSeq.get();

                    priorityQueue.poll();

                    if (oldConsumerSeq == oldProducerSeq - 1) {
                        //there is no more work, so we are going to try to unschedule

                        //we unset the scheduled flag by subtracting one from the producerSeq
                        final long newProducerSeq = oldProducerSeq - 1;

                        if (producerSeq.compareAndSet(oldProducerSeq, newProducerSeq)) {
                            return;
                        }
                        //we did not manage to unset the schedule flag because work has been offered.
                        //so lets continue running.
                    } else {
                        final long newConsumerSeq = oldConsumerSeq + 2;
                        final int slotIndex = toIndex(newConsumerSeq);
                        final Slot slot = ringbuffer[slotIndex];
                        slot.awaitCommitted(newConsumerSeq);
                        final Object task = slot.task;

                        consumerSeq.set(newConsumerSeq);
                        if (task instanceof Operation) {
                            runOperation((Operation)task, false);
                        } else {
                            runPacket((Packet)task);
                        }
                        oldConsumerSeq = newConsumerSeq;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void runPacket(Packet packet) {
            final Connection conn = packet.getConn();
            try {
                final Address caller = conn.getEndPoint();
                final Data data = packet.getData();
                final Operation op = (Operation) nodeEngine.toObject(data);
                op.setNodeEngine(nodeEngine);
                OperationAccessor.setCallerAddress(op, caller);
                OperationAccessor.setConnection(op, conn);
                ResponseHandlerFactory.setRemoteResponseHandler(nodeEngine, op);

                if (!OperationAccessor.isJoinOperation(op) && clusterService.getMember(op.getCallerAddress()) == null) {
                    final Exception error = new CallerNotMemberException(op.getCallerAddress(), op.getPartitionId(),
                            op.getClass().getName(), op.getServiceName());
                    handleOperationError(op, error);
                } else {
                    runOperation(op, false);
                }
            } catch (Throwable e) {
                logger.severe(e);
            }
        }

        private void runOperation(final Operation op, final boolean callerRuns) {
            try {
                op.setNodeEngine(nodeEngine);
                op.setPartitionId(partitionId);
                op.beforeRun();
                op.run();
                op.afterRun();

                Object response = null;
                if (op.returnsResponse()) {
                    response = op.getResponse();

                    ResponseHandler responseHandler = op.getResponseHandler();
                    if (responseHandler != null) {
                        responseHandler.sendResponse(response);
                    }
                }
                op.set(response, callerRuns);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        //todo: padding needed to prevent false sharing.
        private class Slot {
            private volatile long sequence = 0;
            private Object task;

            public void commit(final long sequence) {
                this.sequence = sequence;
            }

            public void awaitCommitted(final long consumerSequence) {
                for (; ; ) {
                    if (sequence >= consumerSequence) {
                        return;
                    }
                }
            }
        }
    }
}


