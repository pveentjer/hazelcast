/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.spi.impl;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.partition.PartitionView;
import com.hazelcast.spi.*;
import com.hazelcast.spi.exception.*;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.executor.ScheduledTaskRunner;

import java.io.IOException;
import java.util.concurrent.*;

abstract class InvocationImpl implements Invocation, Callback<Object> {

    private static final Object NULL_RESPONSE = new Object() {
        public String toString() {
            return "Invocation::NULL_RESPONSE";
        }
    };

    private static final Object RETRY_RESPONSE = new Object() {
        public String toString() {
            return "Invocation::RETRY_RESPONSE";
        }
    };

    private static final Object WAIT_RESPONSE = new Object() {
        public String toString() {
            return "Invocation::WAIT_RESPONSE";
        }
    };

    private static final Object TIMEOUT_RESPONSE = new Object() {
        public String toString() {
            return "Invocation::TIMEOUT_RESPONSE";
        }
    };

    private static long decrementTimeout(long timeout, long diff) {
        if (timeout != Long.MAX_VALUE) {
            timeout -= diff;
        }
        return timeout;
    }

    protected final long callTimeout;
    protected final NodeEngineImpl nodeEngine;
    protected final String serviceName;
    protected final Operation op;
    protected final int partitionId;
    protected final int replicaIndex;
    protected final int tryCount;
    protected final long tryPauseMillis;
    protected final Callback<Object> callback;
    protected final ILogger logger;

    private volatile int invokeCount = 0;
    private volatile Address target;
    private boolean remote = false;
    private final InvocationFuture invocationFuture = new InvocationFuture();

    InvocationImpl(NodeEngineImpl nodeEngine, String serviceName, Operation op, int partitionId,
                   int replicaIndex, int tryCount, long tryPauseMillis, long callTimeout, Callback<Object> callback) {
        this.nodeEngine = nodeEngine;
        this.serviceName = serviceName;
        this.op = op;
        this.partitionId = partitionId;
        this.replicaIndex = replicaIndex;
        this.tryCount = tryCount;
        this.tryPauseMillis = tryPauseMillis;
        this.callTimeout = getCallTimeout(callTimeout);
        this.callback = callback;
        this.logger = nodeEngine.getLogger(Invocation.class.getName());
    }

    abstract ExceptionAction onException(Throwable t);

    public String getServiceName() {
        return serviceName;
    }

    PartitionView getPartition() {
        return nodeEngine.getPartitionService().getPartition(partitionId);
    }

    public int getReplicaIndex() {
        return replicaIndex;
    }

    public int getPartitionId() {
        return partitionId;
    }

    private Future resetAndReInvoke() {
        //responseQ.clear();
        invokeCount = 0;
        doInvoke();
        return new InvocationFuture();
    }

    private long getCallTimeout(long callTimeout) {
        if (callTimeout > 0) {
            return callTimeout;
        }
        final long defaultCallTimeout = nodeEngine.operationService.getDefaultCallTimeout();
        if (op instanceof WaitSupport) {
            final long waitTimeoutMillis = ((WaitSupport) op).getWaitTimeoutMillis();
            if (waitTimeoutMillis > 0 && waitTimeoutMillis < Long.MAX_VALUE && defaultCallTimeout > 10000) {
                return waitTimeoutMillis + 10000;
            }
        }
        return defaultCallTimeout;
    }

    public final Future invoke() {
        if (invokeCount > 0) {   // no need to be pessimistic.
            throw new IllegalStateException("An invocation can not be invoked more than once!");
        }
        if (op.getCallId() != 0) {
            throw new IllegalStateException("An operation[" + op + "] can not be used for multiple invocations!");
        }
        try {
            OperationAccessor.setCallTimeout(op, callTimeout);
            OperationAccessor.setCallerAddress(op, nodeEngine.getThisAddress());
            op.setNodeEngine(nodeEngine).setServiceName(serviceName)
                    .setPartitionId(partitionId).setReplicaIndex(replicaIndex);
            if (op.getCallerUuid() == null) {
                op.setCallerUuid(nodeEngine.getLocalMember().getUuid());
            }
            OperationAccessor.setAsync(op, callback != null);
            if (!nodeEngine.operationService.isInvocationAllowedFromCurrentThread(op) && !OperationAccessor.isMigrationOperation(op)) {
                throw new IllegalThreadStateException(Thread.currentThread() + " cannot make remote call: " + op);
            }
            doInvoke();
        } catch (Exception e) {
            if (e instanceof RetryableException) {
                notify(e);
            } else {
                throw ExceptionUtil.rethrow(e);
            }
        }
        return invocationFuture;
    }

    private void doInvoke() {
        if (!nodeEngine.isActive()) {
            remote = false;
            if (callback == null) {
                throw new HazelcastInstanceNotActiveException();
            } else {
                notify(new HazelcastInstanceNotActiveException());
                return;
            }
        }

        final Address invTarget = getTarget();
        target = invTarget;
        invokeCount++;
        final Address thisAddress = nodeEngine.getThisAddress();
        if (invTarget == null) {
            remote = false;
            if (nodeEngine.isActive()) {
                notify(new WrongTargetException(thisAddress, invTarget, partitionId, replicaIndex, op.getClass().getName(), serviceName));
            } else {
                notify(new HazelcastInstanceNotActiveException());
            }
            return;
        }

        final MemberImpl member = nodeEngine.getClusterService().getMember(invTarget);
        if (!OperationAccessor.isJoinOperation(op) && member == null) {
            notify(new TargetNotMemberException(invTarget, partitionId, op.getClass().getName(), serviceName));
            return;
        }
        if (op.getPartitionId() != partitionId) {
            notify(new IllegalStateException("Partition id of operation: " + op.getPartitionId() +
                    " is not equal to the partition id of invocation: " + partitionId));
            return;
        }
        if (op.getReplicaIndex() != replicaIndex) {
            notify(new IllegalStateException("Replica index of operation: " + op.getReplicaIndex() +
                    " is not equal to the replica index of invocation: " + replicaIndex));
            return;
        }
        final OperationServiceImpl operationService = nodeEngine.operationService;
        OperationAccessor.setInvocationTime(op, nodeEngine.getClusterTime());

        remote = !thisAddress.equals(invTarget);
        if (remote) {
            final RemoteCall call = member != null ? new RemoteCall(member, this) : new RemoteCall(invTarget, this);
            final long callId = operationService.registerRemoteCall(call);
            if (op instanceof BackupAwareOperation) {
                registerBackups((BackupAwareOperation) op, callId);
            }
            OperationAccessor.setCallId(op, callId);
            boolean sent = operationService.send(op, invTarget);
            if (!sent) {
                operationService.deregisterRemoteCall(callId);
                operationService.deregisterBackupCall(callId);
                notify(new RetryableIOException("Packet not sent to -> " + invTarget));
            }
        } else {
            // OperationService.onMemberLeft handles removing call
//                    final long prevCallId = op.getCallId();
//                    if (prevCallId != 0) {
//                        operationService.deregisterRemoteCall(prevCallId);
//                    }
            if (op instanceof BackupAwareOperation) {
                final long callId = operationService.newCallId();
                registerBackups((BackupAwareOperation) op, callId);
                OperationAccessor.setCallId(op, callId);
            }
            ResponseHandlerFactory.setLocalResponseHandler(op, this);
            if (!nodeEngine.operationService.isAllowedToRunInCurrentThread(op)) {
                operationService.executeOperation(op);
            } else {
                operationService.runOperation(op);
            }
        }
    }

    private void registerBackups(BackupAwareOperation op, long callId) {
        final long oldCallId = ((Operation) op).getCallId();
        final OperationServiceImpl operationService = nodeEngine.operationService;
        if (oldCallId != 0) {
            operationService.deregisterBackupCall(oldCallId);
        }
        operationService.registerBackupCall(callId);
    }

    @Override
    public void notify(Object obj) {
         Object response;
        if (obj == null) {
            response = NULL_RESPONSE;
        } else if (obj instanceof CallTimeoutException) {
            response = RETRY_RESPONSE;
            if (logger.isFinestEnabled()) {
                logger.finest("Call timed-out during wait-notify phase, retrying call: " + toString());
            }
            invokeCount--;
        } else if (obj instanceof Throwable) {
            final Throwable error = (Throwable) obj;
            final ExceptionAction action = onException(error);
            final int localInvokeCount = invokeCount;
            if (action == ExceptionAction.RETRY_INVOCATION && localInvokeCount < tryCount) {
                response = RETRY_RESPONSE;
                if (localInvokeCount > 99 && localInvokeCount % 10 == 0) {
                    logger.warning("Retrying invocation: " + toString() + ", Reason: " + error);
                }
            } else if (action == ExceptionAction.CONTINUE_WAIT) {
                response = WAIT_RESPONSE;
            } else {
                response = obj;
            }
        } else {
            response = obj;
        }

        if (response == RETRY_RESPONSE) {
            final ExecutionService ex = nodeEngine.getExecutionService();
            ex.schedule(new ScheduledTaskRunner(ex.getExecutor(ExecutionService.ASYNC_EXECUTOR),
                    new ScheduledInv()), tryPauseMillis, TimeUnit.MILLISECONDS);
        } else if (response == WAIT_RESPONSE) {
            //no-op for the time being.
        } else {
            if (response instanceof Response) {
                if (reinvokeNeeded((Response) response)) {
                    resetAndReInvoke();
                    return;
                }
            }

            invocationFuture.set(response);
        }
    }

    private boolean reinvokeNeeded(Response response) {
        if (op instanceof BackupAwareOperation) {
            try {
                final boolean ok = nodeEngine.operationService.waitForBackups(response.callId, response.backupCount, 5, TimeUnit.SECONDS);
                if (!ok) {
                    if (logger.isFinestEnabled()) {
                        logger.finest( "Backup response cannot be received -> " + InvocationImpl.this.toString());
                    }
                    if (nodeEngine.getClusterService().getMember(target) == null) {
                        return true;
                    }
                }
            } catch (InterruptedException ignored) {
            }
        }
        return false;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("InvocationImpl");
        sb.append("{ serviceName='").append(serviceName).append('\'');
        sb.append(", op=").append(op);
        sb.append(", partitionId=").append(partitionId);
        sb.append(", replicaIndex=").append(replicaIndex);
        sb.append(", tryCount=").append(tryCount);
        sb.append(", tryPauseMillis=").append(tryPauseMillis);
        sb.append(", invokeCount=").append(invokeCount);
        sb.append(", callTimeout=").append(callTimeout);
        sb.append(", target=").append(target);
        sb.append('}');
        return sb.toString();
    }

    private class ScheduledInv implements Runnable {
        public void run() {
            doInvoke();
        }
    }

    private class InvocationFuture implements Future {

        volatile boolean done = false;
        volatile Object response;

        //todo: instead of creating an additional object, we could also use 'this' as monitor.
        final Object monitor = new Object();

        public void set(Object response){
            if(response == null){
                throw new IllegalArgumentException("response can't be null");
            }

            synchronized (monitor) {
                //todo: check if response already has been set.
                this.response = response;
                monitor.notifyAll();
            }

            try {
                final Object realResponse;
                if (response instanceof Response) {
                    final Response responseObj = (Response) response;
                    // no need to deregister backup call, since backups are not registered for async invocations.
                    realResponse = responseObj.response;
                } else if (response == NULL_RESPONSE) {
                    realResponse = null;
                } else {
                    realResponse = response;
                }
                if (callback != null)
                    callback.notify(realResponse);
            } catch (Throwable e) {
                logger.severe(e);
            }
        }

        @Override
        public Object get() throws InterruptedException, ExecutionException {
            try {
                return get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                logger.finest(e);
                return null;
            }
        }

        @Override
        public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            final Object response = resolveResponse(waitForResponse(timeout, unit));
            done = true;
            return response;
        }

        private Object waitForResponse(long time, TimeUnit unit) {
            //if(response!=null){
            //    return response;
            //}

            long timeoutMs = unit.toMillis(time);
            if (timeoutMs < 0) timeoutMs = 0;

            final long maxCallTimeout = callTimeout * 2 > 0 ? callTimeout * 2 : Long.MAX_VALUE;
            final boolean longPolling = timeoutMs > maxCallTimeout;
            int pollCount = 0;
            InterruptedException interrupted = null;

            while (timeoutMs >= 0) {
                final long pollTimeoutMs = Math.min(maxCallTimeout, timeoutMs);
                final long startMs = Clock.currentTimeMillis();
                final long lastPollTime;
                try {
                    synchronized (monitor) {
                        if (response == null) {
                            monitor.wait(pollTimeoutMs);
                        }
                    }

                    if (response != null) {
                        if (interrupted != null) {
                            Thread.currentThread().interrupt();
                        }
                        return response;
                    }

                    lastPollTime = Clock.currentTimeMillis() - startMs;
                    timeoutMs = decrementTimeout(timeoutMs, lastPollTime);
                } catch (InterruptedException e) {
                    // do not allow interruption while waiting for a response!
                    logger.finest(Thread.currentThread().getName() + " is interrupted while waiting " +
                            "response for operation " + op);
                    interrupted = e;
                    if (!nodeEngine.isActive()) {
                        return e;
                    }
                    continue;
                }
                pollCount++;

                if (/* response == null && */ longPolling) {
                    // no response!
                    final Address target = getTarget();
                    if (nodeEngine.getThisAddress().equals(target)) {
                        // target may change during invocation because of migration!
                        continue;
                    }
                    // TODO: @mm - improve logging (see SystemLogService)
                    logger.warning("No response for " + lastPollTime + " ms. " + toString());

                    boolean executing = isOperationExecuting(target);
                    if (!executing) {
                       if (response != null) {
                            continue;
                        }
                        return new OperationTimeoutException("No response for " + (pollTimeoutMs * pollCount)
                                + " ms. Aborting invocation! " + toString());
                    }
                }
            }
            return TIMEOUT_RESPONSE;
        }

        private Object resolveResponse(Object response) throws ExecutionException, InterruptedException, TimeoutException {
            if (response instanceof Throwable) {
                if (remote) {
                    ExceptionUtil.fixRemoteStackTrace((Throwable) response, Thread.currentThread().getStackTrace());
                }
                // To obey Future contract, we should wrap unchecked exceptions with ExecutionExceptions.
                if (response instanceof ExecutionException) {
                    throw (ExecutionException) response;
                }
                if (response instanceof TimeoutException) {
                    throw (TimeoutException) response;
                }
                if (response instanceof Error) {
                    throw (Error) response;
                }
                if (response instanceof InterruptedException) {
                    throw (InterruptedException) response;
                }
                throw new ExecutionException((Throwable) response);
            }
            if (response instanceof Response) {
                return ((Response) response).response;
            }
            if (response == NULL_RESPONSE) {
                return null;
            }
            if (response == TIMEOUT_RESPONSE) {
                throw new TimeoutException();
            }
            return response;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            done = true;
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return done;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("InvocationFuture{");
            sb.append("invocation=").append(InvocationImpl.this.toString());
            sb.append(", done=").append(done);
            sb.append('}');
            return sb.toString();
        }

        private boolean isOperationExecuting(Address target) {
            // ask if op is still being executed?
            Boolean executing = Boolean.FALSE;
            try {
                final Invocation inv = new TargetInvocationImpl(nodeEngine, serviceName,
                        new IsStillExecuting(op.getCallId()), target, 0, 0, 5000, null);
                Future f = inv.invoke();
                // TODO: @mm - improve logging (see SystemLogService)
                logger.warning("Asking if operation execution has been started: " + toString());
                executing = (Boolean) nodeEngine.toObject(f.get(5000, TimeUnit.MILLISECONDS));
            } catch (Exception e) {
                logger.warning("While asking 'is-executing': " + toString(), e);
            }
            // TODO: @mm - improve logging (see SystemLogService)
            logger.warning("'is-executing': " + executing + " -> " + toString());
            return executing;
        }
    }

    public static class IsStillExecuting extends AbstractOperation {

        private long operationCallId;

        IsStillExecuting() {
        }

        private IsStillExecuting(long operationCallId) {
            this.operationCallId = operationCallId;
        }

        public void run() throws Exception {
            NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
            OperationServiceImpl operationService = nodeEngine.operationService;
            boolean executing = operationService.isOperationExecuting(getCallerAddress(), getCallerUuid(), operationCallId);
            getResponseHandler().sendResponse(executing);
        }

        @Override
        public boolean returnsResponse() {
            return false;
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            operationCallId = in.readLong();
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeLong(operationCallId);
        }
    }
}
