/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationexecutor.impl;

import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.inspectOutputMemoryError;

/**
 * An {@link OperationThread} for non partition specific operations.
 */
public final class GenericOperationThread extends OperationThread {

    final OperationRunner operationRunner;

    public GenericOperationThread(String name, int threadId, OperationQueue queue, ILogger logger,
                                  HazelcastThreadGroup threadGroup, NodeExtension nodeExtension,
                                  OperationRunner operationRunner, boolean priority) {
        super(name, threadId, queue, logger, threadGroup, nodeExtension, priority);
        this.operationRunner = operationRunner;
    }

    @Override
    protected void process(Object task) {
        try {
            if (task.getClass() == Packet.class) {
                Packet packet = (Packet) task;
                operationRunner.run(packet);
                completedPacketCount.inc();
            } else if (task instanceof Operation) {
                Operation operation = (Operation) task;
                operationRunner.run(operation);
                completedOperationCount.inc();
            } else if (task instanceof PartitionSpecificRunnable) {
                PartitionSpecificRunnable partitionSpecificRunnable = (PartitionSpecificRunnable) task;
                operationRunner.run(partitionSpecificRunnable);
                completedPartitionSpecificRunnableCount.inc();
            } else if (task instanceof Runnable) {
                Runnable runnable = (Runnable) task;
                runnable.run();
                completedRunnableCount.inc();
            } else {
                throw new IllegalStateException("Unhandled task type for task:" + task);
            }

            completedTotalCount.inc();
        } catch (Throwable t) {
            errorCount.inc();
            inspectOutputMemoryError(t);
            logger.severe("Failed to process packet: " + task + " on " + getName(), t);
        }
    }
}
