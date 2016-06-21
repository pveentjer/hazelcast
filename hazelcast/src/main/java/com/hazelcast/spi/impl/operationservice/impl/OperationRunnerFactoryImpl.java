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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.spi.impl.operationexecutor.OperationRunnerFactory;

class OperationRunnerFactoryImpl implements OperationRunnerFactory {
    private final ILogger logger;
    private final OperationServiceImpl operationService;

    OperationRunnerFactoryImpl(OperationServiceImpl operationService) {
        this.operationService = operationService;
        this.logger = operationService.node.getLogger(OperationRunnerImpl.class);
    }

    @Override
    public OperationRunner createAdHocRunner() {
        return new OperationRunnerImpl(
                OperationRunnerImpl.AD_HOC_PARTITION_ID,
                logger,
                operationService.outboundResponseHandler,
                operationService.operationBackupHandler,
                operationService.node,
                operationService.completedOperationsCount,
                operationService.serializationService);
    }

    @Override
    public OperationRunner createPartitionRunner(int partitionId) {
        return new OperationRunnerImpl(
                partitionId,
                logger,
                operationService.outboundResponseHandler,
                operationService.operationBackupHandler,
                operationService.node,
                operationService.completedOperationsCount,
                operationService.serializationService);
    }

    @Override
    public OperationRunner createGenericRunner() {
        return new OperationRunnerImpl(
                Operation.GENERIC_PARTITION_ID,
                logger,
                operationService.outboundResponseHandler,
                operationService.operationBackupHandler,
                operationService.node,
                operationService.completedOperationsCount,
                operationService.serializationService);
    }
}
