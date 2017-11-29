/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi;

import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

/**
 * A {@link CallStatus} designed when an operation is offloading the processing of the operation to a different system, e.g
 * a different thread.
 *
 * Signals that the Operation has been offloaded e.g. an EntryProcessor. And therefor no response is available when the
 * operation is executed. Only at a later time a response is ready and it is up to the offload functionality to determine
 * how to deal with that. It could be that a response is send using the original operation handler, but it could also
 * be that the operation will be rescheduled on an operation thread (a real continuation).
 */
public abstract class Offloaded extends CallStatus {

    protected OperationServiceImpl operationService;
    private final Operation source;

    public Offloaded(Operation source) {
        super(CallStatus.ENUM_OFFLOADED);
        this.source = source;
    }

    public Operation source() {
        return source;
    }

    public void setOperationService(OperationServiceImpl operationService) {
        this.operationService = operationService;
    }

    public abstract void start() throws Exception;

    public void sendResponse(Object response) {
        operationService.onCompletionAsyncOperation(source);
        source.sendResponse(response);
    }
}
