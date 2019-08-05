/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.unsafe.atomicreference.operations;

import com.hazelcast.cp.internal.datastructures.unsafe.atomicreference.AtomicReferenceDataSerializerHook;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;

public class GetOperation extends AbstractAtomicReferenceOperation implements ReadonlyOperation {

    private Data returnValue;

    public GetOperation() {
    }

    public GetOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        returnValue = getReferenceContainer().get();
    }

    @Override
    public Object getResponse() {
        return returnValue;
    }

    @Override
    public int getClassId() {
        return AtomicReferenceDataSerializerHook.GET;
    }
}
