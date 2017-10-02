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

package com.hazelcast.concurrent.lock.operations;

import com.hazelcast.concurrent.lock.LockDataSerializerHook;
import com.hazelcast.concurrent.lock.LockStoreImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.impl.MutatingOperation;

import java.io.IOException;

public class BeforeAwaitBackupOperation extends AbstractLockOperation implements BackupOperation, MutatingOperation {

    private String conditionId;
    private String originalCaller;

    public BeforeAwaitBackupOperation() {
    }

    public BeforeAwaitBackupOperation(ObjectNamespace namespace, Data key, long threadId,
                                      String conditionId, String originalCaller) {
        super(namespace, key, threadId);
        this.conditionId = conditionId;
        this.originalCaller = originalCaller;
    }

    @Override
    public Object call() throws Exception {
        LockStoreImpl lockStore = getLockStore();
        lockStore.addAwait(key, conditionId, originalCaller, threadId);
        lockStore.unlock(key, originalCaller, threadId, getReferenceCallId());
        return true;
    }

    @Override
    public int getId() {
        return LockDataSerializerHook.BEFORE_AWAIT_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(originalCaller);
        out.writeUTF(conditionId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        originalCaller = in.readUTF();
        conditionId = in.readUTF();
    }
}
