/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.dictionary.impl.operations;

import com.hazelcast.dictionary.impl.Segment;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.CallStatus;

import java.io.IOException;

import static com.hazelcast.dictionary.impl.DictionaryDataSerializerHook.PUT_OPERATION;
import static com.hazelcast.spi.CallStatus.DONE_RESPONSE;

public class PutOperation extends DictionaryOperation {

    private boolean overwrite;
    private Data keyData;
    private Data valueData;
    private transient boolean response;

    public PutOperation() {
    }

    public PutOperation(String name, boolean overwrite, Data keyData, Data valueData) {
        super(name);
        this.overwrite = overwrite;
        this.keyData = keyData;
        this.valueData = valueData;
    }

    @Override
    public CallStatus call() throws Exception {
        int partitionHash = keyData.getPartitionHash();
        Segment segment = partition.segment(partitionHash);
        response = segment.put(keyData, partitionHash, valueData, overwrite);
        return DONE_RESPONSE;
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public int getId() {
        return PUT_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(overwrite);
        out.writeData(keyData);
        out.writeData(valueData);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        overwrite = in.readBoolean();
        keyData = in.readData();
        valueData = in.readData();
    }
}
