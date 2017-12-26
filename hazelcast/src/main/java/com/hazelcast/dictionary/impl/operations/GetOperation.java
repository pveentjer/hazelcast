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

import static com.hazelcast.dictionary.impl.DictionaryDataSerializerHook.GET_OPERATION;
import static com.hazelcast.spi.CallStatus.DONE_RESPONSE;

public class GetOperation extends DictionaryOperation {

    private Data keyData;
    private transient Object response;

    public GetOperation() {
    }

    public GetOperation(String name, Data keyData) {
        super(name);
        this.keyData = keyData;
    }

    @Override
    public CallStatus call() throws Exception {
        int partitionHash = keyData.getPartitionHash();
        Segment segment = partition.segment(partitionHash);
        response = segment.get(keyData, partitionHash);
        return DONE_RESPONSE;
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public int getId() {
        return GET_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(keyData);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        keyData = in.readData();
    }
}
