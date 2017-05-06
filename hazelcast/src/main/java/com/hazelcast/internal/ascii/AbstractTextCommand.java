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

package com.hazelcast.internal.ascii;

import com.hazelcast.nio.ascii.TextCommandDecoder;
import com.hazelcast.nio.ascii.TextCommandEncoder;

public abstract class AbstractTextCommand implements TextCommand {
    protected final TextCommandConstants.TextCommandType type;
    private TextCommandDecoder readHandler;
    private TextCommandEncoder writeHandler;
    private long requestId = -1;

    protected AbstractTextCommand(TextCommandConstants.TextCommandType type) {
        this.type = type;
    }

    @Override
    public TextCommandConstants.TextCommandType getType() {
        return type;
    }

    @Override
    public TextCommandDecoder getReadHandler() {
        return readHandler;
    }

    @Override
    public TextCommandEncoder getWriteHandler() {
        return writeHandler;
    }

    @Override
    public long getRequestId() {
        return requestId;
    }

    @Override
    public void init(TextCommandDecoder textReadHandler, long requestId) {
        this.readHandler = textReadHandler;
        this.requestId = requestId;
        this.writeHandler = textReadHandler.getTextWriteHandler();
    }

    @Override
    public boolean isUrgent() {
        return false;
    }

    @Override
    public boolean shouldReply() {
        return true;
    }

    @Override
    public String toString() {
        return "AbstractTextCommand[" + type + "]{"
                + "requestId="
                + requestId
                + '}';
    }
}
