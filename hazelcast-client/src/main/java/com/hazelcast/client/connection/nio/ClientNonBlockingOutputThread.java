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

package com.hazelcast.client.connection.nio;

import com.hazelcast.logging.ILogger;
import com.hazelcast.internal.connection.tcp.nonblocking.NonBlockingIOThread;
import com.hazelcast.internal.connection.tcp.nonblocking.NonBlockingIOThreadOutOfMemoryHandler;
import com.hazelcast.internal.connection.tcp.nonblocking.SelectionHandler;

import java.nio.channels.SelectionKey;

/**
 * ClientNonBlockingOutputThread facilitates non-blocking writing for Hazelcast Clients.
 */
public final class ClientNonBlockingOutputThread extends NonBlockingIOThread {

    public ClientNonBlockingOutputThread(ThreadGroup threadGroup, String threadName, ILogger logger,
                                         NonBlockingIOThreadOutOfMemoryHandler oomeHandler) {
        super(threadGroup, threadName, logger, oomeHandler);
    }

    @Override
    protected void handleSelectionKey(SelectionKey sk) {
        if (sk.isValid() && sk.isWritable()) {
            sk.interestOps(sk.interestOps() & ~SelectionKey.OP_WRITE);
            SelectionHandler handler = (SelectionHandler) sk.attachment();
            try {
                handler.handle();
            } catch (Throwable t) {
                handler.onFailure(t);
            }
        }
    }
}

