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

package com.hazelcast.internal.networking;

import com.hazelcast.internal.util.counters.SwCounter;

import java.io.Closeable;

/**
 * The ChannelReader is responsible for reading data from the socket, on behalf of a connection, into a
 * {@link java.nio.ByteBuffer}. Once the data is read into the ByteBuffer, this ByteBuffer is passed to the {@link ChannelInboundHandler}
 * that takes care of the actual processing of the incoming data.
 *
 * Each {@link SocketConnection} has its own {@link ChannelReader} instance.
 *
 * There are many different flavors of ChannelReader:
 * <ol>
 * <li>reader for member to member communication</li>
 * <li>reader for (old and new) client to member communication</li>
 * <li>reader for encrypted member to member communication</li>
 * <li>reader for REST/Memcached</li>
 * </ol>
 *
 * A ChannelReader is tightly coupled to the threading model; so a ChannelReader instance is created using
 * {@link IOThreadingModel#newChannelReader(SocketConnection)}.
 *
 * Before Hazelcast 3.6 the name of this interface was ChannelInboundHandler.
 *
 * @see ChannelInboundHandler
 * @see ChannelWriter
 * @see IOThreadingModel
 */
//todo: rename this class to ChannelInboundHandler
public interface ChannelReader extends Closeable {

    /**
     * Returns the last {@link com.hazelcast.util.Clock#currentTimeMillis()} a read of the socket was done.
     *
     * @return the last time a read from the socket was done.
     */
    long lastReadTimeMillis();

    /**
     * Gets the SwCounter that counts the number of normal packets that have been read.
     *
     * todo: these should probably not be exposed in api
     *
     * @return the normal frame counter.
     */
    SwCounter getNormalFramesReadCounter();

    /**
     * Gets the SwCounter that counts the number of priority packets that have been read.
     *
     * todo: these should probably not be exposed in api
     *
     * @return the priority frame counter.
     */
    SwCounter getPriorityFramesReadCounter();

    /**
     * Initializes this ChannelReader.
     *
     * This method is called from an arbitrary thread and is only called once.
     */
    void init();
}
