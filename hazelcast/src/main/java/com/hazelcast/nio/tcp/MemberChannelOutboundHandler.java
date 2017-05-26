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

package com.hazelcast.nio.tcp;

import com.hazelcast.internal.networking.ChannelOutboundHandler;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Packet;

import java.nio.ByteBuffer;

/**
 * A {@link ChannelOutboundHandler} that for member to member communication.
 *
 * It writes {@link Packet} instances to the {@link ByteBuffer}.
 *
 * @see MemberChannelInboundHandler
 */
public class MemberChannelOutboundHandler implements ChannelOutboundHandler<Packet> {
    ILogger logger = Logger.getLogger(getClass());

    @Override
    public boolean onWrite(Packet packet, ByteBuffer dst) {
        int before = dst.position();

        boolean finished = packet.writeTo(dst);

        int after = dst.position();

        //logger.info("Packet.size:"+(after-before));

        return finished;
    }
}
