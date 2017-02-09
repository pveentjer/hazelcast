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

import java.io.IOException;
import java.nio.channels.SocketChannel;

/**
 * Responsible for handling the handshake for a connection. During the handshake the protocol is determined and based on that
 * the connection will be initialized.
 *
 *
 * todo:
 * - integrate ssl
 * - set the buffer size on the socket
 * - clients
 * - closing
 * - member socket reader and the counters
 *
 * done:
 * - pulled out interface
 * - pulled out HandshakeFactory
 * - integrated the factories in node.
 */
public interface Handshake {

    /**
     * Tries to complete the handshake by reading the bytes from the SocketChannel. If all the protocol information is retrieved,
     * the handshake is complete.
     *
     * @param channel the SocketChannel to read from
     * @return
     * @throws IOException
     */
    boolean onRead(SocketChannel channel) throws IOException;

    /**
     * @return the protocol. If no protocol has yet been read, null is returned.
     */
    String getProtocol();
}
