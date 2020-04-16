/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.server;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionListenable;
import com.hazelcast.internal.nio.Packet;

import java.util.Collection;
import java.util.function.Consumer;

/**
 * Responsible for managing {@link ServerConnection} instances.
 */
public interface ServerConnectionManager
        extends ConnectionListenable<ServerConnection>, Consumer<Packet> {

    /**
     * Returns connections that have been successfully established (ie. Bind was completed)
     */
    //todo: I think we can get rid of this method since it is very implementation specific.
    Collection<ServerConnection> getConnections();

    /**
     * Returns <b>all</b> active connections or not that are currently holding resources on this member.
     * If the connection never successfully finishes with the handshake then it will only be visible
     * through this sub-set.
     *
     * @return Collection of all connections hold on this member
     */
    Collection<ServerConnection> getActiveConnections();

    default boolean register(Address remoteAddress, ServerConnection connection){
        return register(remoteAddress,connection,0);
    }


    /**
     * Registers (ie. stores) the connection for the given remote remoteAddress.
     * Once this call finishes every subsequent call to {@link #get(Address)} will return
     * the relevant {@link Connection} resource.
     *
     * @param remoteAddress    - The remote endpoint to register the connection under
     * @param connection - The connection to be registered
     * @return True if the call was successful
     */
    boolean register(Address remoteAddress, ServerConnection connection, int streamId);

    /**
     * Gets the connection for a given address. If the connection does not exist, it returns null.
     *
     * @param address the remote side of the connection
     * @return the found Connection, or none if one doesn't exist
     */
    default ServerConnection get(Address address){
        return get(address, 0);
    }

    ServerConnection get(Address address, int streamId);

    /**
     * Gets the existing connection for a given address or connects. This call is silent.
     *
     * @param address the address to connect to
     * @return the found connection, or {@code null} if no connection exists
     * @see #getOrConnect(Address, boolean)
     */
    default ServerConnection getOrConnect(Address address){
        return getOrConnect(address, 0);
    }

    ServerConnection getOrConnect(Address address, int streamId);

    /**
     * Gets the existing connection for a given address. If it does not exist, the system will try to connect
     * asynchronously. In this case, it returns {@code null}.
     * <p>
     * When the connection is established at some point in time, it can be retrieved using the
     * {@link #get(Address)}.
     *
     * @param address the address to connect to
     * @param silent  if logging should be done on debug level ({@code silent=true}) or on info level ({@code silent=false})
     * @return the existing connection
     */
    default ServerConnection getOrConnect(Address address, boolean silent){
        return getOrConnect(address,silent,0);
    }

    ServerConnection getOrConnect(Address address, boolean silent, int streamId);

       /**
     * Transmits a packet to a certain address.
     * <p>
     * If the connection to the target doesn't exist yet, the system will try to make the connection. In this case
     * true can be returned, even though the connection eventually can't be established.
     *
     * @param packet The Packet to transmit.
     * @param target The address of the target machine where the Packet should be transmitted.
     * @return true if the transmit was a success, false if a failure.
     * @throws NullPointerException if packet or target is null.
     * @see #transmit(Packet, Connection)
     */
    default boolean transmit(Packet packet, Address target){
        return transmit(packet, target,0);
    }

    boolean transmit(Packet packet, Address target, int streamId);


    /**
     * Returns network stats for inbound and outbound traffic.
     * Stats are available only when Advanced Networking is enabled.
     *
     * @return network stats, or {@code null} if Advanced Networking is disabled
     */
    NetworkStats getNetworkStats();

    Server getServer();
}
