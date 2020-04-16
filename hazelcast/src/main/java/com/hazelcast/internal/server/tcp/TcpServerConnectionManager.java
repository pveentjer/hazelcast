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

package com.hazelcast.internal.server.tcp;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.EndpointConfig;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.internal.networking.Networking;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionLifecycleListener;
import com.hazelcast.internal.nio.ConnectionListener;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.server.NetworkStats;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.internal.server.ServerContext;
import com.hazelcast.internal.server.tcp.AbstractChannelInitializer.MemberHandshakeHandler;
import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.internal.util.HashUtil;
import com.hazelcast.internal.util.MutableLong;
import com.hazelcast.internal.util.counters.MwCounter;
import com.hazelcast.internal.util.executor.StripedRunnable;
import com.hazelcast.logging.ILogger;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_ENDPOINT_MANAGER_ACCEPTED_SOCKET_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_ENDPOINT_MANAGER_ACTIVE_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_ENDPOINT_MANAGER_CLOSED_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_ENDPOINT_MANAGER_CONNECTION_LISTENER_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_ENDPOINT_MANAGER_OPENED_COUNT;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.nio.IOUtil.close;
import static com.hazelcast.internal.nio.IOUtil.setChannelOptions;
import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.counters.MwCounter.newMwCounter;
import static com.hazelcast.spi.properties.ClusterProperty.CHANNEL_COUNT;
import static java.util.Collections.newSetFromMap;
import static java.util.Collections.unmodifiableSet;

@SuppressWarnings("checkstyle:methodcount")
public class TcpServerConnectionManager
        implements ServerConnectionManager, Consumer<Packet>, DynamicMetricsProvider {

    private static final int RETRY_NUMBER = 5;
    private static final long DELAY_FACTOR = 100L;

    //@Probe(name = TCP_METRIC_ENDPOINT_MANAGER_IN_PROGRESS_COUNT)

    final Plane[] planes;

    @Probe(name = TCP_METRIC_ENDPOINT_MANAGER_ACTIVE_COUNT, level = MANDATORY)
    final Set<TcpServerConnection> activeConnections = newSetFromMap(new ConcurrentHashMap<>());

    @Probe(name = TCP_METRIC_ENDPOINT_MANAGER_ACCEPTED_SOCKET_COUNT, level = MANDATORY)
    final Set<Channel> acceptedChannels = newSetFromMap(new ConcurrentHashMap<>());

    @Probe(name = TCP_METRIC_ENDPOINT_MANAGER_CONNECTION_LISTENER_COUNT)
    final Set<ConnectionListener> connectionListeners = new CopyOnWriteArraySet<>();

    private final ILogger logger;
    private final ServerContext serverContext;
    private final EndpointConfig endpointConfig;
    private final EndpointQualifier endpointQualifier;
    private final Function<EndpointQualifier, ChannelInitializer> channelInitializerFn;
    private final TcpServer server;
    private final TcpServerConnector connector;
    private final MemberHandshakeHandler memberHandshakeHandler;
    private final NetworkStatsImpl networkStats;
    private final ConstructorFunction<Address, TcpServerConnectionErrorHandler> monitorConstructor =
            endpoint -> new TcpServerConnectionErrorHandler(TcpServerConnectionManager.this, endpoint);

    private final AtomicInteger connectionIdGen = new AtomicInteger();

    @Probe(name = TCP_METRIC_ENDPOINT_MANAGER_OPENED_COUNT)
    private final MwCounter openedCount = newMwCounter();

    @Probe(name = TCP_METRIC_ENDPOINT_MANAGER_CLOSED_COUNT)
    private final MwCounter closedCount = newMwCounter();

    private final EndpointConnectionLifecycleListener connectionLifecycleListener = new EndpointConnectionLifecycleListener();
    private final int planeCount;


    static class Plane {
        final ConcurrentHashMap<Address, TcpServerConnection> connectionMap = new ConcurrentHashMap<>(100);
        final Set<Address> connectionsInProgress = newSetFromMap(new ConcurrentHashMap<>());
        final ConcurrentHashMap<Address, TcpServerConnectionErrorHandler> monitors = new ConcurrentHashMap<>(100);
        final int index;

        public Plane(int index) {
            this.index = index;
        }
    }

    TcpServerConnectionManager(TcpServer server,
                               EndpointConfig endpointConfig,
                               Function<EndpointQualifier, ChannelInitializer> channelInitializerFn,
                               ServerContext serverContext,
                               Set<ProtocolType> supportedProtocolTypes) {
        this.server = server;
        this.endpointConfig = endpointConfig;
        this.endpointQualifier = endpointConfig != null ? endpointConfig.getQualifier() : null;
        this.channelInitializerFn = channelInitializerFn;
        this.serverContext = serverContext;
        this.logger = serverContext.getLoggingService().getLogger(TcpServerConnectionManager.class);
        this.connector = new TcpServerConnector(this);
        this.memberHandshakeHandler = new MemberHandshakeHandler(this, serverContext, logger, supportedProtocolTypes);
        this.networkStats = endpointQualifier == null ? null : new NetworkStatsImpl();
        this.planeCount = serverContext.properties().getInteger(CHANNEL_COUNT);
        System.out.println("ConnectionCount:" + planeCount);
        this.planes = new Plane[planeCount];
        for (int planeIndex = 0; planeIndex < planes.length; planeIndex++) {
            planes[planeIndex] = new Plane(planeIndex);
        }
    }

    public TcpServer getServer() {
        return server;
    }

    public EndpointQualifier getEndpointQualifier() {
        return endpointQualifier;
    }

    public Collection<ServerConnection> getActiveConnections() {
        return unmodifiableSet(activeConnections);
    }

    public Collection<ServerConnection> getConnections() {
        Collection<ServerConnection> connections = new HashSet<>();
        for (Plane plane : planes) {
            connections.addAll(plane.connectionMap.values());
        }
        return connections;
    }

    @Override
    public void addConnectionListener(ConnectionListener listener) {
        checkNotNull(listener, "listener can't be null");
        connectionListeners.add(listener);
    }

    @Override
    public synchronized void accept(Packet packet) {
        memberHandshakeHandler.process(packet);
    }

    @Override
    public ServerConnection get(Address address, int streamId) {
        int planeIndex = HashUtil.hashToIndex(streamId, planeCount);
        return planes[planeIndex].connectionMap.get(address);
    }

    @Override
    public ServerConnection getOrConnect(Address address, int streamId) {
        return getOrConnect(address, false, streamId);
    }

    @Override
    public ServerConnection getOrConnect(final Address address, final boolean silent, int streamId) {
        Plane plane = getPlane(streamId);
        TcpServerConnection connection = plane.connectionMap.get(address);
        if (connection == null && server.isLive()) {
            if (plane.connectionsInProgress.add(address)) {
                connector.asyncConnect(address, silent, plane.index);
            }
        }
        return connection;
    }

    @Override
    public synchronized boolean register(final Address remoteAddress, final ServerConnection c, int streamId) {
        Plane plane = getPlane(streamId);
        TcpServerConnection connection = (TcpServerConnection) c;
        try {
            if (remoteAddress.equals(serverContext.getThisAddress())) {
                return false;
            }

            if (!connection.isAlive()) {
                if (logger.isFinestEnabled()) {
                    logger.finest(connection + " to " + remoteAddress + " is not registered since connection is not active.");
                }
                return false;
            }

            Address currentRemoteAddress = connection.getRemoteAddress();
            if (currentRemoteAddress != null && !currentRemoteAddress.equals(remoteAddress)) {
                throw new IllegalArgumentException(connection + " has already a different remoteAddress than: " + remoteAddress);
            }
            connection.setRemoteAddress(remoteAddress);

            if (!connection.isClient()) {
                connection.setErrorHandler(getErrorHandler(remoteAddress, plane.index, true));
            }

            plane.connectionMap.put(remoteAddress, connection);

            serverContext.getEventService().executeEventCallback(new StripedRunnable() {
                @Override
                public void run() {
                    connectionListeners.forEach(listener -> listener.connectionAdded(connection));
                }

                @Override
                public int getKey() {
                    return remoteAddress.hashCode();
                }
            });
            return true;
        } finally {
            plane.connectionsInProgress.remove(remoteAddress);
        }
    }

    public Plane getPlane(int streamId) {
        int planeIndex = HashUtil.hashToIndex(streamId, planeCount);
        return planes[planeIndex];
    }

    private void fireConnectionRemovedEvent(final Connection connection, final Address endPoint) {
        if (server.isLive()) {
            serverContext.getEventService().executeEventCallback(new StripedRunnable() {
                @Override
                public void run() {
                    connectionListeners.forEach(listener -> listener.connectionRemoved(connection));
                }

                @Override
                public int getKey() {
                    return endPoint.hashCode();
                }
            });
        }
    }

    public synchronized void reset(boolean cleanListeners) {
        acceptedChannels.forEach(IOUtil::closeResource);
        for (Plane plane : planes) {
            plane.connectionMap.values().forEach(conn -> close(conn, "TcpServer is stopping"));
            plane.connectionMap.clear();
        }

        activeConnections.forEach(conn -> close(conn, "TcpServer is stopping"));
        acceptedChannels.clear();
        for (Plane plane : planes) {
            plane.connectionsInProgress.clear();
        }

        for (Plane plane : planes) {
            plane.monitors.clear();
        }

        activeConnections.clear();

        if (cleanListeners) {
            connectionListeners.clear();
        }
    }

    @Override
    public boolean transmit(Packet packet, Address target, int streamId) {
        checkNotNull(packet, "packet can't be null");
        checkNotNull(target, "target can't be null");
        return send(packet, target, null, streamId);
    }

    @Override
    public NetworkStats getNetworkStats() {
        return networkStats;
    }

    void refreshNetworkStats() {
        if (networkStats != null) {
            networkStats.refresh();
        }
    }

    private TcpServerConnectionErrorHandler getErrorHandler(Address endpoint, int planeIndex, boolean reset) {
        TcpServerConnectionErrorHandler monitor = getOrPutIfAbsent(planes[planeIndex].monitors, endpoint, monitorConstructor);
        if (reset) {
            monitor.reset();
        }
        return monitor;
    }

    Channel newChannel(SocketChannel socketChannel, boolean clientMode) throws IOException {
        Networking networking = server.getNetworking();
        ChannelInitializer channelInitializer = channelInitializerFn.apply(endpointQualifier);
        assert channelInitializer != null : "Found NULL channel initializer for endpoint-qualifier " + endpointQualifier;
        Channel channel = networking.register(channelInitializer, socketChannel, clientMode);
        // Advanced Network
        if (endpointConfig != null) {
            setChannelOptions(channel, endpointConfig);
        }
        acceptedChannels.add(channel);
        return channel;
    }

    void removeAcceptedChannel(Channel channel) {
        acceptedChannels.remove(channel);
    }

    void failedConnection(Address address, int planeIndex, Throwable t, boolean silent) {

        // connectionsInProgress.remove(address);
        serverContext.onFailedConnection(address);
        if (!silent) {
            getErrorHandler(address, planeIndex, false).onError(t);
        }
    }

    synchronized TcpServerConnection newConnection(Channel channel, Address endpoint) {
        try {
            if (!server.isLive()) {
                throw new IllegalStateException("connection manager is not live!");
            }

            TcpServerConnection connection = new TcpServerConnection(this, connectionLifecycleListener,
                    connectionIdGen.incrementAndGet(), channel);

            connection.setRemoteAddress(endpoint);
            activeConnections.add(connection);

            if (logger.isFineEnabled()) {
                logger.fine("Established socket connection between " + channel.localSocketAddress() + " and " + channel
                        .remoteSocketAddress());
            }
            openedCount.inc();

            channel.start();

            return connection;
        } finally {
            acceptedChannels.remove(channel);
        }
    }

    private boolean send(Packet packet, Address target, SendTask sendTask, int streamId) {
        //streamId =0;

        Connection connection = get(target, streamId);
        if (connection != null) {
            return connection.write(packet);
        }

        if (sendTask == null) {
            sendTask = new SendTask(packet, target, streamId);
        }

        int retries = sendTask.retries;
        if (retries < RETRY_NUMBER && serverContext.isNodeActive()) {
            getOrConnect(target, true, streamId);
            try {
                server.scheduleDeferred(sendTask, (retries + 1) * DELAY_FACTOR, TimeUnit.MILLISECONDS);
                return true;
            } catch (RejectedExecutionException e) {
                if (server.isLive()) {
                    throw e;
                }
                if (logger.isFinestEnabled()) {
                    logger.finest("Packet send task is rejected. Packet cannot be sent to " + target);
                }
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "TcpServerConnectionManager{"
                + "endpointQualifier=" + endpointQualifier
                + ", connectionsMap=" + null + '}';
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
//        MetricDescriptor rootDescriptor = descriptor.withPrefix(TCP_PREFIX_CONNECTION);
//        if (endpointQualifier == null) {
//            context.collect(rootDescriptor.copy(), this);
//        } else {
//            context.collect(rootDescriptor
//                    .copy()
//                    .withDiscriminator(TCP_DISCRIMINATOR_ENDPOINT, endpointQualifier.toMetricsPrefixString()), this);
//        }
//
//        for (TcpServerConnection connection : activeConnections) {
//            if (connection.getRemoteAddress() != null) {
//                context.collect(rootDescriptor
//                        .copy()
//                        .withDiscriminator(TCP_DISCRIMINATOR_ENDPOINT, connection.getRemoteAddress().toString()), connection);
//            }
//        }
//
//        int clientCount = 0;
//        int textCount = 0;
//        for (Map.Entry<Address, TcpServerConnection> entry : connectionsMap.entrySet()) {
//            Address bindAddress = entry.getKey();
//            TcpServerConnection connection = entry.getValue();
//            if (connection.isClient()) {
//                clientCount++;
//                String connectionType = connection.getConnectionType();
//                if (REST_CLIENT.equals(connectionType) || MEMCACHE_CLIENT.equals(connectionType)) {
//                    textCount++;
//                }
//            }
//
//            if (connection.getRemoteAddress() != null) {
//                context.collect(rootDescriptor
//                        .copy()
//                        .withDiscriminator(TCP_DISCRIMINATOR_BINDADDRESS, bindAddress.toString())
//                        .withTag(TCP_TAG_ENDPOINT, connection.getRemoteAddress().toString()), connection);
//            }
//        }
//
//        if (endpointConfig == null) {
//            context.collect(rootDescriptor.copy(), TCP_METRIC_CLIENT_COUNT, MANDATORY, COUNT, clientCount);
//            context.collect(rootDescriptor.copy(), TCP_METRIC_TEXT_COUNT, MANDATORY, COUNT, textCount);
//        }
    }

    private final class SendTask implements Runnable {
        private final Packet packet;
        private final Address target;
        private final int streamId;
        private volatile int retries;

        private SendTask(Packet packet, Address target, int streamId) {
            this.packet = packet;
            this.target = target;
            this.streamId = streamId;
        }

        @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT", justification = "single-writer, many-reader")
        @Override
        public void run() {
            System.out.println("SendTask:" + target + " streamId: " + streamId + " retries:" + retries);
            retries++;
            if (logger.isFinestEnabled()) {
                logger.finest("Retrying[" + retries + "] packet send operation to: " + target);
            }
            send(packet, target, this, streamId);
        }
    }

    public final class EndpointConnectionLifecycleListener implements ConnectionLifecycleListener<TcpServerConnection> {

        @Override
        public void onConnectionClose(TcpServerConnection connection, Throwable t, boolean silent) {
            closedCount.inc();

            activeConnections.remove(connection);

            if (networkStats != null) {
                // Note: this call must happen after activeConnections.remove
                networkStats.onConnectionClose(connection);
            }

            Address endPoint = connection.getRemoteAddress();
            if (endPoint != null) {
                // connectionsInProgress.remove(endPoint);
                //todo
                //connectionsMap.remove(endPoint, connection);
                fireConnectionRemovedEvent(connection, endPoint);
            }

            if (t != null) {
                serverContext.onFailedConnection(endPoint);
                if (!silent) {
                    getErrorHandler(endPoint, connection.getPlaneIndex(), false).onError(t);
                }
            }
        }
    }

    private class NetworkStatsImpl implements NetworkStats {
        private final AtomicLong bytesReceivedLastCalc = new AtomicLong();
        private final MwCounter bytesReceivedOnClosed = newMwCounter();
        private final AtomicLong bytesSentLastCalc = new AtomicLong();
        private final MwCounter bytesSentOnClosed = newMwCounter();

        @Override
        public long getBytesReceived() {
            return bytesReceivedLastCalc.get();
        }

        @Override
        public long getBytesSent() {
            return bytesSentLastCalc.get();
        }

        void refresh() {
            MutableLong totalReceived = MutableLong.valueOf(bytesReceivedOnClosed.get());
            MutableLong totalSent = MutableLong.valueOf(bytesSentOnClosed.get());
            activeConnections.forEach(conn -> {
                totalReceived.value += conn.getChannel().bytesRead();
                totalSent.value += conn.getChannel().bytesWritten();
            });
            // counters must be monotonically increasing
            bytesReceivedLastCalc.updateAndGet((v) -> Math.max(v, totalReceived.value));
            bytesSentLastCalc.updateAndGet((v) -> Math.max(v, totalSent.value));
        }

        void onConnectionClose(TcpServerConnection connection) {
            bytesReceivedOnClosed.inc(connection.getChannel().bytesRead());
            bytesSentOnClosed.inc(connection.getChannel().bytesWritten());
        }
    }
}
