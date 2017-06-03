package com.hazelcast.nio.tcp;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.EventLoopGroup;
import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionType;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.Packet;

import java.io.EOFException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.CancelledKeyException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The Tcp/Ip implementation of the {@link com.hazelcast.nio.Connection}.
 * <p>
 * A {@link TcpIpConnection} is not responsible for reading or writing data to the socket; that is task of
 * the {@link Channel}.
 *
 * @see EventLoopGroup
 */
@SuppressWarnings("checkstyle:methodcount")
public final class TcpIpConnection implements Connection {

    private final Channel[] channels;

    private final TcpIpConnectionManager connectionManager;

    private final AtomicBoolean alive = new AtomicBoolean(true);

    private final ILogger logger;

    private final int connectionId;

    private final IOService ioService;

    private Address endPoint;

    private TcpIpConnectionErrorHandler errorHandler;

    private volatile ConnectionType type = ConnectionType.NONE;

    private volatile Throwable closeCause;

    private volatile String closeReason;

    public TcpIpConnection(TcpIpConnectionManager connectionManager,
                           int connectionId,
                           Channel[] channels) {
        this.connectionId = connectionId;
        this.connectionManager = connectionManager;
        this.ioService = connectionManager.getIoService();
        this.logger = ioService.getLoggingService().getLogger(TcpIpConnection.class);
        this.channels = channels;


        for (int k = 0; k < channels.length; k++) {
            Channel channel = channels[k];
            ConcurrentMap attributeMap = channel.attributeMap();
            attributeMap.put(TcpIpConnection.class, this);
            attributeMap.put("channelIndex", k);
        }
    }

    public Channel getChannel() {
        return channels[0];
    }

    @Override
    public ConnectionType getType() {
        return type;
    }

    @Override
    public void setType(ConnectionType type) {
        if (this.type == ConnectionType.NONE) {
            this.type = type;
        }
    }

    @Probe
    private int getConnectionType() {
        ConnectionType t = type;
        return t == null ? -1 : t.ordinal();
    }

    public TcpIpConnectionManager getConnectionManager() {
        return connectionManager;
    }

    @Override
    public InetAddress getInetAddress() {
        return channels[0].socket().getInetAddress();
    }

    @Override
    public int getPort() {
        return channels[0].socket().getPort();
    }

    @Override
    public InetSocketAddress getRemoteSocketAddress() {
        return (InetSocketAddress) channels[0].getRemoteSocketAddress();
    }

    @Override
    public boolean isAlive() {
        return alive.get();
    }

    @Override
    public long lastWriteTimeMillis() {
        long result = 0;
        for (Channel channel : channels) {
            long w = channel.lastWriteTimeMillis();
            if (w > result) {
                result = w;
            }
        }

        return result;
    }

    @Override
    public long lastReadTimeMillis() {
        long result = 0;
        for (Channel channel : channels) {
            long r = channel.lastReadTimeMillis();
            if (r > result) {
                result = r;
            }
        }

        return result;
    }

    @Override
    public Address getEndPoint() {
        return endPoint;
    }

    public void setEndPoint(Address endPoint) {
        this.endPoint = endPoint;
    }

    public void setErrorHandler(TcpIpConnectionErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
    }

    public int getConnectionId() {
        return connectionId;
    }

    @Override
    public boolean isClient() {
        ConnectionType t = type;
        return t != null && t != ConnectionType.NONE && t.isClient();
    }

    private final AtomicLong selector = new AtomicLong();

    @Override
    public boolean write(OutboundFrame frame) {
//        int index = (int)(selector.incrementAndGet() % channels.length);
//        Channel channel = channels[index];

        Channel channel;
        if (frame instanceof Packet) {
            Packet packet = (Packet) frame;
            int index =  (packet.getPartitionId() % channels.length);
            channel = channels[index];
        } else {
            int index = (int) (selector.incrementAndGet() % channels.length);
            channel = channels[index];
        }

        if (channel.write(frame)) {
            return true;
        }

        if (logger.isFinestEnabled()) {
            logger.finest("Connection is closed, won't write packet -> " + frame);
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TcpIpConnection)) {
            return false;
        }
        TcpIpConnection that = (TcpIpConnection) o;
        return connectionId == that.getConnectionId();
    }

    @Override
    public int hashCode() {
        return connectionId;
    }

    @Override
    public void close(String reason, Throwable cause) {
        if (!alive.compareAndSet(true, false)) {
            // it is already closed.
            return;
        }

        this.closeCause = cause;
        this.closeReason = reason;

        logClose();

        try {
            for (Channel channel : channels)
                channel.close();
        } catch (Exception e) {
            logger.warning(e);
        }

        connectionManager.onConnectionClose(this);
        ioService.onDisconnect(endPoint, cause);
        if (cause != null && errorHandler != null) {
            errorHandler.onError(cause);
        }
    }

    private void logClose() {
        String message = toString() + " closed. Reason: ";
        if (closeReason != null) {
            message += closeReason;
        } else if (closeCause != null) {
            message += closeCause.getClass().getName() + "[" + closeCause.getMessage() + "]";
        } else {
            message += "Socket explicitly closed";
        }

        if (ioService.isActive()) {
            if (closeCause == null || closeCause instanceof EOFException || closeCause instanceof CancelledKeyException) {
                logger.info(message);
            } else {
                logger.warning(message, closeCause);
            }
        } else {
            if (closeCause == null) {
                logger.finest(message);
            } else {
                logger.finest(message, closeCause);
            }
        }
    }

    @Override
    public Throwable getCloseCause() {
        return closeCause;
    }

    @Override
    public String getCloseReason() {
        if (closeReason == null) {
            return closeCause == null ? null : closeCause.getMessage();
        } else {
            return closeReason;
        }
    }

    @Override
    public String toString() {
        String s = "Connection[id=" + connectionId;
        for (Channel channel : channels) {
            s += ", " + channel.getLocalSocketAddress() + "->" + channel.getRemoteSocketAddress();
        }
        s += ", endpoint=" + endPoint
                + ", alive=" + alive
                + ", type=" + type
                + "]";
        return s;
    }
}
