package com.hazelcast.internal.corethread;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.internal.util.ThreadAffinity;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.uring.IOUringEventLoopGroup;
import io.netty.channel.uring.IOUringServerSocketChannel;
import io.netty.channel.uring.IOUringSocketChannel;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ExecutionException;

import static com.hazelcast.internal.corethread.CoreThreadServer.ExecutorMode.OFFLOADING;
import static com.hazelcast.internal.corethread.CoreThreadServer.ExecutorMode.PARTITION_LOCK;
import static com.hazelcast.internal.corethread.CoreThreadServer.ExecutorMode.PER_CORE;
import static com.hazelcast.internal.corethread.CoreThreadServer.ExecutorMode.UNSYNCHRONIZED;
import static com.hazelcast.internal.corethread.CoreThreadServer.MultiplexMode.EPOLL;
import static com.hazelcast.internal.corethread.CoreThreadServer.MultiplexMode.IO_URING;
import static com.hazelcast.internal.corethread.CoreThreadServer.MultiplexMode.NIO;
import static com.hazelcast.internal.util.ThreadAffinity.newSystemThreadAffinity;

public class CoreThreadServer {

    private static final int SERVER_PORT_DELTA = 10000;
    private static final int BUFFER_SIZE = 128 * 1024;

    private final Address thisAddress;
    private final OperationServiceImpl operationService;
    private final ExecutorMode executorMode;
    private Object[] partitionLocks;
    private MultithreadEventLoopGroup bossGroup;
    private MultithreadEventLoopGroup workerGroup;
    private Bootstrap clientBootstrap;
    private ServerConnectionManager serverConnectionManager;
    private final int threadCount;
    private final MultiplexMode multiplexMode;
    private final ThreadAffinity threadAffinity;
    private final boolean batch;

    enum MultiplexMode {NIO, EPOLL, IO_URING}

    enum ExecutorMode {UNSYNCHRONIZED, PARTITION_LOCK, OFFLOADING, PER_CORE}

    public CoreThreadServer(Address thisAddress, OperationService os, HazelcastProperties properties) {
        this.thisAddress = thisAddress;
        this.operationService = (OperationServiceImpl) os;
        this.batch = Boolean.parseBoolean(System.getProperty("batch", "true"));

        this.threadAffinity = newSystemThreadAffinity("affinity");
        if (threadAffinity.isEnabled()) {
            this.threadCount = threadAffinity.getThreadCount();
        } else {
            this.threadCount = Integer.parseInt(System.getProperty("threads", "" + Runtime.getRuntime().availableProcessors()));
        }

        multiplexMode = getMultiplexMode();
        executorMode = getExecutorMode();

        System.out.println("Mode:" + multiplexMode + " executor:" + executorMode + " threadCount:"
                + threadCount + " thread affinity:" + threadAffinity + " batch:" + batch);

        if (executorMode == PARTITION_LOCK) {
            int partitionCount = properties.getInteger(ClusterProperty.PARTITION_COUNT);
            this.partitionLocks = new Object[partitionCount];
            for (int k = 0; k < partitionCount; k++) {
                partitionLocks[k] = new Object();
            }
        } else if (executorMode == PER_CORE) {
            int channelCount = properties.getInteger(ClusterProperty.CHANNEL_COUNT);
            if (channelCount != threadCount) {
                throw new RuntimeException("Channel must be same as thread count. "
                        + "channelcount=" + channelCount + " threadCount=" + threadCount);
            }
        }
    }

    private ExecutorMode getExecutorMode() {
        String executorModeString = System.getProperty("executor", "unsynchronized");

        switch (executorModeString) {
            case "unsynchronized":
                return UNSYNCHRONIZED;
            case "partition_lock":
                return PARTITION_LOCK;
            case "offloading":
                return OFFLOADING;
            case "per_core":
                return PER_CORE;
            default:
                throw new RuntimeException("unrecognized executorMode [" + executorModeString + "]");
        }
    }

    @NotNull
    private MultiplexMode getMultiplexMode() {
        String modeString = System.getProperty("multiplex", "nio");
        switch (modeString) {
            case "nio":
                return NIO;
            case "epoll":
                return EPOLL;
            case "io_uring":
                return IO_URING;
            default:
                throw new RuntimeException("Unrecognized multiplexMode:" + modeString);
        }
    }

    public void setServerConnectionManager(ServerConnectionManager serverConnectionManager) {
        this.serverConnectionManager = serverConnectionManager;
    }

    public void start() {
        bossGroup = newBossGroup();
        workerGroup = newWorkerGroup();

        newServerBootstrap();
        newClientBootstrap();
    }

    private void newClientBootstrap() {
        clientBootstrap = new Bootstrap();
        clientBootstrap.group(workerGroup);
        clientBootstrap.channel(getChannelClass());
        clientBootstrap.handler(new ChannelInitializer<Channel>() {
            protected void initChannel(Channel ch) {
                ch.pipeline().addLast(
                        new LinkEncoder(thisAddress),
                        new PacketEncoder(),
                        new PacketDecoder(thisAddress),
                        newOperationExecutor());
            }
        }).option(ChannelOption.SO_RCVBUF, BUFFER_SIZE)
                .option(ChannelOption.SO_SNDBUF, BUFFER_SIZE)
                .option(ChannelOption.TCP_NODELAY, true);
    }

    private void newServerBootstrap() {
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(bossGroup, workerGroup)
                .channel(getServerSocketChannelClass())
                .childHandler(new ChannelInitializer<Channel>() {
                    @Override
                    public void initChannel(Channel ch) {
                        ch.pipeline().addLast(
                                new LinkDecoder(thisAddress, serverConnectionManager),
                                new PacketEncoder(),
                                new PacketDecoder(thisAddress),
                                newOperationExecutor());
                    }
                })
                .childOption(ChannelOption.SO_RCVBUF, BUFFER_SIZE)
                .childOption(ChannelOption.SO_SNDBUF, BUFFER_SIZE)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true);

        if (executorMode == PER_CORE) {
            for (int k = 0; k < threadCount; k++) {
                int port = thisAddress.getPort() + SERVER_PORT_DELTA + k * 128;
                System.out.println("Started ThreadPerCoreServer on " + (thisAddress.getHost() + " " + port));
                serverBootstrap.bind(port);
            }
        } else {
            int port = thisAddress.getPort() + SERVER_PORT_DELTA;
            System.out.println("Started ThreadPerCoreServer on " + (thisAddress.getHost() + " " + port));
            serverBootstrap.bind(port);
        }
    }

    @NotNull
    public OperationExecutor newOperationExecutor() {
        switch (executorMode) {
            case PER_CORE:
                return new UnsynchronizedOperationExecutor(operationService, batch);
            case UNSYNCHRONIZED:
                return new UnsynchronizedOperationExecutor(operationService, batch);
            case PARTITION_LOCK:
                return new PartitionLockOperationExecutor(operationService, batch, partitionLocks);
            case OFFLOADING:
                return new OffloadingOperatingExecutor(operationService, batch);
            default:
                throw new RuntimeException("Unknown executorMode: " + executorMode);
        }
    }

    private Class<? extends ServerSocketChannel> getServerSocketChannelClass() {
        switch (multiplexMode) {
            case NIO:
                return NioServerSocketChannel.class;
            case EPOLL:
                return EpollServerSocketChannel.class;
            case IO_URING:
                return IOUringServerSocketChannel.class;
            default:
                throw new RuntimeException();
        }
    }

    private Class<? extends SocketChannel> getChannelClass() {
        switch (multiplexMode) {
            case NIO:
                return NioSocketChannel.class;
            case EPOLL:
                return EpollSocketChannel.class;
            case IO_URING:
                return IOUringSocketChannel.class;
            default:
                throw new RuntimeException();
        }
    }

    private MultithreadEventLoopGroup newWorkerGroup() {
        switch (multiplexMode) {
            case NIO:
                return new NioEventLoopGroup(threadCount, new CoreThreadFactory(threadAffinity));
            case EPOLL:
                return new EpollEventLoopGroup(threadCount, new CoreThreadFactory(threadAffinity));
            case IO_URING:
                return new IOUringEventLoopGroup(threadCount, new CoreThreadFactory(threadAffinity));
            default:
                throw new RuntimeException();
        }
    }

    private MultithreadEventLoopGroup newBossGroup() {
        switch (multiplexMode) {
            case NIO:
                return new NioEventLoopGroup(threadCount);
            case EPOLL:
                return new EpollEventLoopGroup(threadCount);
            case IO_URING:
                return new IOUringEventLoopGroup(threadCount);
            default:
                throw new RuntimeException();
        }
    }

    public Channel connect(Address address, int cpu) {
        if (address == null) {
            throw new NullPointerException("Address can't be null");
        }

        ChannelFuture future = clientBootstrap.connect(address.getHost(), getPort(address, cpu));
        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        return future.channel();
    }

    public int getPort(Address address, int cpu) {
        if (executorMode == PER_CORE) {
            return address.getPort() + SERVER_PORT_DELTA + cpu * 128;
        } else {
            return address.getPort() + SERVER_PORT_DELTA;
        }
    }

    public void shutdown() {
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }

        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
    }
}
