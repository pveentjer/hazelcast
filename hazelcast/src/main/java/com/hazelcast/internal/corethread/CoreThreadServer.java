package com.hazelcast.internal.corethread;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.internal.util.ThreadAffinity;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
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

import static com.hazelcast.internal.util.ThreadAffinity.newSystemThreadAffinity;

public class CoreThreadServer {

    private static final int SERVER_PORT_DELTA = 10000;
    private static final int BUFFER_SIZE = 128 * 1024;

    private final Address thisAddress;
    private final OperationServiceImpl operationService;
    private final String executorString;
    private final Object[] partitionLocks;
    private MultithreadEventLoopGroup bossGroup;
    private MultithreadEventLoopGroup workerGroup;
    private Bootstrap clientBootstrap;
    private ServerConnectionManager serverConnectionManager;
    private final int threadCount;
    private final Mode mode;
    private final ThreadAffinity threadAffinity;
    private final boolean batch;

    enum Mode {NIO, EPOLL, IO_URING}

    public CoreThreadServer(Address thisAddress, OperationService os) {
        this.thisAddress = thisAddress;
        this.operationService = (OperationServiceImpl) os;

        this.batch = Boolean.parseBoolean(System.getProperty("batch", "true"));

        this.threadAffinity = newSystemThreadAffinity("affinity");
        if (threadAffinity.isEnabled()) {
            this.threadCount = threadAffinity.getThreadCount();
        } else {
            this.threadCount = Integer.parseInt(System.getProperty("threads", "" + Runtime.getRuntime().availableProcessors()));
        }

        String modeString = System.getProperty("mode", "nio");
        switch (modeString) {
            case "nio":
                mode = Mode.NIO;
                break;
            case "epoll":
                mode = Mode.EPOLL;
                break;
            case "io_uring":
                mode = Mode.IO_URING;
                break;
            default:
                throw new RuntimeException("Unrecognized mode:" + modeString);
        }

        this.executorString = System.getProperty("executor", "unsynchronized");

        System.out.println("Mode:" + mode + " executor:" + executorString + " threadCount:"
                + threadCount + " thread affinity:" + threadAffinity + " batch:" + batch);

        int partitionCount = operationService.getNode().partitionService.getPartitionCount();
        this.partitionLocks = new Object[partitionCount];
        for (int k = 0; k < partitionCount; k++) {
            partitionLocks[k] = new Object();
        }
    }

    public void setServerConnectionManager(ServerConnectionManager serverConnectionManager) {
        this.serverConnectionManager = serverConnectionManager;
    }

    public void start() {
        int inetPort = thisAddress.getPort() + SERVER_PORT_DELTA;
        System.out.println("Started ThreadPerCoreServer on " + (thisAddress.getHost() + " " + inetPort));

        bossGroup = newBossGroup();
        workerGroup = newWorkerGroup();

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
        serverBootstrap.bind(inetPort);

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

    @NotNull
    public OperationExecutor newOperationExecutor() {
        switch (executorString) {
            case "unsynchronized":
                return new UnsynchronizedOperationExecutor(operationService, batch);
            case "partitionlock":
                return new PartitionLockOperationExecutor(operationService, batch, partitionLocks);
            case "offloading":
                return new OffloadingOperatingExecutor(operationService, batch);
            default:
                throw new RuntimeException("unrecognized executor ["+executorString+"]");
        }
    }

    private Class<? extends ServerSocketChannel> getServerSocketChannelClass() {
        switch (mode) {
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
        switch (mode) {
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
        switch (mode) {
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
        switch (mode) {
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

    public Channel connect(Address address) {
        if (address == null) {
            throw new NullPointerException("Address can't be null");
        }
        ChannelFuture future = clientBootstrap.connect(address.getHost(), address.getPort() + SERVER_PORT_DELTA);
        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        return future.channel();
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
