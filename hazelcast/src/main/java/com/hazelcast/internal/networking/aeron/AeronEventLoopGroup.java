package com.hazelcast.internal.networking.aeron;

import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelErrorHandler;
import com.hazelcast.internal.networking.EventLoopGroup;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static java.lang.String.format;
import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

public class AeronEventLoopGroup implements EventLoopGroup {
    private final static AtomicReferenceFieldUpdater SUBSCRIPTIONS = newUpdater(
            AeronEventLoopGroup.class, AeronChannel[].class, "channels");

    private volatile AeronChannel[] channels = new AeronChannel[0];

    private final LoggingService loggingService;
    private final ChannelErrorHandler exceptionHandler;
    private final FragmentHandlerFactory fragmentHandlerFactory;
    private final ILogger logger;
    private Aeron aeron;
    private MediaDriver driver;
    private volatile boolean shutdown;

    public AeronEventLoopGroup(LoggingService loggingService,
                               ChannelErrorHandler exceptionHandler,
                               FragmentHandlerFactory fragmentHandlerFactory) {
        this.loggingService = loggingService;
        this.exceptionHandler = exceptionHandler;
        this.fragmentHandlerFactory = fragmentHandlerFactory;
        this.logger = loggingService.getLogger(AeronEventLoopGroup.class);
    }

    @Override
    public void register(Channel c) {
        AeronChannel channel = (AeronChannel) c;

        logger.info("register channel:" + channel.socket().getLocalAddress() + "->"
                + channel.socket().getRemoteSocketAddress());

        System.out.println("Channel.class:" + channel.getClass());

        InetSocketAddress remote = (InetSocketAddress) channel.getRemoteSocketAddress();

        Publication publication = aeron.addPublication(
                format("aeron:udp?endpoint=%s:%s", remote.getHostName(), remote.getPort()), 1);

        InetSocketAddress local = (InetSocketAddress) channel.getLocalSocketAddress();

        Subscription subscription = aeron.addSubscription(
                format("aeron:udp?endpoint=%s:%s", local.getHostName(), local.getPort()), 1);

        channel.publication = publication;
        channel.subscription = subscription;
        channel.handler = fragmentHandlerFactory.create(c);

        for (; ; ) {
            AeronChannel[] old = channels;
            AeronChannel[] update = new AeronChannel[old.length + 1];
            System.arraycopy(old, 0, update, 0, old.length);
            update[update.length - 1] = channel;
            if (SUBSCRIPTIONS.compareAndSet(this, old, update)) {
                break;
            }
        }


        System.out.println("Created publication and subscription");
    }

    public class SubscriptionPollingThread extends Thread {
        public SubscriptionPollingThread() {
            super("SubscriptionPollingThread");
        }

        public void run() {
            while (!shutdown) {
                for (AeronChannel channel : channels) {
                    channel.subscription.poll(channel.handler, 10);
                }
            }
        }
    }

    @Override
    public void start() {
        logger.info("Starting AeronEventLoopGroup");

        logger.info("Creating media driver");
        String path = new File("/tmp/aeron-media/" + System.currentTimeMillis()).getAbsolutePath();
        logger.info("Media driver path:" + path);
        MediaDriver.Context mediaContext = new MediaDriver.Context()
                .threadingMode(ThreadingMode.DEDICATED)
                .publicationTermBufferLength(128 * 1024 * 1024)
                .aeronDirectoryName(path);
        this.driver = MediaDriver.launchEmbedded(mediaContext);

        logger.info("Creating aeron");
        Aeron.Context ctx = new Aeron.Context()
                .aeronDirectoryName(driver.aeronDirectoryName());
        this.aeron = Aeron.connect(ctx);

        SubscriptionPollingThread thread = new SubscriptionPollingThread();
        thread.start();

        logger.info("Created");
    }

    @Override
    public void shutdown() {
        logger.info("Shutting down AeronEventLoopGroup");

        shutdown = true;
        driver.close();
        aeron.close();
    }
}
