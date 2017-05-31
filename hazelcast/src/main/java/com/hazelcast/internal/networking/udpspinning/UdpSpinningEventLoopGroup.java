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

package com.hazelcast.internal.networking.udpspinning;

import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelCloseListener;
import com.hazelcast.internal.networking.ChannelErrorHandler;
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.internal.networking.EventLoopGroup;
import com.hazelcast.internal.networking.nio.MigratableHandler;
import com.hazelcast.internal.networking.nio.NioChannelReader;
import com.hazelcast.internal.networking.nio.NioChannelWriter;
import com.hazelcast.internal.networking.nio.NioEventLoopGroup;
import com.hazelcast.internal.networking.nio.NioThread;
import com.hazelcast.internal.networking.nio.iobalancer.IOBalancer;
import com.hazelcast.internal.networking.udpnio.UdpNioChannel;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;

import java.io.IOException;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.networking.nio.SelectorMode.SELECT;
import static com.hazelcast.internal.networking.nio.SelectorMode.SELECT_NOW;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.HashUtil.hashToIndex;
import static com.hazelcast.util.Preconditions.checkInstanceOf;
import static com.hazelcast.util.ThreadUtil.createThreadPoolName;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.logging.Level.FINE;
import static java.util.logging.Level.INFO;

/**
 * A {@link EventLoopGroup} that uses (busy) spinning on the SocketChannels to see if there is something
 * to read or write.
 *
 * Currently there are 2 threads spinning:
 * <ol>
 * <li>1 thread spinning on all SocketChannels for reading</li>
 * <li>1 thread spinning on all SocketChannels for writing</li>
 * </ol>
 * In the future we need to play with this a lot more. 1 thread should be able to saturate a 40GbE connection, but that
 * currently doesn't work for us. So I guess our IO threads are doing too much stuff not relevant like writing the Frames
 * to bytebuffers or converting the bytebuffers to Frames.
 *
 * This is an experimental feature and disabled by default.
 */
public class UdpSpinningEventLoopGroup implements EventLoopGroup {

    private final ChannelCloseListener channelCloseListener = new ChannelCloseListenerImpl();
    private final ILogger logger;
    private final LoggingService loggingService;
   // private final SpinningInputThread inputThread;
    private final SpinningOutputThread outputThread;
    private final ChannelInitializer channelInitializer;
    private final ChannelErrorHandler errorHandler;
    private final MetricsRegistry metricsRegistry;
    private final ILogger readerLogger;
    private final ILogger writerLogger;
    private final List<SpinningUdpChannel> channels = new Vector<>();
    private NioThread[] inputThreads;
    private final AtomicInteger nextInputThreadIndex = new AtomicInteger();
    private int inputThreadCount = 3;
    private IOBalancer ioBalancer;

    public UdpSpinningEventLoopGroup(LoggingService loggingService,
                                     MetricsRegistry metricsRegistry,
                                     ChannelErrorHandler errorHandler,
                                     ChannelInitializer channelInitializer,
                                     String hzName) {
        this.loggingService = loggingService;
        this.logger = loggingService.getLogger(UdpSpinningEventLoopGroup.class);
        this.readerLogger = loggingService.getLogger(UdpSpinningChannelReader.class);
        this.writerLogger = loggingService.getLogger(UdpSpinningChannelWriter.class);
        this.metricsRegistry = metricsRegistry;
        this.errorHandler = errorHandler;
       // this.inputThread = new SpinningInputThread(hzName);
        this.outputThread = new SpinningOutputThread(hzName);
        this.channelInitializer = channelInitializer;
    }

    @Override
    public void register(final Channel channel) {
        SpinningUdpChannel spinningChannel = checkInstanceOf(SpinningUdpChannel.class, channel);
        try {
            spinningChannel.getDatagramChannel().configureBlocking(false);
        } catch (IOException e) {
            throw rethrow(e);
        }

        channels.add(spinningChannel);

        NioChannelReader reader = newChannelReader(spinningChannel);

        spinningChannel.setReader(reader);

        ioBalancer.channelAdded(reader, new MigratableHandler() {
            @Override
            public void requestMigration(NioThread newOwner) {

            }

            @Override
            public NioThread getOwner() {
                return null;
            }

            @Override
            public long getLoad() {
                return 0;
            }
        });

        reader.start();

        UdpSpinningChannelWriter writer = new UdpSpinningChannelWriter(
                spinningChannel, writerLogger, errorHandler, channelInitializer);
        spinningChannel.setWriter(writer);
        outputThread.register(writer);

        String metricsId = channel.getLocalSocketAddress() + "->" + channel.getRemoteSocketAddress();
        metricsRegistry.scanAndRegister(writer, "tcp.connection[" + metricsId + "].out");
        metricsRegistry.scanAndRegister(reader, "tcp.connection[" + metricsId + "].in");

        channel.addCloseListener(channelCloseListener);
    }

    @Override
    public void start() {
        logger.info("TcpIpConnectionManager configured with Spinning IO-threading model: "
                + "1 input thread and 1 output thread");
        outputThread.start();

        this.inputThreads = new NioThread[inputThreadCount];

        for (int i = 0; i < inputThreads.length; i++) {
            NioThread thread = new NioThread(
                    createThreadPoolName("doo", "IO") + "in-" + i,
                    loggingService.getLogger(NioThread.class),
                    errorHandler,
                    SELECT,
                    null);
            thread.id = i;
            inputThreads[i] = thread;
            metricsRegistry.scanAndRegister(thread, "tcp.inputThread[" + thread.getName() + "]");
            thread.start();
        }

        startIOBalancer();
//
//        new Thread(new Runnable() {
//            @Override
//            public void run() {
//                for (; ; ) {
//                    try {
//                        Thread.sleep(1000);
//                    } catch (InterruptedException e) {
//                    }
//
//                    for(SpinningUdpChannel channel:channels){
//                        UdpSpinningChannelWriter writer = channel.getWriter();
//                        System.out.println(writer);
//                        //System.out.println(writer.);
//                    }
//                }
//            }
//        }).start();
    }

    private NioChannelReader newChannelReader(SpinningUdpChannel channel) {
        int index = hashToIndex(nextInputThreadIndex.getAndIncrement(), inputThreadCount);
        NioThread[] threads = inputThreads;
        if (threads == null) {
            throw new IllegalStateException("IO thread is closed!");
        }

        return new NioChannelReader(
                channel,
                channel.getDatagramChannel(),
                threads[index],
                loggingService.getLogger(NioChannelReader.class),
                ioBalancer,
                channelInitializer);
    }


    private void startIOBalancer() {
        ioBalancer = new IOBalancer(inputThreads, new NioThread[0], "foo", 20, loggingService);
        ioBalancer.start();
        metricsRegistry.scanAndRegister(ioBalancer, "tcp.balancer");
    }

    @Override
    public void shutdown() {
        outputThread.shutdown();
    }

    private class ChannelCloseListenerImpl implements ChannelCloseListener {
        @Override
        public void onClose(Channel channel) {
            SpinningUdpChannel spinningChannel = (SpinningUdpChannel) channel;

            metricsRegistry.deregister(spinningChannel.getReader());
            metricsRegistry.deregister(spinningChannel.getWriter());

            outputThread.unregister(spinningChannel.getWriter());
           // inputThread.unregister(spinningChannel.getReader());
        }
    }
}
