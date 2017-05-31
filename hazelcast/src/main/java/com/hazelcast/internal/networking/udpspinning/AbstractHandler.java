package com.hazelcast.internal.networking.udpspinning;

import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelErrorHandler;
import com.hazelcast.logging.ILogger;

import java.nio.channels.DatagramChannel;

public class AbstractHandler{

    protected final SpinningUdpChannel channel;
    protected final DatagramChannel datagramChannel;
    protected final ILogger logger;
    private final ChannelErrorHandler errorHandler;

    AbstractHandler(SpinningUdpChannel channel, ILogger logger, ChannelErrorHandler errorHandler) {
        this.channel = channel;
        this.datagramChannel = channel.getDatagramChannel();
        this.errorHandler = errorHandler;
        this.logger = logger;
    }

    public Channel getChannel() {
        return channel;
    }

    public void onFailure(Throwable e) {
        errorHandler.onError(channel, e);
    }
}
