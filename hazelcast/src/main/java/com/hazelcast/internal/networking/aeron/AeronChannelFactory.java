package com.hazelcast.internal.networking.aeron;

import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelFactory;

import java.nio.channels.SocketChannel;

public class AeronChannelFactory implements ChannelFactory {
    @Override
    public Channel create(SocketChannel channel, boolean clientMode, boolean directBuffer) throws Exception {
        return new AeronChannel(channel,clientMode);
    }
}
