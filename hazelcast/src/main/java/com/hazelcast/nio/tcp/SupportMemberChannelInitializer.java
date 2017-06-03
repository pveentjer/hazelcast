package com.hazelcast.nio.tcp;

import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelInboundHandler;
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.internal.networking.ChannelOutboundHandler;
import com.hazelcast.internal.networking.InitResult;

import java.io.IOException;

/**
 * Created by alarmnummer on 6/3/17.
 */
public class SupportMemberChannelInitializer implements ChannelInitializer {
    @Override
    public InitResult<ChannelInboundHandler> initInbound(Channel channel) throws IOException {
        return null;
    }

    @Override
    public InitResult<ChannelOutboundHandler> initOutbound(Channel channel) {
        return null;
    }
}
