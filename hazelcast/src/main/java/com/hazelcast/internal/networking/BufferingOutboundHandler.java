package com.hazelcast.internal.networking;

import com.hazelcast.nio.OutboundFrame;


/**
 * The idea behind the {@link BufferingOutboundHandler} is that it is possible to add interleaving since it has access
 * to the pending packets.
 */
public interface BufferingOutboundHandler extends ChannelOutboundHandler {

    void offer(boolean urgent, OutboundFrame frame);

    boolean isEmpty();

    int pending();

    void clear();
}
