package com.hazelcast.internal.networking;

import com.hazelcast.nio.OutboundFrame;


/**
 * A {@link ChannelOutboundHandler} that buffers messages to be written. The {@link BufferingOutboundHandler} needs
 * to be the first in an outbound pipeline.
 *
 * The idea behind the {@link BufferingOutboundHandler} is that it is possible to add interleaving since it has access
 * to the pending packets.
 */
public interface BufferingOutboundHandler extends ChannelOutboundHandler {

    /**
     * Offers a frame to be written.
     *
     * todo: there should not be a reliance on OutboundFrame.
     * todo: the offer returns no value.. So it isn't an offer like a queue.offer.
     *
     * @param urgent if the frame is urgent.
     * @param frame
     */
    void offer(boolean urgent, OutboundFrame frame);

    /**
     * checks if there are any pending frames.
     */
    boolean isEmpty();

    /**
     * @return the number of pending frames.
     */
    int pending();

    /**
     * Clears any pending frames.
     */
    void clear();
}
