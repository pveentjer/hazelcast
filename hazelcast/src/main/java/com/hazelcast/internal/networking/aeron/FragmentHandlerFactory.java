package com.hazelcast.internal.networking.aeron;

import com.hazelcast.internal.networking.Channel;
import io.aeron.logbuffer.FragmentHandler;

public interface FragmentHandlerFactory {

    FragmentHandler create(Channel channel);
}
