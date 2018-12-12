package com.hazelcast.internal.networking;

public class InitDstBuffer extends OutboundHandler {

    @Override
    public void handlerAdded() {
        initDstBuffer();
    }

    @Override
    public HandlerStatus onWrite() {
        channel.outboundPipeline().remove(this);
        return HandlerStatus.CLEAN;
    }
}

