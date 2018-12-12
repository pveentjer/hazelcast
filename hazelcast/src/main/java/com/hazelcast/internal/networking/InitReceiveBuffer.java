package com.hazelcast.internal.networking;

public class InitReceiveBuffer extends InboundHandler {

    @Override
    public void handlerAdded() {
        initSrcBuffer();
        dst(src);
    }

    @Override
    public HandlerStatus onRead() {
        channel.inboundPipeline().remove(this);
        return HandlerStatus.CLEAN;
    }
}
