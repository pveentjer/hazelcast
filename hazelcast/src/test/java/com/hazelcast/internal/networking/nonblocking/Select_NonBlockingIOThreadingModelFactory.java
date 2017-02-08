package com.hazelcast.internal.networking.nonblocking;

import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.tcp.IOThreadingModelFactory;
import com.hazelcast.nio.tcp.MockIOService;

public class Select_NonBlockingIOThreadingModelFactory implements IOThreadingModelFactory {

    @Override
    public NonBlockingIOThreadingModel create(MockIOService ioService, MetricsRegistry metricsRegistry) {
        LoggingService loggingService = ioService.loggingService;
        NonBlockingIOThreadingModel threadingModel = new NonBlockingIOThreadingModel(
                loggingService,
                metricsRegistry,
                ioService.hazelcastThreadGroup,
                ioService.getIoOutOfMemoryHandler(), ioService.getInputThreadCount(),
                ioService.getOutputThreadCount(),
                ioService.getBalancerIntervalSeconds(),
                null,null,null,null
        );
        threadingModel.setSelectorMode(SelectorMode.SELECT);
        return threadingModel;
    }
}
