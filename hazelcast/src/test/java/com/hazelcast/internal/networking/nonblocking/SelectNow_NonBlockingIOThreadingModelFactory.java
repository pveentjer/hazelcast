package com.hazelcast.internal.networking.nonblocking;

import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.logging.LoggingServiceImpl;
import com.hazelcast.nio.tcp.IOThreadingModelFactory;
import com.hazelcast.nio.tcp.MockIOService;

public class SelectNow_NonBlockingIOThreadingModelFactory implements IOThreadingModelFactory {

    @Override
    public NonBlockingIOThreadingModel create(MockIOService ioService, MetricsRegistry metricsRegistry) {

        LoggingServiceImpl loggingService = ioService.loggingService;
        NonBlockingIOThreadingModel threadingModel = new NonBlockingIOThreadingModel(
                loggingService,
                metricsRegistry,
                ioService.hazelcastThreadGroup,
                ioService.getIoOutOfMemoryHandler(), ioService.getInputThreadCount(),
                ioService.getOutputThreadCount(),
                ioService.getBalancerIntervalSeconds(),
                null,null,null,null
        );
        threadingModel.setSelectorMode(SelectorMode.SELECT_NOW);
        return threadingModel;
    }
}
