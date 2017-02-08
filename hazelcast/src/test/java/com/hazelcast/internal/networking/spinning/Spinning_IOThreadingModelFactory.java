package com.hazelcast.internal.networking.spinning;

import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.tcp.IOThreadingModelFactory;
import com.hazelcast.nio.tcp.MockIOService;

public class Spinning_IOThreadingModelFactory implements IOThreadingModelFactory {

    @Override
    public SpinningIOThreadingModel create(MockIOService ioService, MetricsRegistry metricsRegistry) {
        LoggingService loggingService = ioService.loggingService;
        return new SpinningIOThreadingModel(
                loggingService,
                ioService.hazelcastThreadGroup,
                ioService.getIoOutOfMemoryHandler(),
                null,null,null,null);
    }
}
