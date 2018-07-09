package com.hazelcast.internal.metrics;

public interface ProbeRoot {

    void scanMetrics(MetricsProvideContext context);
}
