package com.hazelcast.internal.metrics.impl;

import com.hazelcast.internal.metrics.MetricsProvideContext;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.ProbeRoot;
import com.hazelcast.internal.metrics.renderers.ProbeRenderer;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MetricRootTest extends HazelcastTestSupport {

    private ILogger logger;
    private MetricsRegistryImpl metricsRegistry;

    @Before
    public void before(){
        logger = Logger.getLogger(MetricsRegistryImpl.class);
        metricsRegistry = new MetricsRegistryImpl(logger, ProbeLevel.DEBUG);
    }

@Test
    public void test(){
        metricsRegistry.addRoot(new MyProbeRoot(), "operations");

        metricsRegistry.render(new ProbeRenderer() {
            @Override
            public void renderLong(String name, long value) {
                System.out.println("renderLong "+name+"="+value);
            }

            @Override
            public void renderDouble(String name, double value) {

            }

            @Override
            public void renderException(String name, Exception e) {

            }

            @Override
            public void renderNoValue(String name) {

            }
        });
    }

    private static class MyProbeRoot implements ProbeRoot {
        @Probe
        public long field=20;

        @Override
        public void scanMetrics(MetricsProvideContext context) {

        }
    }
}
