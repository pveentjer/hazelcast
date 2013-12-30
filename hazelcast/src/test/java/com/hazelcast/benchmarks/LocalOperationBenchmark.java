package com.hazelcast.benchmarks;

import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import com.hazelcast.concurrent.atomiclong.AtomicLongBaseOperation;
import com.hazelcast.concurrent.atomiclong.AtomicLongService;
import com.hazelcast.concurrent.atomiclong.proxy.AtomicLongProxy;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.util.concurrent.Future;

@AxisRange(min = 0, max = 1)
@BenchmarkMethodChart(filePrefix = "benchmark-localoperation")
@BenchmarkHistoryChart(filePrefix = "benchmark-localoperation-history", labelWith = LabelType.CUSTOM_KEY, maxRuns = 20)
public class LocalOperationBenchmark extends HazelcastTestSupport {

    @Rule
    public TestRule benchmarkRun = new BenchmarkRule();

    private static HazelcastInstance hazelcastInstance;

    @BeforeClass
    public static void beforeClass() throws Exception {
        hazelcastInstance = Hazelcast.newHazelcastInstance();
        warmUpPartitions(hazelcastInstance);
    }

    @AfterClass
    public static void afterClass() {
        Hazelcast.shutdownAll();
    }

    //this test
    @Test
    public void noResponse() throws Exception {
        long startMs = System.currentTimeMillis();
        int iterations = 50 * 1000 * 1000;

        OperationService opService = getNode(hazelcastInstance).nodeEngine.getOperationService();
        Future[] futures = new Future[1];
        int futureIndex = 0;
        for (int k = 0; k < iterations; k++) {

            NoResponseOperation no = new NoResponseOperation();
            no.setPartitionId(1);
            Future f = opService.invokeOnPartition(AtomicLongService.SERVICE_NAME, no, 1);
            futures[futureIndex] = f;
            futureIndex++;
            if (futureIndex >= futures.length) {
                for (Future fu : futures) {
                    fu.get();
                }
                futureIndex = 0;
            }

            if (k % 2000000 == 0) {
                System.out.println("at " + k);
            }
        }
        long durationMs = System.currentTimeMillis() - startMs;
        double performance = (iterations * 1000d) / durationMs;
        System.out.println("Performance: " + String.format("%1$,.2f", performance));
    }

    public class NoResponseOperation extends AbstractOperation implements PartitionAwareOperation {

        public NoResponseOperation() {
        }

        @Override
        public void run() throws Exception {
        }

        @Override
        public boolean returnsResponse() {
            return false;
        }
    }

    //this test
    @Test
    public void withResponse() throws Exception {
        long startMs = System.currentTimeMillis();
        int iterations = 50 * 1000 * 1000;

        OperationService opService = getNode(hazelcastInstance).nodeEngine.getOperationService();
        Future[] futures = new Future[1];
        int futureIndex = 0;
        for (int k = 0; k < iterations; k++) {

            ResponseOperation no = new ResponseOperation();
            no.setPartitionId(1);
            Future f = opService.invokeOnPartition(AtomicLongService.SERVICE_NAME, no, 1);
            futures[futureIndex] = f;
            futureIndex++;
            if (futureIndex >= futures.length) {
                for (Future fu : futures) {
                    fu.get();
                }
                futureIndex = 0;
            }

            if (k % 2000000 == 0) {
                System.out.println("at " + k);
            }
        }
        long durationMs = System.currentTimeMillis() - startMs;
        double performance = (iterations * 1000d) / durationMs;
        System.out.println("Performance: " + String.format("%1$,.2f", performance));
    }

    public class ResponseOperation extends AbstractOperation implements PartitionAwareOperation  {

        public ResponseOperation() {
        }

        @Override
        public void run() throws Exception {
        }

        @Override
        public boolean returnsResponse() {
            return true;
        }
    }
}
