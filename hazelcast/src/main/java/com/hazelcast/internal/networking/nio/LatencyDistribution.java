package com.hazelcast.internal.networking.nio;

import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

public final class LatencyDistribution {
    public static final int LOW_WATERMARK_MICROS = 8;
    public static final int LATENCY_BUCKET_COUNT = 32;

    private static final AtomicLongFieldUpdater<LatencyDistribution> COUNT
            = newUpdater(LatencyDistribution.class, "count");
    private static final AtomicLongFieldUpdater<LatencyDistribution> TOTAL_MICROS
            = newUpdater(LatencyDistribution.class, "totalMicros");
    private static final AtomicLongFieldUpdater<LatencyDistribution> MAX_MICROS
            = newUpdater(LatencyDistribution.class, "maxMicros");

    public final AtomicLongArray distribution = new AtomicLongArray(LATENCY_BUCKET_COUNT);

    private volatile long count;
    private volatile long maxMicros;
    private volatile long totalMicros;

    public long count() {
        return count;
    }

    public long maxMicros() {
        return maxMicros;
    }

    public long totalMicros() {
        return totalMicros;
    }

    public long averageMicros() {
        return count == 0 ? 0 : totalMicros / count;
    }

    private static final String[] LATENCY_KEYS;

    static {
        LATENCY_KEYS = new String[LATENCY_BUCKET_COUNT];
        long maxDurationForBucket = LOW_WATERMARK_MICROS;
        long p = 0;
        for (int k = 0; k < LATENCY_KEYS.length; k++) {
            LATENCY_KEYS[k] = p + ".." + (maxDurationForBucket - 1) + "us";
            p = maxDurationForBucket;
            maxDurationForBucket *= 2;
        }
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        for (int k = 0; k < distribution.length(); k++) {
            long value = distribution.get(k);
            if (value > 0) {
                sb.append(LATENCY_KEYS[k] + "=" + value);
                sb.append("\n");
            }

        }
        return sb.toString();
    }

    public void record(long durationNanos) {
        long durationMicros = NANOSECONDS.toMicros(durationNanos);

        COUNT.addAndGet(this, 1);
        TOTAL_MICROS.addAndGet(this, durationMicros);

        for (; ; ) {
            long currentMax = maxMicros;
            if (durationMicros <= currentMax) {
                break;
            }

            if (MAX_MICROS.compareAndSet(this, currentMax, durationMicros)) {
                break;
            }
        }

        // this can be done cheaper
        // https://stackoverflow.com/questions/3305059/how-do-you-calculate-log-base-2-in-java-for-integers
        int bucketIndex = 0;
        long maxDurationForBucket = LOW_WATERMARK_MICROS;
        for (int k = 0; k < distribution.length() - 1; k++) {
            if (durationMicros >= maxDurationForBucket) {
                bucketIndex++;
                maxDurationForBucket *= 2;
            } else {
                break;
            }
        }

        distribution.incrementAndGet(bucketIndex);
    }
}

