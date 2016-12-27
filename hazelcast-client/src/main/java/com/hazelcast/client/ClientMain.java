package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

/**
 * Created by alarmnummer on 12/27/16.
 */
public class ClientMain {

    public static void main(String[] args) {
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().getSocketOptions().setBufferSize(2048);
        config.getNetworkConfig().addAddress("10.212.1.114");

        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
        IMap<String, byte[]> map = client.getMap("test");
        System.out.println("Map Size:" + map.size());

        for (int k = 0; k < 20; k++) {
            benchmark(true, map);
        }

        benchmark(false, map);
    }

    private static void benchmark(boolean warmup, IMap<String, byte[]> map) {
        int[] sizes = {1, 10, 100, 200};
        for (int i = 0; i < 3; i++) {
            byte[] d = new byte[sizes[i] * 1024 * 1024];
            long startTime = System.currentTimeMillis();
            map.set("mykey", d);
            long putTime = System.currentTimeMillis() - startTime;
            startTime = System.currentTimeMillis();
            d = map.get("mykey");
            long getTime = System.currentTimeMillis() - startTime;
            if (!warmup) {
                toGigabitPerSecond(d, putTime);
                System.out.println(sizes[i] + "MB");
                System.out.println("\tput-time:" + putTime + "ms, speed:" + toGigabitPerSecond(d, putTime) + " Gbit/second");
                System.out.println("\tget time: " + getTime + "ms, speed:" + toGigabitPerSecond(d, getTime) + " Gbit/second");
            }
        }
        map.clear();
    }

    private static double toGigabitPerSecond(byte[] d, long time) {
        double bytesPerSecond = (d.length * 1000d / time);
        return (8 * bytesPerSecond) / (1024 * 1024 * 1024);
    }
}
