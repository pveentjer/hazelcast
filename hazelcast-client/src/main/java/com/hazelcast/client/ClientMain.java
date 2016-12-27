package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

import java.util.concurrent.ConcurrentMap;

/**
 * Created by alarmnummer on 12/27/16.
 */
public class ClientMain {

    public static void main(String[] args){
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().getSocketOptions().setBufferSize(2048);
        config.getNetworkConfig().addAddress("10.212.1.114");

        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
        IMap<String, byte[]> map = client.getMap("test");
        System.out.println("Map Size:" + map.size());

        int [] sizes = {1, 10,100,200};
        for(int i =0; i < 3; i ++)
        {
            byte [] d = new byte[sizes[i] * 1024*1024];
            long startTime = System.currentTimeMillis();
            map.set("mykey", d);
            long putTime = System.currentTimeMillis() - startTime;
            startTime = System.currentTimeMillis();
            d = map.get("mykey");
            long getTime = System.currentTimeMillis() - startTime;
            System.out.println(sizes[i] + "MB put:" + putTime + "ms get time: " + getTime + "ms");
        }
    }
}
