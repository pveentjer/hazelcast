package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class Main {


    public static void main(String[] args){
        HazelcastInstance server = Hazelcast.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress("127.0.0.1:5701");
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        System.out.println("Connected");
        IMap<Object, Object> map = client.getMap("Foo");
        map.put("key","value");
        System.out.println(map.get("key"));
        System.exit(0);
    }
}
