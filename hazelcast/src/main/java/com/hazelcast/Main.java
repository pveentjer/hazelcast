package com.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.GroupProperty;

public class Main {

    public static void main(String[] args){
        System.setProperty("hazelcast.io.eventloopgroup","aeron");
        Config config = new Config()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(),"5");
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config);

        for(int k=0;k<100000000;k++) {
            hz1.getMap("map").put(k % 100, "");
            if(k%10000 == 0){
                System.out.println("at :"+k);
            }
        }

        System.out.println("----------------------------------------");
        System.out.println(hz1.getMap("map").size());
        System.out.println(hz2.getMap("map").size());
    }
}
