package com.hazelcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class Main {

    public static void main(String[] args){
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();

        IMap map = hz1.getMap("foo");
        for(int k=0;k<100;k++){
            Object result = map.get(k);
            System.out.println("at: "+k);
        }

        System.out.println("done");
    }
}
