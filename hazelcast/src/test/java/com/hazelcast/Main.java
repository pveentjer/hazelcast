package com.hazelcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

import java.nio.channels.SelectionKey;

/**
 * Created by alarmnummer on 5/26/17.
 */
public class Main {

    public static void main(String[] args){
        System.out.println("OP_WRITE:"+ SelectionKey.OP_READ);
        System.out.println("OP_WRITE:"+ SelectionKey.OP_WRITE);


        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance();
        System.out.println("hz1 started");
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();
        System.out.println("hz2 started");
        HazelcastInstance hz3 = Hazelcast.newHazelcastInstance();
        System.out.println("hz2 started");

        IMap map1 = hz1.getMap("foo");
        for(int k=0;k<10;k++){
            map1.put(k,k);
        }

        IMap map2 = hz2.getMap("foo");
        for(int k=0;k<10;k++){
            System.out.println(map2.get(k));
        }


        System.out.println("done");
    }
}
