package com.hazelcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * Created by alarmnummer on 2/8/17.
 */
public class Main {

    public static void main(String[] args){
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();
        System.out.println("done");
    }
}
