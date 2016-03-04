package com.hazelcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * Created by alarmnummer on 3/4/16.
 */
public class Main {

    public static void main(String[] args){
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();

        for(int k=0;k<100;k++){
            hz1.getMap("foo").put(k,k);
        }

        for(int k=0;k<100;k++){
            System.out.println(hz1.getMap("foo").get(k));
        }
    }
}
