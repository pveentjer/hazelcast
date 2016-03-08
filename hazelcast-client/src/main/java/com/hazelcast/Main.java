package com.hazelcast;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * Created by alarmnummer on 3/8/16.
 */
public class Main {
    public static void main(String[] args){
        HazelcastInstance server  = Hazelcast.newHazelcastInstance();
        HazelcastInstance client = HazelcastClient.newHazelcastClient();

        for(int k=0;k<100;k++){
            System.out.println(k+"="+client.getMap("foo").get(k));
        }

    }
}
