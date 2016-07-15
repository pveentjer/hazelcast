package com.hazelcast.client;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;

/**
 * Created by alarmnummer on 7/15/16.
 */
public class Main {

    public static void main(String[] args) throws InterruptedException {
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        HazelcastInstance client = HazelcastClient.newHazelcastClient();

        IAtomicLong l = client.getAtomicLong("foo");
        for(int k=0;k<1000000000;k++){
            l.get();
            Thread.sleep(100);
            System.out.println("At:"+k);
        }
    }
}
