package com.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * Created by alarmnummer on 6/29/16.
 */
public class Main {

    public static void main(String[] args){

         HazelcastInstance hz = Hazelcast.newHazelcastInstance();
    }
}
