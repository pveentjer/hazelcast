package com.hazelcast.client;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.GroupProperty;

/**
 * Created by alarmnummer on 12/27/16.
 */
public class ServerMain {

    public static void main(String[] args){
        Config config = new Config();
        config.setProperty(GroupProperty.SOCKET_RECEIVE_BUFFER_SIZE.getName(),"2048");
        config.setProperty(GroupProperty.SOCKET_SEND_BUFFER_SIZE.getName(),"2048");
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
    }
}
