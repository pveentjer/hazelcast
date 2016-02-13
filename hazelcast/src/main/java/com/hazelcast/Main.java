package com.hazelcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by alarmnummer on 2/13/16.
 */
public class Main {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();

        List<Future> futures = new LinkedList<Future>();
        for(int k=0;k<1000000;k++){
            Future f = hz1.getMap("foo").getAsync(k);
            futures.add(f);
            if(k%10000==0){
                System.out.println("at: "+k);
            }
        }

        for(Future f: futures){
            f.get();
        }

        System.out.println("done");
        System.exit(0);
    }
}
