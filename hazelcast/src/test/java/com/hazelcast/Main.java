package com.hazelcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IFunction;
import com.hazelcast.core.IMap;


/**
 * reason of stalling:
 * UdpSpinningChannelWriter{
 * channel=SpinningUdpChannel{/10.8.0.10:5701->/10.8.0.10:33723},
 * writeBuffer=Buffer{id=2, writeIndex=43, readIndex=0},
 * readBuffer=Buffer{id=1, writeIndex=0, readIndex=0},
 * bytesWritten=Counter{value=65509},
 * dirtyBuffer=null}
 *
 * The writers write to a different buffer, than the io thread is reading from. So the write that is done, is not visible and
 * then we are in a stuck situation.
 *
 * The question is how this could have happened.
 */
public class Main {

    public static void main(String[] args) {
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance();
        System.out.println("hz1 started");
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();
        System.out.println("hz2 started");

        IMap map1 = hz1.getMap("foo");
        for (int k = 0; k < 10000; k++) {
            map1.put(k, k);
            if (k % 100 == 0) {
                System.out.println("insert at:" + k);
            }
        }

        System.out.println("Starting----------------------------------------");

        IMap map2 = hz2.getMap("foo");
        for (int k = 0; k < 10000; k++) {
            System.out.println("at " + k + " value=" + map2.get(k));
        }

        System.out.println("Done---------------------------------------------");

    }

    static class MyFunction implements IFunction<Long, Long> {
        @Override
        public Long apply(Long inpunt) {
            System.out.println(Thread.currentThread());
            return 10l;
        }
    }
}
