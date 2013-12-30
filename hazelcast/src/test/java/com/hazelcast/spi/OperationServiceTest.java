package com.hazelcast.spi;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OperationServiceTest extends HazelcastTestSupport {

    @Test
    public void invokeOnLocalTargetWithCallback() throws Exception {
        HazelcastInstance local = createHazelcastInstanceFactory(1).newHazelcastInstance();

        warmUpPartitions(local);

        DummyOperation op = new DummyOperation();
        OperationService operationService = getNode(local).nodeEngine.getOperationService();
        Address target = getAddress(local);
        final CountDownLatch latch = new CountDownLatch(1);
        Callback callback = new Callback() {
            @Override
            public void notify(Object object) {
                latch.countDown();
            }
        };
        Future f = operationService.createInvocationBuilder(null, op, target).setCallback(callback).invoke();
        assertEquals(new Integer(10), f.get());
        assertTrue("the callback failed to be called", latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void invokeOnRemoteTargetWithCallback() throws Exception {
        HazelcastInstance[] instances = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = instances[0];
        HazelcastInstance remote = instances[1];

        warmUpPartitions(instances);

        DummyOperation op = new DummyOperation();
        OperationService operationService = getNode(local).nodeEngine.getOperationService();
        Address target = getAddress(remote);
        final CountDownLatch latch = new CountDownLatch(1);
        Callback callback = new Callback() {
            @Override
            public void notify(Object object) {
                latch.countDown();
            }
        };
        Future f = operationService.createInvocationBuilder(null, op, target).setCallback(callback).invoke();
        assertEquals(new Integer(10), f.get());
        assertTrue("the callback failed to be called", latch.await(10, TimeUnit.SECONDS));
    }

     @Test
    public void invokeOnRemoteTarget() throws Exception {
        HazelcastInstance[] instances = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = instances[0];
        HazelcastInstance remote = instances[1];

         warmUpPartitions(local,remote);

        DummyOperation op = new DummyOperation();
        OperationService operationService = getNode(local).nodeEngine.getOperationService();
        Future f = operationService.invokeOnTarget(null, op, getAddress(remote));
        assertEquals(new Integer(10), f.get());
    }

    @Test
    public void invokeOnLocalTarget() throws Exception {
        HazelcastInstance local = createHazelcastInstanceFactory(1).newHazelcastInstance();

        warmUpPartitions(local);

        DummyOperation op = new DummyOperation();
        OperationService operationService = getNode(local).nodeEngine.getOperationService();
        Future f = operationService.invokeOnTarget(null, op, getAddress(local));
        assertEquals(new Integer(10), f.get());
    }

    @Test
    public void invokeOnLocalPartition() throws Exception {
        HazelcastInstance local = createHazelcastInstanceFactory(1).newHazelcastInstance();

        warmUpPartitions(local);

        DummyOperation op = new DummyOperation();
        OperationService operationService = getNode(local).nodeEngine.getOperationService();
        Future f = operationService.invokeOnPartition(null, op, 1);
        assertEquals(new Integer(10), f.get());
    }

    @Test
    public void invokeOnRemotePartition() throws Exception {
        HazelcastInstance[] instances = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = instances[0];
        HazelcastInstance remote = instances[1];

        warmUpPartitions(instances);

        DummyOperation op = new DummyOperation();
        OperationService operationService = getNode(local).nodeEngine.getOperationService();
        int partitionId = findAnyPartitionId(remote);
        Future f = operationService.invokeOnPartition(null, op, partitionId);
        assertEquals(new Integer(10), f.get());
    }

    private int findAnyPartitionId(HazelcastInstance hz) {
        for (Partition p : hz.getPartitionService().getPartitions()) {
            Member owner = p.getOwner();
            if (owner.localMember()) {
                return p.getPartitionId();
            }
        }

        throw new RuntimeException("No owned partition found");
    }

    private Address getAddress(HazelcastInstance hz) {
        return new Address(hz.getCluster().getLocalMember().getSocketAddress());
    }

    public static class DummyOperation extends AbstractOperation {
        @Override
        public void run() throws Exception {
            System.out.println("================================================");
            System.out.println("DummyOperation has run");
            System.out.println("================================================");
        }

        @Override
        public boolean returnsResponse() {
            return true;
        }

        @Override
        public Object getResponse() {
            return new Integer(10);
        }
    }
}
