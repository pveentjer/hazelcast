package com.hazelcast.spi.impl.operationexecutor.impl;

import com.hazelcast.spi.Operation;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class OperationExecutorImpl_IsRunAllowedTest extends OperationExecutorImpl_AbstractTest {

    @Test(expected = NullPointerException.class)
    public void test_whenNullOperation() {
        initExecutor();

        executor.isRunAllowed(null);
    }

    // ============= generic operations ==============================

    @Test
    public void test_whenGenericOperation_andCallingFromUserThread() {
        initExecutor();

        DummyGenericOperation genericOperation = new DummyGenericOperation();

        boolean result = executor.isRunAllowed(genericOperation);

        assertTrue(result);
    }

    @Test
    public void test_whenGenericOperation_andCallingFromPartitionOperationThread() {
        initExecutor();

        final DummyGenericOperation genericOperation = new DummyGenericOperation();

        PartitionSpecificCallable task = new PartitionSpecificCallable() {
            @Override
            public Object call() {
                return executor.isRunAllowed(genericOperation);
            }
        };

        executor.execute(task);

        assertEqualsEventually(task, TRUE);
    }

    @Test
    public void test_whenGenericOperation_andCallingFromGenericOperationThread() {
        initExecutor();

        final DummyGenericOperation genericOperation = new DummyGenericOperation();

        PartitionSpecificCallable task = new PartitionSpecificCallable(0) {
            @Override
            public Object call() {
                return executor.isRunAllowed(genericOperation);
            }
        };

        executor.execute(task);

        assertEqualsEventually(task, TRUE);
    }

    @Test
    public void test_whenGenericOperation_andCallingFromOperationHostileThread() {
        initExecutor();

        final DummyGenericOperation genericOperation = new DummyGenericOperation();

        FutureTask<Boolean> futureTask = new FutureTask<Boolean>(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return executor.isRunAllowed(genericOperation);
            }
        });

        DummyOperationHostileThread thread = new DummyOperationHostileThread(futureTask);
        thread.start();

        assertEqualsEventually(futureTask, FALSE);
    }

    // ===================== partition specific operations ========================

    @Test
    public void test_whenPartitionOperation_andCallingFromUserThread() {
        initExecutor();

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation(0);

        boolean result = executor.isRunAllowed(partitionOperation);

        assertFalse(result);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromGenericThread() {
        fail();
        initExecutor();

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation(0);

        PartitionSpecificCallable task = new PartitionSpecificCallable(Operation.GENERIC_PARTITION_ID) {
            @Override
            public Object call() {
                return executor.isRunAllowed(partitionOperation);
            }
        };

        executor.execute(task);

        assertEqualsEventually(task, FALSE);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromPartitionOperationThread_andCorrectPartition() {
        initExecutor();

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation(0);

        PartitionSpecificCallable task = new PartitionSpecificCallable(partitionOperation.getPartitionId()) {
            @Override
            public Object call() {
                return executor.isRunAllowed(partitionOperation);
            }
        };

        executor.execute(task);

        assertEqualsEventually(task, TRUE);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromPartitionOperationThread_andWrongPartition() {
        initExecutor();

        final DummyPartitionOperation partitionOperation = new DummyPartitionOperation(0);

        int wrongPartition = partitionOperation.getPartitionId() + 1;
        PartitionSpecificCallable task = new PartitionSpecificCallable(wrongPartition) {
            @Override
            public Object call() {
                return executor.isRunAllowed(partitionOperation);
            }
        };

        executor.execute(task);

        assertEqualsEventually(task, FALSE);
    }

    @Test
    public void test_whenPartitionOperation_andCallingFromOperationHostileThread() {
        initExecutor();

        final DummyPartitionOperation operation = new DummyPartitionOperation();

        FutureTask<Boolean> futureTask = new FutureTask<Boolean>(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return executor.isRunAllowed(operation);
            }
        });

        DummyOperationHostileThread thread = new DummyOperationHostileThread(futureTask);
        thread.start();

        assertEqualsEventually(futureTask, FALSE);
    }
}
