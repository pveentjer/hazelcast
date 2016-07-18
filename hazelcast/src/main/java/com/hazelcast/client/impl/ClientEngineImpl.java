/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.client.impl;

import com.hazelcast.cache.impl.JCacheDetector;
import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.ClientEndpointManager;
import com.hazelcast.client.ClientEngine;
import com.hazelcast.client.ClientEvent;
import com.hazelcast.client.ClientEventType;
import com.hazelcast.client.impl.operations.ClientDisconnectionOperation;
import com.hazelcast.client.impl.operations.GetConnectedClientsOperation;
import com.hazelcast.client.impl.operations.PostJoinClientOperation;
import com.hazelcast.client.impl.protocol.ClientExceptionFactory;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.MessageTaskFactory;
import com.hazelcast.client.impl.protocol.task.GetPartitionsMessageTask;
import com.hazelcast.client.impl.protocol.task.MessageTask;
import com.hazelcast.client.impl.protocol.task.PingMessageTask;
import com.hazelcast.config.Config;
import com.hazelcast.core.Client;
import com.hazelcast.core.ClientListener;
import com.hazelcast.core.ClientType;
import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.spi.CoreService;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MemberAttributeServiceEvent;
import com.hazelcast.spi.MembershipAwareService;
import com.hazelcast.spi.MembershipServiceEvent;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PostJoinAwareService;
import com.hazelcast.spi.ProxyService;
import com.hazelcast.spi.UrgentSystemOperation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.transaction.TransactionManagerService;
import com.hazelcast.util.executor.ExecutorType;

import javax.security.auth.login.LoginException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.spi.ExecutionService.CLIENT_EXECUTOR;
import static com.hazelcast.spi.impl.OperationResponseHandlerFactory.createEmptyResponseHandler;
import static com.hazelcast.spi.properties.GroupProperty.CLIENT_ENGINE_THREAD_COUNT;

/**
 * Class that requests, listeners from client handled in node side.
 */
public class ClientEngineImpl implements ClientEngine, CoreService, PostJoinAwareService,
        ManagedService, MembershipAwareService, EventPublishingService<ClientEvent, ClientListener> {

    public final static boolean PRIORITY_SCHEDULING = Boolean.parseBoolean(System.getProperty("client.priorityScheduling", "true"));
    public final static boolean EXECUTOR_TRACKING = Boolean.parseBoolean(System.getProperty("client.executor.tracking", "true"));

    /**
     * Service name to be used in requests.
     */
    public static final String SERVICE_NAME = "hz:core:clientEngine";

    private static final int ENDPOINT_REMOVE_DELAY_SECONDS = 10;
    private static final int EXECUTOR_QUEUE_CAPACITY_PER_CORE = 100000;
    private static final int THREADS_PER_CORE = 20;

    private final ConcurrentHashMap<Class, MessageTaskStatistics> taskStatistics = new ConcurrentHashMap<Class, MessageTaskStatistics>();


    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final Executor executor;

    private final SerializationService serializationService;
    // client uuid -> member uuid
    private final ConcurrentMap<String, String> ownershipMappings = new ConcurrentHashMap<String, String>();

    private final ClientEndpointManagerImpl endpointManager;
    private final ILogger logger;
    private final ConnectionListener connectionListener = new ConnectionListenerImpl();

    private final MessageTaskFactory messageTaskFactory;
    private final ClientExceptionFactory clientExceptionFactory;

    public ClientEngineImpl(Node node) {
        this.logger = node.getLogger(ClientEngine.class);
        this.node = node;
        this.serializationService = node.getSerializationService();
        this.nodeEngine = node.nodeEngine;
        this.endpointManager = new ClientEndpointManagerImpl(this, nodeEngine);
        this.executor = newExecutor();
        this.messageTaskFactory = new CompositeMessageTaskFactory(this.nodeEngine);
        this.clientExceptionFactory = initClientExceptionFactory();

        ClientHeartbeatMonitor heartBeatMonitor = new ClientHeartbeatMonitor(
                endpointManager, this, nodeEngine.getExecutionService(), node.getProperties());
        heartBeatMonitor.start();

        new ClientExecutorDelayMonitorThread().start();

        logger.info("Priority scheduling enabled:" + PRIORITY_SCHEDULING);
        logger.info("Tracking enabled:" + EXECUTOR_TRACKING);
    }

    private ClientExceptionFactory initClientExceptionFactory() {
        boolean jcacheAvailable = JCacheDetector.isJcacheAvailable(nodeEngine.getConfigClassLoader());
        return new ClientExceptionFactory(jcacheAvailable);
    }

    private Executor newExecutor() {
        final ExecutionService executionService = nodeEngine.getExecutionService();
        int coreSize = Runtime.getRuntime().availableProcessors();

        int threadCount = node.getProperties().getInteger(CLIENT_ENGINE_THREAD_COUNT);
        if (threadCount <= 0) {
            threadCount = coreSize * THREADS_PER_CORE;
        }

        return executionService.register(CLIENT_EXECUTOR,
                threadCount, coreSize * EXECUTOR_QUEUE_CAPACITY_PER_CORE,
                ExecutorType.CONCRETE);
    }

    //needed for testing purposes
    public ConnectionListener getConnectionListener() {
        return connectionListener;
    }

    @Override
    public SerializationService getSerializationService() {
        return serializationService;
    }

    @Override
    public int getClientEndpointCount() {
        return endpointManager.size();
    }

    public void handleClientMessage(ClientMessage clientMessage, Connection connection) {
        InternalOperationService operationService = nodeEngine.getOperationService();
        int partitionId = clientMessage.getPartitionId();
        final MessageTask messageTask = messageTaskFactory.create(clientMessage, connection);
        if (partitionId < 0) {
            if (isUrgent(messageTask) && PRIORITY_SCHEDULING) {
                operationService.execute(new PriorityRunnable(messageTask));
            } else {
                executor.execute(EXECUTOR_TRACKING ? new TrackingMessageTask(messageTask) : messageTask);
            }
        } else {
            operationService.execute(messageTask);
        }
    }

    private boolean isUrgent(MessageTask messageTask) {
        Class clazz = messageTask.getClass();
        return clazz == PingMessageTask.class
                || clazz == GetPartitionsMessageTask.class;
    }


    @Override
    public IPartitionService getPartitionService() {
        return nodeEngine.getPartitionService();
    }

    @Override
    public ClusterService getClusterService() {
        return nodeEngine.getClusterService();
    }

    @Override
    public EventService getEventService() {
        return nodeEngine.getEventService();
    }

    @Override
    public ProxyService getProxyService() {
        return nodeEngine.getProxyService();
    }

    @Override
    public Address getMasterAddress() {
        return node.getMasterAddress();
    }

    @Override
    public Address getThisAddress() {
        return node.getThisAddress();
    }

    @Override
    public MemberImpl getLocalMember() {
        return node.getLocalMember();
    }

    @Override
    public Config getConfig() {
        return node.getConfig();
    }

    @Override
    public ILogger getLogger(Class clazz) {
        return node.getLogger(clazz);
    }

    public ClientEndpointManager getEndpointManager() {
        return endpointManager;
    }

    public ClientExceptionFactory getClientExceptionFactory() {
        return clientExceptionFactory;
    }

    @Override
    public SecurityContext getSecurityContext() {
        return node.securityContext;
    }

    public void bind(final ClientEndpoint endpoint) {
        final Connection conn = endpoint.getConnection();
        if (conn instanceof TcpIpConnection) {
            Address address = new Address(conn.getRemoteSocketAddress());
            ((TcpIpConnection) conn).setEndPoint(address);
        }
        ClientEvent event = new ClientEvent(endpoint.getUuid(),
                ClientEventType.CONNECTED,
                endpoint.getSocketAddress(),
                endpoint.getClientType());
        sendClientEvent(event);
    }

    void sendClientEvent(ClientEvent event) {
        final EventService eventService = nodeEngine.getEventService();
        final Collection<EventRegistration> regs = eventService.getRegistrations(SERVICE_NAME, SERVICE_NAME);
        String uuid = event.getUuid();
        eventService.publishEvent(SERVICE_NAME, regs, event, uuid.hashCode());
    }

    @Override
    public void dispatchEvent(ClientEvent event, ClientListener listener) {
        if (event.getEventType() == ClientEventType.CONNECTED) {
            listener.clientConnected(event);
        } else {
            listener.clientDisconnected(event);
        }
    }

    @Override
    public void memberAdded(MembershipServiceEvent event) {
    }

    @Override
    public void memberRemoved(MembershipServiceEvent event) {
        if (event.getMember().localMember()) {
            return;
        }

        final String deadMemberUuid = event.getMember().getUuid();
        try {
            nodeEngine.getExecutionService().schedule(new DestroyEndpointTask(deadMemberUuid),
                    ENDPOINT_REMOVE_DELAY_SECONDS, TimeUnit.SECONDS);

        } catch (RejectedExecutionException e) {
            if (logger.isFinestEnabled()) {
                logger.finest(e);
            }
        }
    }

    @Override
    public void memberAttributeChanged(MemberAttributeServiceEvent event) {
    }

    public Collection<Client> getClients() {
        final HashSet<Client> clients = new HashSet<Client>();
        for (ClientEndpoint endpoint : endpointManager.getEndpoints()) {
            clients.add((Client) endpoint);
        }
        return clients;
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        node.getConnectionManager().addConnectionListener(connectionListener);
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
        for (ClientEndpoint ce : endpointManager.getEndpoints()) {
            ClientEndpointImpl endpoint = (ClientEndpointImpl) ce;
            try {
                endpoint.destroy();
            } catch (LoginException e) {
                logger.finest(e.getMessage());
            }
            try {
                final Connection conn = endpoint.getConnection();
                if (conn.isAlive()) {
                    conn.close("Shutdown of ClientEngine", null);
                }
            } catch (Exception e) {
                logger.finest(e);
            }
        }
        endpointManager.clear();
        ownershipMappings.clear();
    }

    public void addOwnershipMapping(String clientUuid, String ownerUuid) {
        ownershipMappings.put(clientUuid, ownerUuid);
    }

    public void removeOwnershipMapping(String clientUuid) {
        ownershipMappings.remove(clientUuid);
    }

    public TransactionManagerService getTransactionManagerService() {
        return node.nodeEngine.getTransactionManagerService();
    }

    private final class ConnectionListenerImpl implements ConnectionListener {

        @Override
        public void connectionAdded(Connection conn) {
            //no-op
            //unfortunately we can't do the endpoint creation here, because this event is only called when the
            //connection is bound, but we need to use the endpoint connection before that.
        }

        @Override
        public void connectionRemoved(Connection connection) {
            if (connection.isClient() && nodeEngine.isRunning()) {
                ClientEndpointImpl endpoint = (ClientEndpointImpl) endpointManager.getEndpoint(connection);
                if (endpoint == null) {
                    return;
                }

                if (!endpoint.isFirstConnection()) {
                    return;
                }

                String localMemberUuid = node.getLocalMember().getUuid();
                String ownerUuid = endpoint.getPrincipal().getOwnerUuid();
                if (localMemberUuid.equals(ownerUuid)) {
                    callDisconnectionOperation(endpoint);
                }
            }
        }

        private void callDisconnectionOperation(ClientEndpointImpl endpoint) {
            Collection<Member> memberList = nodeEngine.getClusterService().getMembers();
            OperationService operationService = nodeEngine.getOperationService();
            ClientDisconnectionOperation op = createClientDisconnectionOperation(endpoint.getUuid());
            operationService.runOperationOnCallingThread(op);

            for (Member member : memberList) {
                if (!member.localMember()) {
                    op = createClientDisconnectionOperation(endpoint.getUuid());
                    operationService.send(op, member.getAddress());
                }
            }
        }
    }

    private ClientDisconnectionOperation createClientDisconnectionOperation(String clientUuid) {
        ClientDisconnectionOperation op = new ClientDisconnectionOperation(clientUuid);
        op.setNodeEngine(nodeEngine)
                .setServiceName(SERVICE_NAME)
                .setService(this)
                .setOperationResponseHandler(createEmptyResponseHandler());
        return op;
    }

    private class DestroyEndpointTask implements Runnable {
        private final String deadMemberUuid;

        public DestroyEndpointTask(String deadMemberUuid) {
            this.deadMemberUuid = deadMemberUuid;
        }

        @Override
        public void run() {
            endpointManager.removeEndpoints(deadMemberUuid);
            removeMappings();
        }

        void removeMappings() {
            Iterator<Map.Entry<String, String>> iterator = ownershipMappings.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, String> entry = iterator.next();
                String clientUuid = entry.getKey();
                String memberUuid = entry.getValue();
                if (deadMemberUuid.equals(memberUuid)) {
                    iterator.remove();
                    ClientDisconnectionOperation op = createClientDisconnectionOperation(clientUuid);
                    nodeEngine.getOperationService().runOperationOnCallingThread(op);
                }
            }
        }
    }

    @Override
    public Operation getPostJoinOperation() {
        return ownershipMappings.isEmpty() ? null : new PostJoinClientOperation(ownershipMappings);
    }

    @Override
    public Map<ClientType, Integer> getConnectedClientStats() {

        int numberOfCppClients = 0;
        int numberOfDotNetClients = 0;
        int numberOfJavaClients = 0;
        int numberOfOtherClients = 0;

        Operation clientInfoOperation = new GetConnectedClientsOperation();
        OperationService operationService = node.nodeEngine.getOperationService();
        Map<ClientType, Integer> resultMap = new HashMap<ClientType, Integer>();
        Map<String, ClientType> clientsMap = new HashMap<String, ClientType>();

        for (Member member : node.getClusterService().getMembers()) {
            Address target = member.getAddress();
            Future<Map<String, ClientType>> future
                    = operationService.invokeOnTarget(SERVICE_NAME, clientInfoOperation, target);
            try {
                Map<String, ClientType> endpoints = future.get();
                if (endpoints == null) {
                    continue;
                }
                //Merge connected clients according to their uuid.
                for (Map.Entry<String, ClientType> entry : endpoints.entrySet()) {
                    clientsMap.put(entry.getKey(), entry.getValue());
                }
            } catch (Exception e) {
                logger.warning("Cannot get client information from: " + target.toString(), e);
            }
        }

        //Now we are regrouping according to the client type
        for (ClientType clientType : clientsMap.values()) {
            switch (clientType) {
                case JAVA:
                    numberOfJavaClients++;
                    break;
                case CSHARP:
                    numberOfDotNetClients++;
                    break;
                case CPP:
                    numberOfCppClients++;
                    break;
                default:
                    numberOfOtherClients++;
            }
        }

        resultMap.put(ClientType.CPP, numberOfCppClients);
        resultMap.put(ClientType.CSHARP, numberOfDotNetClients);
        resultMap.put(ClientType.JAVA, numberOfJavaClients);
        resultMap.put(ClientType.OTHER, numberOfOtherClients);

        return resultMap;
    }

    private static class PriorityRunnable implements PartitionSpecificRunnable, UrgentSystemOperation {

        private final MessageTask task;

        public PriorityRunnable(MessageTask task) {
            this.task = task;
        }

        @Override
        public void run() {
            task.run();
        }

        @Override
        public int getPartitionId() {
            return task.getPartitionId();
        }
    }

    private class TrackingMessageTask implements MessageTask {
        private final MessageTask task;

        public TrackingMessageTask(MessageTask task) {
            this.task = task;
        }

        @Override
        public int getPartitionId() {
            return task.getPartitionId();
        }

        @Override
        public void run() {
            long startMs = System.currentTimeMillis();
            try {
                task.run();
            } finally {
                long durationMs = System.currentTimeMillis() - startMs;
                MessageTaskStatistics statistics = getStatistics(task.getClass());
                statistics.update(durationMs);
            }
        }

        private MessageTaskStatistics getStatistics(Class clazz) {
            MessageTaskStatistics statistics = taskStatistics.get(clazz);
            if (statistics == null) {
                MessageTaskStatistics update = new MessageTaskStatistics(clazz);
                MessageTaskStatistics found = taskStatistics.putIfAbsent(clazz, update);
                statistics = found == null ? update : found;
            }
            return statistics;
        }
    }

    private class ClientExecutorDelayMonitorThread extends Thread {

        @Override
        public void run() {
            MyRunnable command = new MyRunnable();
            executor.execute(command);

            try {
                int k = 0;
                while (isAlive()) {
                    Thread.sleep(1000);
                    k++;

                    if (k % 10 == 0) {
                        printOperations();
                    }

                    long delayMillis = System.currentTimeMillis() - command.startMillis;
                    if (delayMillis > 5000) {
                        logger.warning("Delay in client task: " + delayMillis + " ms");
                    }
                }
            } catch (InterruptedException e) {
            }
        }

        public void printOperations() {
            printTopInvocations();
            printTopExecutionTime();
            printTopMaxExecutionTime();
            printTopAverageExecutionTime();
        }

        private void printTopInvocations() {
            List<Map.Entry<Class, Long>> mostCalled = new ArrayList<Map.Entry<Class, Long>>();

            for (Map.Entry<Class, MessageTaskStatistics> entry : taskStatistics.entrySet()) {
                mostCalled.add(new SimpleMapEntry<Class, Long>(entry.getKey(), entry.getValue().totalInvocations.get()));
            }

            StringBuffer sb = new StringBuffer("Top invocations:\n");
            for (int k = 0; k < mostCalled.size() && k < 10; k++) {
                Map.Entry<Class, Long> entry = mostCalled.get(k);
                sb.append("\t").append(entry.getKey().getName()).append("=").append(entry.getValue()).append("\n");
            }
            logger.info(sb.toString());
        }


        private void printTopExecutionTime() {
            List<Map.Entry<Class, Long>> mostCalled = new ArrayList<Map.Entry<Class, Long>>();

            for (Map.Entry<Class, MessageTaskStatistics> entry : taskStatistics.entrySet()) {
                mostCalled.add(new SimpleMapEntry<Class, Long>(entry.getKey(), entry.getValue().totalTime.get()));
            }

            StringBuffer sb = new StringBuffer("Top execution time (ms):\n");
            for (int k = 0; k < mostCalled.size() && k < 10; k++) {
                Map.Entry<Class, Long> entry = mostCalled.get(k);
                sb.append("\t").append(entry.getKey().getName()).append("=").append(entry.getValue()).append("\n");
            }
            logger.info(sb.toString());
        }

        private void printTopMaxExecutionTime() {
            List<Map.Entry<Class, Long>> mostCalled = new ArrayList<Map.Entry<Class, Long>>();

            for (Map.Entry<Class, MessageTaskStatistics> entry : taskStatistics.entrySet()) {
                mostCalled.add(new SimpleMapEntry<Class, Long>(entry.getKey(), entry.getValue().maxTime.get()));
            }

            StringBuffer sb = new StringBuffer("Top max time (ms):\n");
            for (int k = 0; k < mostCalled.size() && k < 10; k++) {
                Map.Entry<Class, Long> entry = mostCalled.get(k);
                sb.append("\t").append(entry.getKey().getName()).append("=").append(entry.getValue()).append("\n");
            }
            logger.info(sb.toString());
        }

        private void printTopAverageExecutionTime() {
            List<Map.Entry<Class, Long>> mostCalled = new ArrayList<Map.Entry<Class, Long>>();

            for (Map.Entry<Class, MessageTaskStatistics> entry : taskStatistics.entrySet()) {
                MessageTaskStatistics taskStatistics = entry.getValue();
                long average = taskStatistics.totalTime.get() / taskStatistics.totalInvocations.get();
                mostCalled.add(new SimpleMapEntry<Class, Long>(entry.getKey(), average));
            }

            StringBuffer sb = new StringBuffer("Top average time (ms):\n");
            for (int k = 0; k < mostCalled.size() && k < 10; k++) {
                Map.Entry<Class, Long> entry = mostCalled.get(k);
                sb.append("\t").append(entry.getKey().getName()).append("=").append(entry.getValue()).append("\n");
            }
            logger.info(sb.toString());
        }

        private class SimpleMapEntry<K, V> implements Map.Entry<K, V> {

            private final K key;
            private final V value;

            public SimpleMapEntry(K key, V value) {
                this.key = key;
                this.value = value;
            }

            @Override
            public K getKey() {
                return key;
            }

            @Override
            public V getValue() {
                return value;
            }

            @Override
            public V setValue(V value) {
                throw new UnsupportedOperationException();
            }
        }

        public List<Map.Entry<Class, Long>> sort(Map<Class, Long> map) {
            List<Map.Entry<Class, Long>> entries = new ArrayList<Map.Entry<Class, Long>>(map.entrySet());
            Collections.sort(entries, new Comparator<Map.Entry<Class, Long>>() {
                @Override
                public int compare(Map.Entry<Class, Long> o1, Map.Entry<Class, Long> o2) {
                    return o1.getValue().compareTo(o2.getValue());
                }
            });
            return entries;
        }

        private class MyRunnable implements Runnable {
            private volatile long startMillis = System.currentTimeMillis();

            @Override
            public void run() {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    return;
                }

                startMillis = System.currentTimeMillis();
                executor.execute(this);
            }
        }
    }


    private static class MessageTaskStatistics {
        private final Class clazz;
        private final AtomicLong totalTime = new AtomicLong();
        private final AtomicLong totalInvocations = new AtomicLong();
        private final AtomicLong maxTime = new AtomicLong();

        public MessageTaskStatistics(Class clazz) {
            this.clazz = clazz;
        }

        public void update(long time) {
            totalInvocations.incrementAndGet();
            totalTime.addAndGet(time);

            for (; ; ) {
                long current = maxTime.get();
                if (time <= current) {
                    break;
                }

                if (maxTime.compareAndSet(current, time)) {
                    break;
                }
            }
        }
    }
}
