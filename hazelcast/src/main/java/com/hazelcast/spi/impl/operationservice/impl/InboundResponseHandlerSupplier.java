/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.internal.metrics.MetricsProvider;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.NodeEngine;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;

/**
 * A {@link Supplier} responsible for providing a {@link Consumer} that
 * processes inbound responses.
 *
 * Depending on the {@link com.hazelcast.spi.properties.GroupProperty#RESPONSE_THREAD_COUNT}
 * it will return the appropriate response handler:
 * <ol>
 * <li>a 'sync' response handler that doesn't offload to a different thread and
 * processes the response on the calling (IO) thread.</li>
 * <li>a single threaded Packet Consumer that offloads the response processing a
 * ResponseThread</li>
 * <li>a multi threaded Packet Consumer that offloads the response processing
 * to a pool of ResponseThreads.</li>
 * </ol>
 * Having multiple threads processing responses improves performance and
 * stability of the throughput.
 *
 * In case of asynchronous response processing, the response is put in the
 * responseQueue of the ResponseThread. Then the ResponseThread takes it from
 * this responseQueue and calls a {@link Consumer} for the actual processing.
 *
 * The reason that the IO thread doesn't immediately deal with the response is that
 * dealing with the response and especially notifying the invocation future can be
 * very expensive.
 */
public class InboundResponseHandlerSupplier implements MetricsProvider, Supplier<Consumer<Packet>> {

    // these references are needed for metrics.
    private final List<InboundResponseHandler> inboundResponseHandlers = new ArrayList<>();
    private final NodeEngine nodeEngine;
    private final InvocationRegistry invocationRegistry;

    InboundResponseHandlerSupplier(InvocationRegistry invocationRegistry,
                                   NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.invocationRegistry = invocationRegistry;
    }

    @Override
    public Consumer<Packet> get() {
        InboundResponseHandler handler = new InboundResponseHandler(invocationRegistry, nodeEngine);
        inboundResponseHandlers.add(handler);
        return handler;
    }

    public InboundResponseHandler backupHandler() {
        return inboundResponseHandlers.get(0);
    }

    @Probe(name = "responses[normal]", level = MANDATORY)
    long responsesNormal() {
        long result = 0;
        for (InboundResponseHandler handler : inboundResponseHandlers) {
            result += handler.responsesNormal.get();
        }
        return result;
    }

    @Probe(name = "responses[timeout]", level = MANDATORY)
    long responsesTimeout() {
        long result = 0;
        for (InboundResponseHandler handler : inboundResponseHandlers) {
            result += handler.responsesTimeout.get();
        }
        return result;
    }

    @Probe(name = "responses[backup]", level = MANDATORY)
    long responsesBackup() {
        long result = 0;
        for (InboundResponseHandler handler : inboundResponseHandlers) {
            result += handler.responsesBackup.get();
        }
        return result;
    }

    @Probe(name = "responses[error]", level = MANDATORY)
    long responsesError() {
        long result = 0;
        for (InboundResponseHandler handler : inboundResponseHandlers) {
            result += handler.responsesError.get();
        }
        return result;
    }

    @Probe(name = "responses[missing]", level = MANDATORY)
    long responsesMissing() {
        long result = 0;
        for (InboundResponseHandler handler : inboundResponseHandlers) {
            result += handler.responsesMissing.get();
        }
        return result;
    }

    @Override
    public void provideMetrics(MetricsRegistry registry) {
        registry.scanAndRegister(this, "operation");
    }
}
