/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi;

/**
 * <h1></h1>
 *
 *
 *
 *
 * <h1>Future additions</h1>
 * In the future we can add more values to this enumeration, for example 'YIELD' for batching operations that wants to
 * release the operation thread so that other operations can be interleaved.
 */
public class CallStatus {

    /**
     *
     */
    public static final int ENUM_DONE_RESPONSE = 0;
    /**
     *
     */
    public static final int ENUM_DONE_VOID = 1;
    /**
     *
     */
    public static final int ENUM_WAIT_RESPONSE = 2;
    /**
     *
     */
    public static final int ENUM_OFFLOADED = 3;

    /**
     * Signals that the Operation is done running and that a response is ready to be returned. Most of the normal operations
     * like IAtomicLong.get will fall in this category.
     */
    public static final CallStatus DONE_RESPONSE = new CallStatus(ENUM_DONE_RESPONSE);

    /**
     * Signals that the Operation is done running, but no response will be returned. Most of the regular operations like map.get
     * will return a response, but there are also fire and forget operations (lot of cluster operations) that don't return a
     * response.
     */
    public static final CallStatus DONE_VOID = new CallStatus(ENUM_DONE_VOID);

    /**
     * Indicates that the call could not complete because waiting is required. E.g. a queue.take on an empty queue. This can
     * only be returned by BlockingOperations.
     */
    public static final CallStatus WAIT = new CallStatus(ENUM_WAIT_RESPONSE);

    private final int ordinalValue;

    public CallStatus(int ordinalValue) {
        this.ordinalValue = ordinalValue;
    }

    public int ordinal() {
        return ordinalValue;
    }

    //
//    public enum CallStatusEnum {
//        DONE_RESPONSE,
//        DONE_VOID,
//
//        WAIT,
//
//        OFFLOADED
//    }
}

