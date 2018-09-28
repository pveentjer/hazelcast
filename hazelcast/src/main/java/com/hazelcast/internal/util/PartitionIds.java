/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.util.Arrays.copyOf;


/**
 * A collection to store partition ids in a space/litter efficient manner.
 *
 * This should be used as a replacement for {@link java.util.BitSet} or using a
 * collection of Integers.
 *
 * Based on the idea of a {@link java.util.BitSet} but more concrete API.
 */
public final class PartitionIds {
    private final int capacity;
    private long[] words;

    /**
     * Creates a PartitionId based on an existing PartitionIds.
     *
     * The content of the existing PartitionIds is cloned.
     *
     * @param partitionIds the PartitionIds to clone
     * @throws NullPointerException if partitionIds is null.
     */
    public PartitionIds(PartitionIds partitionIds) {
        checkNotNull(partitionIds, "partitionIds can't be null");
        this.capacity = partitionIds.capacity;
        this.words = copyOf(partitionIds.words, partitionIds.words.length);
    }

    /**
     * Creates a PartitionIds with the given capacity.
     *
     * The capacity is normally the partition-count.
     *
     * @param capacity the capacity
     * @throws NullPointerException if capacity smaller than 0.
     */
    public PartitionIds(int capacity) {
        if (capacity < 0) {
            throw new IllegalArgumentException("capacity can't be smaller than 0, capacity=" + capacity);
        }
        this.capacity = capacity;
        this.words = new long[(wordIndex(capacity - 1)) + 1];
    }

    // just for testing
    long[] words() {
        return words;
    }

    /**
     * Checks if the partitionId already is in this PartitionIds.
     *
     * @param partitionId the partition id to check
     * @return true if part of this partitionIds, false otherwise.
     * @throws IllegalArgumentException if partitionId is not a valid partition Id.
     */
    public boolean contains(int partitionId) {
        checkPartitionId(partitionId);

        int wordIndex = wordIndex(partitionId);
        return (wordIndex < words.length) && ((words[wordIndex] & (1L << partitionId)) != 0);
    }

    private void checkPartitionId(int partitionId) {
        if (partitionId < 0) {
            throw new IllegalArgumentException("PartitionId can't be smaller than 0, partitionId=" + partitionId);
        }

        if (partitionId >= capacity) {
            throw new IllegalArgumentException("PartitionId can't be equal or larger than capacity. "
                    + "capacity=" + capacity + " partitionId=" + partitionId);
        }
    }

    /**
     * Adds the partitionId to this set. If already added, it is ignored.
     *
     * @param partitionId the partition id to add
     * @return true if part of this partitionIds, false otherwise.
     * @throws IllegalArgumentException if partitionId is not a valid partition Id.
     */
    public void add(int partitionId) {
        checkPartitionId(partitionId);
        int wordIndex = wordIndex(partitionId);
        words[wordIndex] |= (1L << partitionId);
    }

    private int wordIndex(int partitionId) {
        return partitionId >> 3;
    }

    /**
     * Return the capacity of this PartitionIds.
     *
     * @return the capacity.
     */
    public int capacity() {
        return capacity;
    }

    /**
     * Adds all partition ids of that to this set.
     *
     * If the partition id already is set, it is ignored.
     *
     * @param that the partition ids to add.
     * @throws NullPointerException     if that is null.
     * @throws IllegalArgumentException if that has a different capacity than this.
     */
    public PartitionIds addAll(PartitionIds that) {
        checkNotNull(that, "partitionIds can't be null");
        checkCapacity(that);

        for (int k = 0; k < words.length; k++) {
            words[k] = this.words[k] | that.words[k];
        }

        return this;
    }

    /**
     * Adds all partitions from the array to this PartitionIds.
     *
     * If the partition id is already set, it is ignored.
     *
     * @param partitionIds the partitions to add
     * @return this reference.
     * @throws NullPointerException     if partitionIds is null.
     * @throws IllegalArgumentException if one of the partition id's is invalid.
     */
    public PartitionIds addAll(int[] partitionIds) {
        checkNotNull(partitionIds, "partitionIds can't be null");

        for (int partitionId : partitionIds) {
            add(partitionId);
        }

        return this;
    }

    private void checkCapacity(PartitionIds that) {
        if (capacity != that.capacity) {
            throw new IllegalArgumentException("capacity doesn't match, "
                    + "expected=" + capacity + " found=" + that.capacity);
        }
    }

    /**
     * Returns the partitionId that occurs on or after the specified from partition.
     *
     * If no partition is found, -1 is returned.
     *
     * @param fromPartitionId the first partition to look for.
     * @return the paritionId or -1 if no such partition is found.
     */
    public int nextPartitionId(int fromPartitionId) {
        if (fromPartitionId < 0) {
            throw new IllegalArgumentException("fromPartitionId can't be smaller than 0, fromPartitionId=" + fromPartitionId);
        }

        for (int k = fromPartitionId; k < capacity; k++) {
            if (contains(k)) {
                return k;
            }
        }

        return -1;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (int k = 0; k < capacity; k++) {
            if (contains(k)) {
                if (first) {
                    first = false;
                } else {
                    sb.append(',');
                }
                sb.append(k);
            }
        }
        sb.append('}');
        return sb.toString();
    }
}
