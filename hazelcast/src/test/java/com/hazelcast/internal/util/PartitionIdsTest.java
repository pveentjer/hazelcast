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

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class PartitionIdsTest {

    @Test
    public void test_copyConstructor() {
        PartitionIds original = new PartitionIds(10);
        original.add(2);

        PartitionIds copy = new PartitionIds(original);
        assertEquals(original.capacity(), copy.capacity());
        assertArrayEquals(original.words(), copy.words());
        // we need to make sure we get a copy of the words
        assertNotSame(original.words(), copy.words());
    }

    @Test(expected = NullPointerException.class)
    public void test_copyConstructor_whenNull() {
        new PartitionIds(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_whenNegativePartitionCount() {
        new PartitionIds(-1);
    }

    @Test
    public void constructor_wordLength() {
        assertWordLength(0, 0);
        assertWordLength(1, 1);
        assertWordLength(8, 1);
        assertWordLength(9, 2);
        assertWordLength(16, 2);
        assertWordLength(271, 34);
        assertWordLength(2710, 339);
    }

    private void assertWordLength(int partitionCount, int expectedWordLength) {
        assertEquals(expectedWordLength, new PartitionIds(partitionCount).words().length);
    }

    @Test
    public void add_whenNotAdded() {
        PartitionIds partitionIds = new PartitionIds(10);
        partitionIds.add(2);

        assertTrue(partitionIds.contains(2));
    }

    @Test
    public void add_whenAlreadyAdded() {
        PartitionIds partitionIds = new PartitionIds(10);
        partitionIds.add(2);
        partitionIds.add(2);

        assertTrue(partitionIds.contains(2));
    }

    @Test(expected = IllegalArgumentException.class)
    public void add_whenTooSmall() {
        PartitionIds partitionIds = new PartitionIds(10);
        partitionIds.add(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void set_whenToLarge() {
        PartitionIds partitionIds = new PartitionIds(10);
        partitionIds.add(10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void contains_whenTooSmall() {
        PartitionIds partitionIds = new PartitionIds(10);
        partitionIds.contains(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void contains_whenToLarge() {
        PartitionIds partitionIds = new PartitionIds(10);
        partitionIds.contains(10);
    }

    @Test(expected = NullPointerException.class)
    public void addAll_PartitionIds_whenNull() {
        PartitionIds a = new PartitionIds(10);
        a.addAll((PartitionIds) null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void addAll_PartitionIds__whenPartitionCountMismatch() {
        PartitionIds a = new PartitionIds(10);
        a.addAll(new PartitionIds(11));
    }

    @Test
    public void addAll_PartitionIds() {
        PartitionIds a = new PartitionIds(10);
        a.add(1);
        a.add(2);

        PartitionIds b = new PartitionIds(10);
        b.add(2);
        b.add(3);
        b.add(4);

        a.addAll(b);

        assertTrue(a.contains(1));
        assertTrue(a.contains(2));
        assertTrue(a.contains(3));
        assertTrue(a.contains(4));
    }

    @Test(expected = NullPointerException.class)
    public void addAll_intArray_whenNull() {
        PartitionIds a = new PartitionIds(10);
        a.addAll((int[]) null);
    }

    @Test
    public void addAll_intArray() {
        PartitionIds a = new PartitionIds(10);
        a.add(1);
        a.add(2);

        int[] b = new int[]{2, 3, 4};
        a.addAll(b);

        assertTrue(a.contains(1));
        assertTrue(a.contains(2));
        assertTrue(a.contains(3));
        assertTrue(a.contains(4));
    }

    @Test(expected = IllegalArgumentException.class)
    public void nextPartitionId_whenNegative() {
        PartitionIds a = new PartitionIds(10);
        a.nextPartitionId(-1);
    }

    @Test
    public void nextPartitionId() {
        PartitionIds a = new PartitionIds(7);
        a.add(1);
        a.add(2);
        a.add(5);

        assertEquals(1, a.nextPartitionId(0));
        assertEquals(1, a.nextPartitionId(1));
        assertEquals(2, a.nextPartitionId(2));
        assertEquals(5, a.nextPartitionId(3));
        assertEquals(5, a.nextPartitionId(4));
        assertEquals(5, a.nextPartitionId(5));
        assertEquals(-1, a.nextPartitionId(6));
        assertEquals(-1, a.nextPartitionId(7));
    }

    @Test
    public void test_toString() {
        PartitionIds a = new PartitionIds(10);
        a.add(1);
        assertEquals("{1}", a.toString());

        a.add(4);
        a.add(5);

        assertEquals("{1,4,5}", a.toString());
    }
}
