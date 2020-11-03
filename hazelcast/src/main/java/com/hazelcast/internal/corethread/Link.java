package com.hazelcast.internal.corethread;

import com.hazelcast.cluster.Address;

public class Link {

    public final Address address;
    public final int plane;

    public Link(Address address, int plane) {
        this.address = address;
        this.plane = plane;
    }
}
