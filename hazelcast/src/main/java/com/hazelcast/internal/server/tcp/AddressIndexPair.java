package com.hazelcast.internal.server.tcp;

import com.hazelcast.cluster.Address;

public final class AddressIndexPair {
    private final Address address;
    private final int index;

    public AddressIndexPair(Address address, int index) {
        this.address = address;
        this.index = index;
    }

    public Address getAddress() {
        return address;
    }

    public int getIndex() {
        return index;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AddressIndexPair that = (AddressIndexPair) o;

        if (index != that.index) return false;
        return address != null ? address.equals(that.address) : that.address == null;
    }

    @Override
    public int hashCode() {
        int result = address != null ? address.hashCode() : 0;
        result = 31 * result + index;
        return result;
    }

    @Override
    public String toString() {
        return "AddressIndexPair{" +
                "address=" + address +
                ", index=" + index +
                '}';
    }
}
