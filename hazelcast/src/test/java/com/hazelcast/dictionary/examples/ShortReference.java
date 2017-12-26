package com.hazelcast.dictionary.examples;

import java.io.Serializable;

public class ShortReference implements Serializable {

    public Short field;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ShortReference that = (ShortReference) o;

        return field != null ? field.equals(that.field) : that.field == null;
    }

    @Override
    public int hashCode() {
        return field != null ? field.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "" + field;
    }
}