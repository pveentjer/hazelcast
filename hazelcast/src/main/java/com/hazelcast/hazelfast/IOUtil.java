package com.hazelcast.hazelfast;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static java.lang.Class.forName;
import static java.lang.System.arraycopy;

public class IOUtil {
    public static final int INT_AS_BYTES = 4;
    public static final int LONG_AS_BYTES = 8;

    public static ByteBuffer allocateByteBuffer(boolean direct, int capacity){
        return direct ? ByteBuffer.allocateDirect(capacity) : ByteBuffer.allocate(capacity);
    }

    public static void compactOrClear(ByteBuffer bb) {
        if (bb.hasRemaining()) {
            bb.compact();
        } else {
            bb.clear();
        }
    }

    public static void setSendBufferSize(SocketChannel channel, int sendBufferSize) throws SocketException {
        channel.socket().setSendBufferSize(sendBufferSize);
        if (channel.socket().getSendBufferSize() != sendBufferSize) {
            System.out.println("socket doesn't have expected sendBufferSize, expected:"
                    + sendBufferSize + " actual:" + channel.socket().getSendBufferSize());
        }
    }

    public static void setReceiveBufferSize(SocketChannel channel, int receiveBufferSize) throws SocketException {
        channel.socket().setReceiveBufferSize(receiveBufferSize);
        if (channel.socket().getReceiveBufferSize() != receiveBufferSize) {
            System.out.println("socket doesn't have expected receiveBufferSize, expected:"
                    + receiveBufferSize + " actual:" + channel.socket().getReceiveBufferSize());
        }
    }

    public static void write(ByteBuffer dst, String s){
        dst.putInt(s.length());

    }

    /**
     * Creates a debug String for te given ByteBuffer. Useful when debugging IO.
     * <p>
     * Do not remove even if this method isn't used.
     *
     * @param name       name of the ByteBuffer.
     * @param byteBuffer the ByteBuffer
     * @return the debug String
     */
    public static String toDebugString(String name, ByteBuffer byteBuffer) {
        return name + "(pos:" + byteBuffer.position() + " lim:" + byteBuffer.limit()
                + " remain:" + byteBuffer.remaining() + " cap:" + byteBuffer.capacity() + ")";
    }

    static final String SELECTOR_IMPL = "sun.nio.ch.SelectorImpl";

    /**
     * Creates a new Selector and will optimize it if possible.
     *
    * @return the created Selector.
     * @throws NullPointerException if logger is null.
     */
    public static Selector newSelector() {

        Selector selector;
        try {
            selector = Selector.open();
        } catch (IOException e) {
            throw new RuntimeException("Failed to open a Selector", e);
        }

        boolean optimize = Boolean.parseBoolean(System.getProperty("hazelfast.io.optimizeselector", "true"));
        if (optimize) {
            optimize(selector);
        }
        return selector;
    }

    /**
     * Tries to optimize the provided Selector.
     *
     * @param selector the selector to optimize
     * @return an FastSelectionKeySet if the optimization was a success, null otherwise.
     * @throws NullPointerException if selector or logger is null.
     */
    static SelectionKeysSet optimize(Selector selector) {

        try {
            SelectionKeysSet set = new SelectionKeysSet();

            Class<?> selectorImplClass = findOptimizableSelectorClass(selector);
            if (selectorImplClass == null) {
                return null;
            }

            Field selectedKeysField = selectorImplClass.getDeclaredField("selectedKeys");
            selectedKeysField.setAccessible(true);

            Field publicSelectedKeysField = selectorImplClass.getDeclaredField("publicSelectedKeys");
            publicSelectedKeysField.setAccessible(true);

            selectedKeysField.set(selector, set);
            publicSelectedKeysField.set(selector, set);

            System.out.println("Optimized Selector: " + selector.getClass().getName());
            return set;
        } catch (Throwable t) {
            // we don't want to print at warning level because it could very well be that the target JVM doesn't
            // support this optimization. That is why we print on finest
            System.out.println("Failed to optimize Selector: " + selector.getClass().getName());
            t.printStackTrace();
            return null;
        }
    }

    static Class<?> findOptimizableSelectorClass(Selector selector) throws ClassNotFoundException {
        Class<?> selectorImplClass = forName(SELECTOR_IMPL, false, IOUtil.class.getClassLoader());

        // Ensure the current selector implementation is what we can instrument.
        if (!selectorImplClass.isAssignableFrom(selector.getClass())) {
            return null;
        }
        return selectorImplClass;
    }

    static class SelectionKeysSet extends AbstractSet<SelectionKey> {
        // the active SelectionKeys is the one where is being added to.
        SelectionKeys activeKeys = new SelectionKeys();
        // the passive SelectionKeys is one that is being read using the iterator.
        SelectionKeys passiveKeys = new SelectionKeys();

        // the iterator is recycled.
        private final IteratorImpl iterator = new IteratorImpl();

        SelectionKeysSet() {
        }

        @Override
        public boolean add(SelectionKey o) {
            return activeKeys.add(o);
        }

        @Override
        public int size() {
            return activeKeys.size;
        }

        @Override
        public Iterator<SelectionKey> iterator() {
            iterator.init(flip());
            return iterator;
        }

        private SelectionKey[] flip() {
            SelectionKeys tmp = activeKeys;
            activeKeys = passiveKeys;
            passiveKeys = tmp;

            activeKeys.size = 0;
            return passiveKeys.keys;
        }

        @Override
        public boolean remove(Object o) {
            return false;
        }

        @Override
        public boolean contains(Object o) {
            return false;
        }
    }

    static final class SelectionKeys {
        static final int INITIAL_CAPACITY = 32;

        SelectionKey[] keys = new SelectionKey[INITIAL_CAPACITY];
        int size;

        private boolean add(SelectionKey key) {
            if (key == null) {
                return false;
            }

            ensureCapacity();
            keys[size] = key;
            size++;
            return true;
        }

        private void ensureCapacity() {
            if (size < keys.length) {
                return;
            }

            SelectionKey[] newKeys = new SelectionKey[keys.length * 2];
            arraycopy(keys, 0, newKeys, 0, size);
            keys = newKeys;
        }
    }

    static final class IteratorImpl implements Iterator<SelectionKey> {

        SelectionKey[] keys;
        int index;

        private void init(SelectionKey[] keys) {
            this.keys = keys;
            this.index = -1;
        }

        @Override
        public boolean hasNext() {
            if (index >= keys.length - 1) {
                return false;
            }

            return keys[index + 1] != null;
        }

        @Override
        public SelectionKey next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            index++;
            return keys[index];
        }

        @Override
        public void remove() {
            if (index == -1 || index >= keys.length || keys[index] == null) {
                throw new IllegalStateException();
            }

            keys[index] = null;
        }
    }
}
