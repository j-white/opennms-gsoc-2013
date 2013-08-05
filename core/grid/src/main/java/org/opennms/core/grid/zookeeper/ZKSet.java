package org.opennms.core.grid.zookeeper;

import java.util.Set;

import org.apache.curator.framework.CuratorFramework;

/**
 * My lazy implementation of a set.
 *
 * @author jwhite
 */
public class ZKSet<T> extends ZKQueue<T> implements Set<T> {

    public ZKSet(CuratorFramework client, String name) {
        super(client, "internal.set." + name);
    }

    @Override
    public boolean add(T el) {
        if (el == null) {
            throw new NullPointerException("The element cannot be null.");
        }

        if (!contains(el)) {
            super.add(el);
            return true;
        }

        return false;
    }
}
