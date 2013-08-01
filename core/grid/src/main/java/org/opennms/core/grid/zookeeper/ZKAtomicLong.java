package org.opennms.core.grid.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.opennms.core.grid.AtomicLong;

public class ZKAtomicLong implements AtomicLong {

    public static final String PATH_PREFIX = "/onms/atomic/";

    private final DistributedAtomicLong m_atomicLong;

    public ZKAtomicLong(CuratorFramework client, String name) {
        m_atomicLong = new DistributedAtomicLong(client, PATH_PREFIX + name, ZKConfigFactory.getInstance().getRetryPolicy());
    }

    @Override
    public long addAndGet(long delta) {
        try {
            return m_atomicLong.add(delta).postValue();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean compareAndSet(long expect, long update) {
        try {
            return m_atomicLong.compareAndSet(expect, update).postValue() == update;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long decrementAndGet() {
        try {
            return m_atomicLong.decrement().postValue();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getAndDecrement() {
        try {
            return m_atomicLong.decrement().preValue();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long get() {
        try {
            return m_atomicLong.get().postValue();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getAndAdd(long delta) {
        try {
            return m_atomicLong.add(delta).preValue();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getAndSet(long newValue) {
        try {
            return m_atomicLong.trySet(newValue).preValue();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long incrementAndGet() {
        try {
            return m_atomicLong.increment().postValue();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getAndIncrement() {
        try {
            return m_atomicLong.increment().preValue();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void set(long newValue) {
        try {
            m_atomicLong.trySet(newValue);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String toString() {
        return Long.valueOf(get()).toString();
    }
}
