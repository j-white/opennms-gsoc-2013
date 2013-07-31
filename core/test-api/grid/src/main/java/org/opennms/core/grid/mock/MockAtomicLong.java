package org.opennms.core.grid.mock;

import java.util.concurrent.atomic.AtomicLong;


public class MockAtomicLong implements org.opennms.core.grid.AtomicLong {

    private final AtomicLong m_atomicLong = new AtomicLong();

    @Override
    public long addAndGet(long delta) {
        return m_atomicLong.addAndGet(delta);
    }

    @Override
    public boolean compareAndSet(long expect, long update) {
        return m_atomicLong.compareAndSet(expect, update);
    }

    @Override
    public long decrementAndGet() {
        return m_atomicLong.decrementAndGet();
    }

    @Override
    public long get() {
        return m_atomicLong.get();
    }

    @Override
    public long getAndAdd(long delta) {
        return m_atomicLong.getAndAdd(delta);
    }

    @Override
    public long getAndSet(long newValue) {
        return m_atomicLong.getAndSet(newValue);
    }

    @Override
    public long incrementAndGet() {
        return m_atomicLong.incrementAndGet();
    }

    @Override
    public long getAndIncrement() {
        return m_atomicLong.getAndIncrement();
    }

    @Override
    public void set(long newValue) {
        m_atomicLong.set(newValue);
    }
}
