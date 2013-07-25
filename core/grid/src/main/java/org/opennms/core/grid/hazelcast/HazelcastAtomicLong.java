package org.opennms.core.grid.hazelcast;

import org.opennms.core.grid.AtomicLong;

import com.hazelcast.core.IAtomicLong;

/**
 * Wrapper from com.hazelcast.core.IAtomicLong to
 * org.opennms.core.grid.AtomicLong.
 * 
 * @author jwhite
 * 
 */
public class HazelcastAtomicLong implements AtomicLong {
    private final IAtomicLong m_atomicLong;

    public HazelcastAtomicLong(IAtomicLong atomicLong) {
        m_atomicLong = atomicLong;
    }

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
