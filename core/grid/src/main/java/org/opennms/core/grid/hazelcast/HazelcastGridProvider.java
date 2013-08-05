package org.opennms.core.grid.hazelcast;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.opennms.core.logging.Logging;
import org.opennms.core.grid.AtomicLong;
import org.opennms.core.grid.DataGridProvider;
import org.opennms.core.grid.Member;
import org.opennms.core.grid.MembershipListener;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;

/**
 * Hazelcast Data Grid Provider
 * 
 * @author jwhite
 */
public class HazelcastGridProvider implements DataGridProvider {

    private static final String LOG4J_PREFIX = "hazelcast";

    private HazelcastInstance m_hazelcastInstance = null;

    private synchronized HazelcastInstance getHazelcastInstance() {
        if (m_hazelcastInstance == null) {
            @SuppressWarnings("rawtypes")
            Map mdc = Logging.getCopyOfContextMap();
            try {
                Logging.putPrefix(LOG4J_PREFIX);
                m_hazelcastInstance = Hazelcast.newHazelcastInstance();
            } finally {
                Logging.setContextMap(mdc);
            }
        }

        return m_hazelcastInstance;
    }

    /** {@inheritDoc} */
    @Override
    public void init() {
        getHazelcastInstance();
    }

    /** {@inheritDoc} */
    @Override
    public void shutdown() {
        getHazelcastInstance().getLifecycleService().shutdown();
    }

    /** {@inheritDoc} */
    @Override
    public AtomicLong getAtomicLong(String name) {
        return new HazelcastAtomicLong(
                                       getHazelcastInstance().getAtomicLong(name));
    }

    /** {@inheritDoc} */
    @Override
    public Condition getCondition(Lock lock, String name) {
        if (lock instanceof ILock) {
            return ((ILock)lock).newCondition(name);
        } else {
            throw new RuntimeException("Invalid lock");
        }
    }

    /** {@inheritDoc} */
    @Override
    public Lock getLock(String name) {
        return getHazelcastInstance().getLock(name);
    }

    /** {@inheritDoc} */
    @Override
    public <K, V> Map<K, V> getMap(String name) {
        return getHazelcastInstance().getMap(name);
    }

    /** {@inheritDoc} */
    @Override
    public <T> BlockingQueue<T> getQueue(String name) {
        return getHazelcastInstance().getQueue(name);
    }

    /** {@inheritDoc} */
    @Override
    public <T> Set<T> getSet(String name) {
        return getHazelcastInstance().getSet(name);
    }

    /** {@inheritDoc} */
    @Override
    public Member getLocalMember() {
        return new HazelcastMember(
                                   getHazelcastInstance().getCluster().getLocalMember());
    }

    /** {@inheritDoc} */
    @Override
    public Set<Member> getGridMembers() {
        Set<Member> members = new HashSet<Member>();
        for (com.hazelcast.core.Member member : getHazelcastInstance().getCluster().getMembers()) {
            members.add(new HazelcastMember(member));
        }
        return members;
    }

    /** {@inheritDoc} */
    @Override
    public String addMembershipListener(MembershipListener listener) {
        return getHazelcastInstance().getCluster().addMembershipListener(new HazelcastMembershipListener(
                                                                                                         listener));
    }

    /** {@inheritDoc} */
    @Override
    public void removeMembershipListener(String registrationId) {
        getHazelcastInstance().getCluster().removeMembershipListener(registrationId);
    }

    public String toString() {
        return "Hazelcast Data Grid Provider[" + getHazelcastInstance() + "]";
    }

    public void shutdownAll() {
        Hazelcast.shutdownAll();
    }

    @Override
    public boolean isRunning() {
        return getHazelcastInstance().getLifecycleService().isRunning();
    }
}
