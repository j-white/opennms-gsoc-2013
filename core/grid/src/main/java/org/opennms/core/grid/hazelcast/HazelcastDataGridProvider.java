package org.opennms.core.grid.hazelcast;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.Lock;

import org.opennms.core.logging.Logging;
import org.opennms.core.grid.DataGridProvider;
import org.opennms.core.grid.Member;
import org.opennms.core.grid.MembershipListener;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * Hazelcast Data Grid Provider
 * 
 * @author jwhite
 */
public class HazelcastDataGridProvider implements DataGridProvider {

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
    public Lock getLock(Object key) {
        return getHazelcastInstance().getLock(key);
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
    public String getName() {
        return getHazelcastInstance().getName();
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
}
