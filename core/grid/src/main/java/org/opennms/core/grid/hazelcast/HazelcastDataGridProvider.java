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
import org.opennms.core.grid.Topic;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;

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


    @Override
    public void shutdown() {
        //TODO
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
    public <T> Topic<T> getTopic(String name) {
        ITopic<T> topic = getHazelcastInstance().getTopic(name);
        return new HazcelcastTopic<T>(topic);
    }

    /** {@inheritDoc} */
    @Override
    public String getName() {
        return getHazelcastInstance().getName();
    }

    /** {@inheritDoc} */
    @Override
    public Set<Member> getClusterMembers() {
        Set<Member> members = new HashSet<Member>();
        for (com.hazelcast.core.Member member : getHazelcastInstance().getCluster().getMembers()) {
            members.add(new HazelcastMember(member));
        }
        return members;
    }

    public String toString() {
        return "Hazelcast Data Grid Provider";
    }

    @Override
    public void addMembershipListener(MembershipListener listener) {
        getHazelcastInstance().getCluster().addMembershipListener(new HazelcastMembershipListener(listener));
    }

    @Override
    public void removeMembershipListener(String registrationId) {
        getHazelcastInstance().getCluster().removeMembershipListener(registrationId);
    }
}
