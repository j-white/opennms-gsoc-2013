package org.opennms.core.grid.hazelcast;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import org.opennms.core.grid.DataGridProvider;
import org.opennms.core.grid.Member;
import org.opennms.core.grid.Topic;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;

public class HazelcastDataGridProvider implements DataGridProvider {

    private HazelcastInstance m_hazelcastInstance = null;

    private synchronized HazelcastInstance getHazelcastInstance() {
        if (m_hazelcastInstance == null) {
            m_hazelcastInstance = Hazelcast.newHazelcastInstance();
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
}
