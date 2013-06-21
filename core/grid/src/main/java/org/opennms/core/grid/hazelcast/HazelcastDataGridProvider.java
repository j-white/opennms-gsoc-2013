package org.opennms.core.grid.hazelcast;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import org.opennms.core.grid.DataGridProvider;
import org.opennms.core.grid.Member;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

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
