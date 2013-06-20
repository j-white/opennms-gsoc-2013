package org.opennms.core.grid;

import java.util.concurrent.locks.Lock;

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

    @Override
    public Lock getLock(Object key) {
        return getHazelcastInstance().getLock(key);
    }

    public String toString() {
        return "Hazelcast";
    }
}
