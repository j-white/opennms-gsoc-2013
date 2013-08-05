package org.opennms.core.grid.zookeeper;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.opennms.core.grid.AtomicLong;
import org.opennms.core.grid.DataGridProvider;
import org.opennms.core.grid.Member;
import org.opennms.core.grid.MembershipListener;

/**
 * Implements the data grid provider interface using ZooKeeper.
 * 
 * Exception and error handling needs work.
 * Also, I still need to figure out a way to deal with interrupted exceptions. Ignore them like Hazelcast does?
 * 
 * @author jwhite
 */
public class ZooKeeperGridProvider implements DataGridProvider {
    private CuratorFramework m_client = null;
    private ZKMemberManager m_memberManager;

    @Override
    public synchronized void init() {
        if (m_client != null) {
            return;
        }

        try {
            m_client = CuratorFrameworkFactory.newClient(getConfig().getServerConnectionString(),
                                                         getConfig().getRetryPolicy());
            m_client.start();

            m_memberManager = new ZKMemberManager(m_client);
        } catch (RuntimeException e) {
            if (m_client != null) {
                m_client.close();
                m_client = null;
            }
            throw e;
        }
    }

    /** @inheritDoc */
    @Override
    public void shutdown() {
        m_client.close();
        m_client = null;
    }

    /** @inheritDoc */
    @Override
    public void shutdownAll() {

    }

    /** @inheritDoc */
    @Override
    public AtomicLong getAtomicLong(String name) {
        init();
        return new ZKAtomicLong(m_client, name);
    }

    /** @inheritDoc */
    @Override
    public Condition getCondition(final Lock lock, final String name) {
        if (lock instanceof ZKLock) {
            return ((ZKLock)lock).newCondition(name);
        } else {
            throw new RuntimeException("Invalid lock");
        }
    }

    /** @inheritDoc */
    @Override
    public Lock getLock(String name) {
        init();
        return new ZKLock(m_client, name);
    }

    /** @inheritDoc */
    @Override
    public <K, V> Map<K, V> getMap(final String name) {
        init();
        return new ZKMap<K, V>(m_client, name);
    }

    /** @inheritDoc */
    @Override
    public <T> Set<T> getSet(String name) {
        init();
        return new ZKSet<T>(m_client, name);
    }

    /** @inheritDoc */
    @Override
    public <T> BlockingQueue<T> getQueue(final String name) {
        init();
        return new ZKQueue<T>(m_client, name);
    }

    /** @inheritDoc */
    @Override
    public Member getLocalMember() {
        return m_memberManager.getLocalMember();
    }

    /** @inheritDoc */
    @Override
    public Set<Member> getGridMembers() {
        return m_memberManager.getGridMembers();
    }

    /** @inheritDoc */
    @Override
    public String addMembershipListener(MembershipListener listener) {
        return m_memberManager.addMembershipListener(listener);
    }

    /** @inheritDoc */
    @Override
    public void removeMembershipListener(String registrationId) {
        m_memberManager.removeMembershipListener(registrationId);
    }

    private static ZKConfigDao getConfig() {
        return ZKConfigFactory.getInstance();
    }

    @Override
    public boolean isRunning() {
        return true;
    }
}
