package org.opennms.core.grid.zookeeper;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.opennms.core.grid.AtomicLong;
import org.opennms.core.grid.Member;
import org.opennms.core.grid.MembershipListener;
import org.opennms.core.grid.activemq.ActiveMQGridProvider;

/**
 * Implements the data grid provider interface using ZooKeeper.
 * 
 * @author jwhite
 */
public class ZooKeeperGridProvider extends ActiveMQGridProvider {
    private CuratorFramework m_client = null;

    @Override
    public synchronized void init() {
        if (m_client != null) {
            return;
        }

        m_client = CuratorFrameworkFactory.newClient(getConfig().getServerConnectionString(),
                                                     getConfig().getRetryPolicy());
        m_client.start();
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
    public Condition getCondition(Lock lock, String name) {
        // TODO Auto-generated method stub
        return null;
    }

    /** @inheritDoc */
    @Override
    public Lock getLock(String name) {
        init();
        return new ZKLock(m_client, name);
    }

    /** @inheritDoc */
    @Override
    public <K, V> Map<K, V> getMap(String name) {
        return null;
    }

    /** @inheritDoc */
    @Override
    public <T> Set<T> getSet(String name) {
        /*
         * /onms/sets/${name}/${elements}
         */
        return null;
    }

    /** @inheritDoc */
    @Override
    public String getName() {
        return null;
    }

    /** @inheritDoc */
    @Override
    public Member getLocalMember() {
        return null;
    }

    /** @inheritDoc */
    @Override
    public Set<Member> getGridMembers() {
        return null;
    }

    /** @inheritDoc */
    @Override
    public String addMembershipListener(MembershipListener listener) {
        return null;
    }

    /** @inheritDoc */
    @Override
    public void removeMembershipListener(String registrationId) {
        // this method is intentionally left blank
    }

    private static ZKConfigDao getConfig() {
        return ZKConfigFactory.getInstance();
    }

    @Override
    public boolean isRunning() {
        return true;
    }
}
