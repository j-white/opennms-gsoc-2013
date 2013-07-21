package org.opennms.netmgt.dao.mock;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.opennms.core.grid.DataGridProvider;
import org.opennms.core.grid.Member;
import org.opennms.core.grid.MembershipListener;

public class MockDataGridProvider implements DataGridProvider {

    @Override
    public void init() {
        // this method is intentionally left blank
    }

    @Override
    public void shutdown() {
        // this method is intentionally left blank
    }

    @Override
    public Lock getLock(Object key) {
        return new ReentrantLock();
    }

    @Override
    public <K, V> Map<K, V> getMap(String name) {
        return new HashMap<K,V>();
    }

    @Override
    public <T> BlockingQueue<T> getQueue(String name) {
        return new LinkedBlockingQueue<T>();
    }

    @Override
    public <T> Set<T> getSet(String name) {
        return new HashSet<T>();
    }

    @Override
    public String getName() {
        return "MockDataGridProvider";
    }

    @Override
    public Member getLocalMember() {
        return new Member() {
            @Override
            public String getUuid() {
                return "3.14";
            }

            @Override
            public InetSocketAddress getInetSocketAddress() {
                return new InetSocketAddress(0);
            }
        };
    }

    @Override
    public Set<Member> getGridMembers() {
        Set<Member> members = new HashSet<Member>();
        members.add(getLocalMember());
        return members;
    }

    @Override
    public String addMembershipListener(MembershipListener listener) {
        return "";
    }

    @Override
    public void removeMembershipListener(String registrationId) {
        // this method is intentionally left blank
    }
}
