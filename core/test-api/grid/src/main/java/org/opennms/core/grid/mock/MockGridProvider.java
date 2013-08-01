package org.opennms.core.grid.mock;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.opennms.core.grid.AtomicLong;
import org.opennms.core.grid.DataGridProvider;
import org.opennms.core.grid.Member;
import org.opennms.core.grid.MembershipEvent;
import org.opennms.core.grid.MembershipListener;

public class MockGridProvider implements DataGridProvider, Member {
    public static final Map<String, Object> gridMap = new ConcurrentHashMap<String, Object>();
    public static final Set<Member> members = new HashSet<Member>();
    public static final Map<String, MembershipListener> membershipListeners = new ConcurrentHashMap<String, MembershipListener>();
    private final String m_uuid = UUID.randomUUID().toString();

    private volatile boolean m_initialized = false;

    @Override
    public synchronized void init() {
        join(this);
        m_initialized = true;
    }

    @Override
    public synchronized void shutdown() {
        leave(this);
        m_initialized = false;
    }

    @Override
    public synchronized void shutdownAll() {
        for (Member member : members) {
            leave(member, false);
        }
        members.clear();
        gridMap.clear();
    }

    @Override
    public boolean isRunning() {
        return members.contains(this);
    }
 
    private void initIfRequired() {
        if (!m_initialized) {
            init();
        }
    }

    private synchronized Object getOrSet(final String key, Object o) {
        initIfRequired();
        if (!gridMap.containsKey(key)) {
            gridMap.put(key, o);
        }
        return gridMap.get(key);
    }

    @Override
    public synchronized AtomicLong getAtomicLong(String name) {
        return (AtomicLong)getOrSet("AtomicLong." + name, new MockAtomicLong());
    }

    @Override
    public Condition getCondition(Lock lock, String name) {
        return lock.newCondition();
    }

    @Override
    public synchronized Lock getLock(String name) {
        return (Lock)getOrSet("Lock." + name, new ReentrantLock());
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized <K, V> Map<K, V> getMap(final String name) {
        return (Map<K, V>)getOrSet("Map." + name, new HashMap<K,V>());
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized <T> BlockingQueue<T> getQueue(String name) {
        return (BlockingQueue<T>)getOrSet("Queue." + name, new LinkedBlockingQueue<T>());
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized <T> Set<T> getSet(String name) {
        return (Set<T>)getOrSet("Set." + name, new HashSet<T>());
    }

    @Override
    public String getName() {
        return "MockDataGridProvider";
    }

    @Override
    public Member getLocalMember() {
        initIfRequired();
        return this;
    }

    @Override
    public synchronized Set<Member> getGridMembers() {
        initIfRequired();
        return members;
    }

    @Override
    public synchronized String addMembershipListener(MembershipListener listener) {
        initIfRequired();
        String registrationId = null;
        while (0 != 1) {
            registrationId = UUID.randomUUID().toString();
            if (!membershipListeners.containsKey(registrationId)) {
                membershipListeners.put(registrationId, listener);
                break;
            }
        }
        return registrationId;
    }

    @Override
    public synchronized void removeMembershipListener(String registrationId) {
        membershipListeners.remove(registrationId);
    }

    private void join(final Member member) {
        members.add(member);
        for(MembershipListener listener : membershipListeners.values()) {
            listener.memberAdded(new MembershipEvent() {
                @Override
                public int getEventType() {
                    return MembershipEvent.MEMBER_ADDED;
                }
                @Override
                public Member getMember() {
                    return member;
                }
            });
        }
    }

    private void leave(final Member member) {
        leave(member,true);
    }

    private void leave(final Member member, final boolean remove) {
        if (remove) {
            members.remove(member);
        }

        for(MembershipListener listener : membershipListeners.values()) {
            listener.memberRemoved(new MembershipEvent() {
                @Override
                public int getEventType() {
                    return MembershipEvent.MEMBER_REMOVED;
                }
                @Override
                public Member getMember() {
                    return member;
                }
            });
        }
    }

    @Override
    public String getUuid() {
        return m_uuid;
    }

    @Override
    public InetSocketAddress getInetSocketAddress() {
        return new InetSocketAddress(0);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (m_initialized ? 1231 : 1237);
        result = prime * result + ((m_uuid == null) ? 0 : m_uuid.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MockGridProvider other = (MockGridProvider) obj;
        if (m_initialized != other.m_initialized)
            return false;
        if (m_uuid == null) {
            if (other.m_uuid != null)
                return false;
        } else if (!m_uuid.equals(other.m_uuid))
            return false;
        return true;
    }
}
