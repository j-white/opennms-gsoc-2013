package org.opennms.core.grid.hazelcast;

import java.net.InetSocketAddress;

import org.opennms.core.grid.Member;

/**
 * Wrapper from com.hazelcast.core.Member to org.opennms.core.grid.Member.
 * 
 * @author jwhite
 * 
 */
public class HazelcastMember implements Member {
    private com.hazelcast.core.Member m_hazelcastMember;

    public HazelcastMember(com.hazelcast.core.Member hazelcastMember) {
        setHazelcastMember(hazelcastMember);
    }

    public com.hazelcast.core.Member getHazelcastMember() {
        return m_hazelcastMember;
    }

    public void setHazelcastMember(com.hazelcast.core.Member hazelcastMember) {
        m_hazelcastMember = hazelcastMember;
    }

    /** {@inheritDoc} */
    @Override
    public String getUuid() {
        return m_hazelcastMember.getUuid();
    }

    /** {@inheritDoc} */
    @Override
    public InetSocketAddress getInetSocketAddress() {
        return m_hazelcastMember.getInetSocketAddress();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime
                * result
                + ((m_hazelcastMember == null) ? 0
                                              : m_hazelcastMember.hashCode());
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
        HazelcastMember other = (HazelcastMember) obj;
        if (m_hazelcastMember == null) {
            if (other.m_hazelcastMember != null)
                return false;
        } else if (!m_hazelcastMember.equals(other.m_hazelcastMember))
            return false;
        return true;
    }
}
