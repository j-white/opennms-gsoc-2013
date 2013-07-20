package org.opennms.core.grid.hazelcast;

import org.opennms.core.grid.MembershipListener;

import com.hazelcast.core.MembershipEvent;

public class HazelcastMembershipListener implements com.hazelcast.core.MembershipListener {
    MembershipListener m_membershipListener;
    
    public HazelcastMembershipListener(MembershipListener membershipListener) {
        m_membershipListener = membershipListener;
    }

    @Override
    public void memberAdded(MembershipEvent membershipEvent) {
        m_membershipListener.memberAdded(new HazelcastMembershipEvent(membershipEvent));
    }

    @Override
    public void memberRemoved(MembershipEvent membershipEvent) {
        m_membershipListener.memberRemoved(new HazelcastMembershipEvent(membershipEvent));
    }
}
