package org.opennms.core.grid.hazelcast;

import org.opennms.core.grid.Member;
import org.opennms.core.grid.MembershipEvent;

public class HazelcastMembershipEvent extends MembershipEvent {
    com.hazelcast.core.MembershipEvent m_membershipEvent;

    public HazelcastMembershipEvent(com.hazelcast.core.MembershipEvent membershipEvent) {
        m_membershipEvent = membershipEvent;
    }

    @Override
    public int getEventType() {
        return m_membershipEvent.getEventType();
    }

    @Override
    public Member getMember() {
        return new HazelcastMember(m_membershipEvent.getMember());
    }
}
