package org.opennms.core.grid.hazelcast;

import org.opennms.core.grid.MembershipEvent;

public class HazelcastMembershipEvent extends MembershipEvent {
    com.hazelcast.core.MembershipEvent m_membershipEvent;

    public HazelcastMembershipEvent(com.hazelcast.core.MembershipEvent membershipEvent) {
        m_membershipEvent = membershipEvent;
    }
}
