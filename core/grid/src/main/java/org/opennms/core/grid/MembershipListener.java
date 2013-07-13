package org.opennms.core.grid;

public interface MembershipListener {
    void memberAdded(MembershipEvent membershipEvent);

    void memberRemoved(MembershipEvent membershipEvent);
}
