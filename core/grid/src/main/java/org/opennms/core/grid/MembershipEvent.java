package org.opennms.core.grid;

public abstract class MembershipEvent {

    public static final int MEMBER_ADDED = 1;

    public static final int MEMBER_REMOVED = 3;
    
    public abstract int getEventType();

    public abstract Member getMember();
}
