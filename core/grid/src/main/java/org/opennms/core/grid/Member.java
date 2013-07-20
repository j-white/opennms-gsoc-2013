package org.opennms.core.grid;

import java.net.InetSocketAddress;

/**
 * Cluster member interface inspired by com.hazelcast.core.Member.
 * 
 * @author jwhite
 */
public interface Member {
    /**
     * Returns UUID of this member.
     * 
     * @return UUID of this member.
     */
    public String getUuid();

    /**
     * Returns the InetSocketAddress of this member.
     * 
     * @return InetSocketAddress of this member
     */
    InetSocketAddress getInetSocketAddress();
}
