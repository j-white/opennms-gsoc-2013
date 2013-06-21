package org.opennms.core.grid;

import java.util.Set;
import java.util.concurrent.locks.Lock;

public interface DataGridProvider {
    /**
     * Manually initializes the grid provider.
     * Will be done automatically on the first method call.
     */
    public void init();

    /**
     * Returns the distributed lock instance for the specified key object.
     * The specified object is considered to be the key for this lock.
     * So keys are considered equals cluster-wide as long as
     * they are serialized to the same byte array such as String, long,
     * Integer.
     *
     * @param key key of the lock instance
     * @return distributed lock instance for the specified key.
     */
    public Lock getLock(Object key);

    /**
     * Set of current members of the cluster.
     * Returning set instance is not modifiable.
     * Every member in the cluster has the same member list in the same
     * order. First member is the oldest member.
     *
     * @return current members of the cluster
     */
    public Set<Member> getClusterMembers();
}
