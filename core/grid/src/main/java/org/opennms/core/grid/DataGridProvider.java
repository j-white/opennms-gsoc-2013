package org.opennms.core.grid;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.Lock;

/**
 * Data grid provider used to abstract the implementation of distributed
 * primitives.
 * 
 * @author jwhite
 */
public interface DataGridProvider {

    /**
     * Initializes the grid provider.
     * 
     * Will be done automatically on the first method call if not manually
     * invoked.
     */
    public void init();

    /**
     * Shutdowns the grid provider.
     * 
     * Disconnects the client from the cluster.
     */
    public void shutdown();

    public void shutdownAll();

    /**
     * Retrieves a distributed atomic long.
     * 
     * @param name
     *            unique key that references this atomic long across the grid
     * @return atomic long
     */
    public AtomicLong getAtomicLong(String name);

    /**
     * Retrieves a distributed lock.
     * 
     * GOTCHAS: synchronized() doesn't work with these locks. You must use
     * lock.lock() instead.
     * 
     * @param name
     *            unique key that references this lock across the grid
     * @return distributed lock
     */
    public Lock getLock(String name);

    /**
     * Retrieves a distributed map.
     * 
     * GOTCHAS: You must put any element back in the map after changing it.
     * 
     * @param name
     *            unique key that references this map across the grid
     * @return distributed map
     */
    public <K, V> Map<K, V> getMap(String name);

    /**
     * Retrieves a distributed queue.
     * 
     * @param name
     *            unique key that references this queue across the grid
     * @return distributed queue
     */
    public <T> BlockingQueue<T> getQueue(String name);

    /**
     * Retrieves a distributed set.
     * 
     * @param name
     *            unique key that references this set across the grid
     * @return distributed set
     */
    public <T> Set<T> getSet(String name);

    /**
     * Returns the name of this grid provider instance.
     * 
     * @return name of this grid provider instance
     */
    public String getName();

    /**
     * Retrieves the member for this grid provider instance.
     * 
     * @return member for this grid provider instance
     */
    public Member getLocalMember();

    /**
     * Retrieves the set of members currently active on the grid.
     * 
     * @return set of grid members
     */
    public Set<Member> getGridMembers();

    /**
     * Adds a new membership listener.
     * 
     * @param listener
     *            membership event listener
     * @return registration id used to remove the listener
     */
    public String addMembershipListener(MembershipListener listener);

    /**
     * Removes a existing membership listener
     * 
     * @param registrationId
     *            registration id
     */
    public void removeMembershipListener(String registrationId);
}
