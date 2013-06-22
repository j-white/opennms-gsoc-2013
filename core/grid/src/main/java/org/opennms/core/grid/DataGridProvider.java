package org.opennms.core.grid;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;

public interface DataGridProvider {
    /**
     * Manually initializes the grid provider.
     * 
     * Will be done automatically on the first method call if not
     * manually invoked.
     */
    public void init();

    /**
     * GOTCHAS: synchronized() doesn't work with these locks. you must use lock.lock() instead.
     */
    public Lock getLock(Object key);

    /**
     * GOTCHAS: you must put any element back in the map after changing it
     */
    <K, V> Map<K, V> getMap(String name);


    String getName();


    public Set<Member> getClusterMembers();


    <T> Topic<T> getTopic(String name);
}
