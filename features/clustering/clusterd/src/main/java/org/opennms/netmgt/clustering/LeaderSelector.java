/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2008-2013 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2013 The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is a registered trademark of The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * OpenNMS(R) is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with OpenNMS(R).  If not, see:
 *      http://www.gnu.org/licenses/
 *
 * For more information contact:
 *     OpenNMS(R) Licensing <license@opennms.org>
 *     http://www.opennms.org/
 *     http://www.opennms.com/
 *******************************************************************************/

package org.opennms.netmgt.clustering;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.opennms.core.utils.ThreadCategory;

/**
 * Leader selector which uses a distributed lock provided by Hazelcast
 * to elect a leader.
 * 
 * When the lock is obtained, the listener is invoked. When the listener
 * function returns the lock is released and made available for another
 * candidate to secure.
 *
 * @author jwhite
 *
 */
public class LeaderSelector implements Runnable {

    /**
     * The listener we should invoke when we obtain the leader lock.
     */
    private LeaderSelectorListener m_listener;

    /**
     * Thread for acquiring the lock.
     */
    private Thread m_thread = null;
 
    /**
     * Flag used to notify the thread to return.
     */
    private boolean m_stopped = true;

    /**
     * Unique ID for the leader lock.
     */
    public static final String LEADER_LOCK_ID = "org.opennms.netmgt.clustering.LeaderSelector.LOCK";

    public LeaderSelector(LeaderSelectorListener listener) {
        m_listener = listener;
    }

    public void start() {
        m_stopped = false;
        Thread m_thread = new Thread(this);
        m_thread.start();
    }

    public void stop() {
        m_stopped = true;
        m_thread.interrupt();
    }

    @Override
    public void run() {
        // FIXME: Initialization of Hazelcast instances should probably go elsewhere
        HazelcastInstance hazelcastInstance;

        // Only create a single instance of Hazelcast at a time
        synchronized(LEADER_LOCK_ID) {
            hazelcastInstance = Hazelcast.newHazelcastInstance();
        }

        // FIXME: We may want to abstract the Hazelcast functions and move these to some core package
        Lock lock = hazelcastInstance.getLock(LEADER_LOCK_ID);
        log().debug("Waiting for leader lock...");
        while (true) {
            try {
                if (lock.tryLock(500, TimeUnit.MILLISECONDS)) {
                    try {
                        log().debug("Got leader lock!");
                        m_listener.takeLeadership();
                    } finally {
                        lock.unlock();
                    }
                }

                if (m_stopped) {
                    log().debug("Stopping...");
                    break;
                }
            } catch (InterruptedException e) {
                log().info("Interrupted...", e);
                break;
            }
        }
    }

    private ThreadCategory log() {
        return ThreadCategory.getInstance(getClass());
    }
}
