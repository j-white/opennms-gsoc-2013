/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2007-2013 The OpenNMS Group, Inc.
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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.opennms.core.grid.DataGridProvider;
import org.opennms.core.grid.HazelcastDataGridProvider;

import org.opennms.core.utils.ThreadCategory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Required;

/**
 * This class is used to help select a leader amongst the cluster members:
 * 
 * A distributed lock obtained via the default grid provider.
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
     * Listener we should invoke when we obtain the leader lock.
     */
    private LeaderSelectorListener m_listener = null;

    /**
     * Used to obtain the distributed lock.
     */
    @Autowired
    private DataGridProvider m_dataGridProvider = new HazelcastDataGridProvider();

    /**
     * Thread used to acquire the lock.
     */
    private Thread m_thread = null;

    /**
     * Flag used to notify the thread to return.
     */
    private boolean m_stopped = true;

    /**
     * Number of milliseconds used to wait for the lock before checking the
     * stopped flag.
     */
    public static final int LOCK_WAIT_MS = 500;

    /**
     * Unique ID for the leader lock.
     */
    public static final String LEADER_LOCK_ID = "org.opennms.netmgt.clustering.LeaderSelector.LOCK";

    /**
     * Default constructor.
     */
    public LeaderSelector() {
        // This method is intentionally left blank
    }

    public LeaderSelector(LeaderSelectorListener listener) {
        setListener(listener);
    }

    public LeaderSelector(LeaderSelectorListener listener,
            DataGridProvider dataGridProvider) {
        setListener(listener);
        setDataGridProvider(dataGridProvider);
    }

    public void start() {
        assert (m_dataGridProvider != null);
        assert (m_listener != null);
        m_stopped = false;
        Thread m_thread = new Thread(this);
        m_thread.start();
    }

    public void stop() {
        m_stopped = true;
        if (m_thread != null) {
            m_thread.interrupt();
        }
    }

    @Override
    public void run() {
        log().debug("Using distributed lock from " + m_dataGridProvider);
        Lock lock = m_dataGridProvider.getLock(LEADER_LOCK_ID);

        log().debug("Waiting for leader lock...");
        while (true) {
            try {
                if (lock.tryLock(LOCK_WAIT_MS, TimeUnit.MILLISECONDS)) {
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

    public LeaderSelectorListener getListener() {
        return m_listener;
    }

    @Required
    public void setListener(LeaderSelectorListener listener) {
        m_listener = listener;
    }

    public DataGridProvider getDataGridProvider() {
        return m_dataGridProvider;
    }

    public void setDataGridProvider(DataGridProvider dataGridProvider) {
        m_dataGridProvider = dataGridProvider;
    }

    private ThreadCategory log() {
        return ThreadCategory.getInstance(getClass());
    }
}
