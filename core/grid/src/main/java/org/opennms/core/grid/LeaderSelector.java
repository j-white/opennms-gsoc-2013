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

package org.opennms.core.grid;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.util.Assert;

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
     * Unique ID for the shared lock.
     */
    private final String m_leaderLockId;

    /**
     * Used to obtain the distributed lock.
     */
    @Autowired
    private DataGridProvider m_dataGridProvider = null;

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
     * Prefix for the shared lock.
     */
    public static final String LEADER_LOCK_ID_PREFIX = "org.opennms.core.grid.LeaderSelector.LOCK.";

    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(LeaderSelector.class);

    public LeaderSelector(String id) {
        m_leaderLockId = LEADER_LOCK_ID_PREFIX + id;
    }

    public LeaderSelector(String id, LeaderSelectorListener listener) {
        m_leaderLockId = LEADER_LOCK_ID_PREFIX + id;
        setListener(listener);
    }

    public LeaderSelector(String id, LeaderSelectorListener listener,
            DataGridProvider dataGridProvider) {
        m_leaderLockId = LEADER_LOCK_ID_PREFIX + id;
        setListener(listener);
        setDataGridProvider(dataGridProvider);
    }

    public void start() {
        Assert.isTrue(m_listener != null, "Listener not set");
        Assert.isTrue(m_dataGridProvider != null, "Data grid provider not set");

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
        LOG.debug("Using distributed lock called {} from {}.", m_leaderLockId, m_dataGridProvider);
        Lock lock = m_dataGridProvider.getLock(m_leaderLockId);

        LOG.debug("Waiting for leader lock...");
        while (true) {
            try {
                if (lock.tryLock(LOCK_WAIT_MS, TimeUnit.MILLISECONDS)) {
                    try {
                        LOG.debug("Got leader lock!");
                        m_listener.takeLeadership();
                    } finally {
                        lock.unlock();
                    }
                }

                if (m_stopped) {
                    LOG.debug("Stopping...");
                    break;
                }
            } catch (InterruptedException e) {
                LOG.info("Interrupted...", e);
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
}
