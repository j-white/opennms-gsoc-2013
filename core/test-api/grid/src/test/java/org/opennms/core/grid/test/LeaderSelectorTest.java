/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2007-2011 The OpenNMS Group, Inc.
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

package org.opennms.core.grid.test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.junit.Test;
import org.opennms.core.grid.LeaderSelector;
import org.opennms.core.grid.LeaderSelectorListener;
import org.opennms.core.test.grid.GridTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.jayway.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.*;

/**
 * LeaderSelectorIntegrationTest
 * 
 * @author jwhite
 */
public class LeaderSelectorTest extends GridTest {
    /**
     * Used to prevent any clients from gaining or relinquishing leadership so
     * that we can very only a single leader is elected.
     */
    private Object leaderLock = new Object();

    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(LeaderSelectorTest.class);

    /**
     * A cluster client that tries to become leader and maintains the leader
     * state for a random amount of time.
     * 
     * @author jwhite
     * 
     */
    private class ClusterClient implements LeaderSelectorListener {
        private String m_id;
        private int m_leaderCount = 0;
        private boolean m_isLeader = false;
        private LeaderSelector m_leaderSelector;

        public ClusterClient(String id) {
            m_id = id;
            m_leaderSelector = new LeaderSelector("test", this, m_dataGridProvider);
        }

        public void start() {
            m_leaderSelector.start();
        }

        public void stop() {
            m_leaderSelector.stop();
        }

        @Override
        public void takeLeadership() {
            synchronized (leaderLock) {
                m_isLeader = true;
            }
            LOG.debug(m_id + " was elected leader for the "
                                + ++m_leaderCount + "th time.");

            int waitMillis = (int) (500 * Math.random()) + 500;
            LOG.debug(m_id + " sleeping for " + waitMillis + " ms");

            try {
                Thread.sleep(waitMillis);
            } catch (InterruptedException e) {
                LOG.error(m_id + " was interrupted.");
                Thread.currentThread().interrupt();
            } finally {
                synchronized (leaderLock) {
                    m_isLeader = false;
                }
            }
        }

        public boolean isLeader() {
            return m_isLeader;
        }
    }

    /**
     * Verifies that the leader selector actually elects a leader.
     * 
     * @throws Exception
     *             Some error occurred
     */
    @Test
    public void leaderElection() throws Exception {
        List<ClusterClient> clients = new ArrayList<ClusterClient>(
                                                                   N_MEMBERS);
        assertFalse(isLeaderActive(clients).call());

        // Spawn N clients
        for (int i = 1; i <= N_MEMBERS; i++) {
            ClusterClient clusterClient = new ClusterClient("n" + i);
            clusterClient.start();
            clients.add(clusterClient);
        }

        for (ClusterClient client : clients) {
            // Wait until a leader gets elected
            await().until(isLeaderActive(clients));

            // We know we have a leader now - let's make sure there's only one
            assertEquals(1, getNumActiveLeaders(clients));

            // Stop one of the clients
            client.stop();
        }

        await().until(isLeaderActive(clients), is(false));
    }

    private int getNumActiveLeaders(final List<ClusterClient> clients) {
        int i = 0;
        for (ClusterClient client : clients) {
            if (client.isLeader()) {
                i++;
            }
        }
        return i;
    }

    private Callable<Boolean> isLeaderActive(final List<ClusterClient> clients) {
        return new Callable<Boolean>() {
            public Boolean call() throws Exception {
                return getNumActiveLeaders(clients) > 0;
            }
        };
    }
}
