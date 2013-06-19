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

package org.opennms.netmgt.clustering;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opennms.core.test.MockLogAppender;
import org.opennms.core.utils.ThreadCategory;

import com.hazelcast.core.Hazelcast;

import static com.jayway.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertFalse;

/**
 * Verify that the leader selector actually elects a leader.
 * 
 * TODO: Verify that only a single leader is elected at any given time.
 * 
 * @author jwhite
 */
public class LeaderSelectorIntegrationTest {
    private final static int NUMBER_OF_CLIENTS = 3;

    @Before
    public void setUp() {
        MockLogAppender.setupLogging(true, "ERROR");

        System.setProperty("hazelcast.logging.type", "log4j");
        Hazelcast.shutdownAll();
    }

    @After
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }

    private class ClusterClient implements LeaderSelectorListener {
        private String m_id;
        private int m_leaderCount = 0;
        private boolean m_isLeader = false;
        private LeaderSelector m_leaderSelector;

        public ClusterClient(String id) {
            m_id = id;
            m_leaderSelector = new LeaderSelector(this);
        }

        public void start() {
            m_leaderSelector.start();
        }

        public void stop() {
            m_leaderSelector.stop();
        }

        @Override
        public void takeLeadership() {
            m_isLeader = true;
            log().debug(m_id + " was elected leader for the "
                                + ++m_leaderCount + "th time.");

            int waitMillis = (int) (500 * Math.random()) + 500;
            log().debug(m_id + " sleeping for " + waitMillis + " ms");

            try {
                Thread.sleep(waitMillis);
            } catch (InterruptedException e) {
                log().error(m_id + " was interrupted.");
                Thread.currentThread().interrupt();
            } finally {
                m_isLeader = false;
            }

        }

        public boolean isLeader() {
            return m_isLeader;
        }

        private ThreadCategory log() {
            return ThreadCategory.getInstance(getClass());
        }
    }

    @Test
    public void leaderElection() throws Exception {
        List<ClusterClient> clients = new ArrayList<ClusterClient>(
                                                                   NUMBER_OF_CLIENTS);
        assertFalse(isLeaderActive(clients).call());

        // Spawn N clients
        for (int i = 1; i <= NUMBER_OF_CLIENTS; i++) {
            ClusterClient clusterClient = new ClusterClient("n" + i);
            clusterClient.start();
            clients.add(clusterClient);
        }

        // Wait until a client gets elected leader and
        // stop the clients one by one
        for (ClusterClient client : clients) {
            await().until(isLeaderActive(clients));
            client.stop();
        }

        await().until(isLeaderActive(clients), is(false));
    }

    private Callable<Boolean> isLeaderActive(final List<ClusterClient> clients) {
        return new Callable<Boolean>() {
            public Boolean call() throws Exception {
                for (ClusterClient client : clients) {
                    if (client.isLeader()) {
                        return true;
                    }
                }
                return false;
            }
        };
    }

    public ThreadCategory log() {
        return ThreadCategory.getInstance(getClass());
    }
}
