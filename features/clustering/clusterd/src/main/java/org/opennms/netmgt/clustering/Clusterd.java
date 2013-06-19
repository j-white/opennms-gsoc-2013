/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2008-2013 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2011 The OpenNMS Group, Inc.
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

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.opennms.core.utils.ThreadCategory;
import org.opennms.netmgt.config.ServiceConfigFactory;
import org.opennms.netmgt.config.service.Service;
import org.opennms.netmgt.config.service.types.ServiceType;
import org.opennms.netmgt.daemon.AbstractServiceDaemon;
import org.opennms.netmgt.vmmgr.ServiceManager;

/**
 * The Clusterd daemon is responsible for electing a leader in the cluster and
 * having the leader run services which can not yet be distributed.
 * 
 * @author jwhite
 */
public class Clusterd extends AbstractServiceDaemon implements
        LeaderSelectorListener {
    private static final String DAEMON_NAME = "Clusterd";

    /**
     * The leader selector process used to elect a leader.
     */
    private LeaderSelector m_leaderSelector = null;

    /**
     * Thread handle to our leader handler. Only when we are the leader.
     */
    private Thread m_leaderThread = null;

    /**
     * Used to notify the leader thread we are stopping.
     */
    private boolean m_stopped = true;

    /**
     * Used to start the services.
     */
    private ServiceManager m_serviceManager = new ServiceManager();

    public Clusterd() {
        super("OpenNMS." + DAEMON_NAME);
    }

    @Override
    protected void onInit() {
        try {
            ServiceConfigFactory.init();
        } catch (IOException e) {
            throw new RuntimeException(
                                       "Failed to load the service configuration.",
                                       e);
        }

        m_leaderSelector = new LeaderSelector(this);
    }

    @Override
    protected void onStart() {
        log().debug("Starting the leader selector.");
        m_stopped = false;
        m_leaderSelector.start();
    }

    @Override
    protected void onStop() {
        log().debug("Stopping the leader selector.");
        m_leaderSelector.stop();
        if (m_leaderThread != null) {
            m_leaderThread.interrupt();
        }
        m_stopped = true;
    }

    @Override
    public void takeLeadership() {
        log().info("Node was elected master.");

        // Get the list of vanilla services that are not running
        List<Service> vanillaServices = ServiceConfigFactory.getInstance().getServicesOfType(ServiceType.VANILLA);
        List<Service> servicesToStart = new LinkedList<Service>();
        for (Service svc : vanillaServices) {
            if (!m_serviceManager.isActive(svc)) {
                servicesToStart.add(svc);
            }
        }

        // Start those who need starting
        m_serviceManager.start(servicesToStart);

        // Wait until we're interrupted
        try {
            while(true) {
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            // Reset the interrupted flag
            Thread.interrupted();
        } finally {
            log().error("Relinquishing leadership.");
            m_leaderThread = null;
        }
    }

    public ThreadCategory log() {
        return ThreadCategory.getInstance(getClass());
    }
}
