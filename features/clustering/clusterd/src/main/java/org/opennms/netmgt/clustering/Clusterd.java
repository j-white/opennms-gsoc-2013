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

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.opennms.core.utils.ThreadCategory;
import org.opennms.netmgt.config.ServiceConfigDao;
import org.opennms.netmgt.config.ServiceConfigFactory;
import org.opennms.netmgt.config.service.Service;
import org.opennms.netmgt.config.service.types.ServiceType;
import org.opennms.netmgt.daemon.AbstractServiceDaemon;
import org.opennms.netmgt.vmmgr.ServiceManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Required;

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
    private LeaderSelector m_leaderSelector;

    /**
     * Thread handle to our leader handler. Only when we are the leader.
     */
    private Thread m_leaderThread;

    /**
     * Used to retrieve the list of services that are not automatically
     * started by the manager.
     */
    private ServiceConfigDao m_serviceConfigDao = null;

    /**
     * Used to start the services.
     */
    @Autowired
    private ServiceManager m_serviceManager;

    /**
     * Used to notify the leader thread we are stopping.
     */
    private boolean m_stopped = true;

    public Clusterd() {
        super("OpenNMS." + DAEMON_NAME);
    }

    @Override
    protected void onInit() {
        if (m_serviceConfigDao == null) {
            try {
                ServiceConfigFactory.init();
            } catch (IOException e) {
                throw new RuntimeException(
                                           "Failed to load the service configuration.",
                                           e);
            }
            m_serviceConfigDao = ServiceConfigFactory.getInstance();
        }
    }

    @Override
    protected void onStart() {
        assert (m_leaderSelector != null);
        assert (m_serviceManager != null);
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
        log().info("Node was elected leader.");

        // Get the list of vanilla services that are not running
        List<Service> vanillaServices = m_serviceConfigDao.getServicesOfType(ServiceType.VANILLA);
        List<Service> servicesToStart = new LinkedList<Service>();
        for (Service svc : vanillaServices) {
            if (!m_serviceManager.isStarted(svc)) {
                servicesToStart.add(svc);
            }
        }

        // Start those who need starting
        log().debug("Starting " + servicesToStart.size() + " services.");
        m_serviceManager.start(servicesToStart);

        log().debug("Done starting the services. Keeping leadership until we're stopped.");
        try {
            while (true) {
                Thread.sleep(1000);
                if (m_stopped) {
                    break;
                }
            }
        } catch (InterruptedException e) {
            // Reset the interrupted flag
            Thread.interrupted();
        } finally {
            log().info("Relinquishing leadership.");
            m_leaderThread = null;
        }
    }

    public LeaderSelector getLeaderSelector() {
        return m_leaderSelector;
    }

    @Required
    public void setLeaderSelector(LeaderSelector leaderSelector) {
        if (!m_stopped) {
            throw new RuntimeException("The leader selector can only be set "
                    + "when the service is stopped.");
        }
        m_leaderSelector = leaderSelector;
    }

    public ServiceConfigDao getServiceConfigDao() {
        return m_serviceConfigDao;
    }

    public void setServiceConfigDao(ServiceConfigDao serviceConfigDao) {
        m_serviceConfigDao = serviceConfigDao;
    }

    public ServiceManager getServiceManager() {
        return m_serviceManager;
    }

    public void setServiceManager(ServiceManager serviceManager) {
        m_serviceManager = serviceManager;
    }

    public ThreadCategory log() {
        return ThreadCategory.getInstance(getClass());
    }
}
