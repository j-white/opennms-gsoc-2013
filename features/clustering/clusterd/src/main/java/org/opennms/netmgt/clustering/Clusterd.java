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

import org.opennms.netmgt.config.ServiceConfigDao;
import org.opennms.netmgt.config.ServiceConfigFactory;
import org.opennms.netmgt.config.service.Service;
import org.opennms.netmgt.config.service.types.ServiceType;
import org.opennms.netmgt.daemon.AbstractServiceDaemon;
import org.opennms.netmgt.vmmgr.ServiceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.util.Assert;

/**
 * Clusterd is responsible for electing a leader amongst the cluster members.
 * 
 * When a member is elected leader it will: -Start all of the vanilla services
 * on the local JVM
 * 
 * Only one member may be leader at any given time.
 * 
 * @author jwhite
 */
public class Clusterd extends AbstractServiceDaemon implements
        LeaderSelectorListener {
    /**
     * Log4j category
     */
    private final static String LOG4J_CATEGORY = "clusterd";

    /**
     * Used to perform leader election.
     */
    private LeaderSelector m_leaderSelector;

    /**
     * Set to to the thread handle on which the takeLeadership() function is
     * being run. Only set when we are the leader.
     */
    private Thread m_leaderThread = null;

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

    /**
     * Number of milliseconds used to sleep before checking the stopped flag
     * when leader.
     */
    public static final int LEADER_SLEEP_MS = 500;

    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(Clusterd.class);

    /**
     * Default constructor.
     */
    public Clusterd() {
        super(LOG4J_CATEGORY);
    }

    /** {@inheritDoc} */
    @Override
    protected void onInit() {
        if (m_serviceConfigDao == null) {
            try {
                ServiceConfigFactory.init();
                m_serviceConfigDao = ServiceConfigFactory.getInstance();
            } catch (IOException e) {
                LOG.error("Failed to load the service configuration.", e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    protected void onStart() {
        Assert.state(m_serviceConfigDao != null, "serviceConfigDao must be set");
        Assert.state(m_leaderSelector != null, "leaderSelector must be set");
        Assert.state(m_serviceManager != null, "serviceManager must be set");
        LOG.debug("Starting the leader selector.");
        m_stopped = false;
        m_leaderSelector.start();
    }

    /** {@inheritDoc} */
    @Override
    protected void onStop() {
        LOG.debug("Stopping the leader selector.");
        m_leaderSelector.stop();
        if (m_leaderThread != null) {
            m_leaderThread.interrupt();
        }
        m_stopped = true;
    }

    /**
     * Starts all of the vanilla services when leadership is acquired.
     * 
     * Services are started in the same order as they appear in the
     * configuration file.
     * 
     * If an exception occurs while starting any of the services a call to
     * Manager.doSystemExit() will be made, and another node will have the
     * chance to become leader.
     * 
     */
    @Override
    public void takeLeadership() {
        LOG.info("Node was elected leader.");

        // Get the list of vanilla services that are not running
        List<Service> vanillaServices = m_serviceConfigDao.getServicesOfType(ServiceType.VANILLA);
        List<Service> servicesToStart = new LinkedList<Service>();
        for (Service svc : vanillaServices) {
            if (!m_serviceManager.isStarted(svc)) {
                servicesToStart.add(svc);
            }
        }

        // Start those who need starting
        LOG.debug("Starting " + servicesToStart.size() + " services.");
        m_serviceManager.start(servicesToStart);

        LOG.debug("Done starting the services. Keeping leadership until we're stopped.");
        try {
            while (true) {
                Thread.sleep(LEADER_SLEEP_MS);
                if (m_stopped) {
                    break;
                }
            }
        } catch (InterruptedException e) {
            // Reset the interrupted flag
            Thread.interrupted();
        } finally {
            LOG.info("Relinquishing leadership.");
            m_leaderThread = null;
        }
    }

    /**
     * <p>
     * getLeaderSelector
     * </p>
     */
    public LeaderSelector getLeaderSelector() {
        return m_leaderSelector;
    }

    /**
     * <p>
     * setLeaderSelector
     * </p>
     */
    @Required
    public void setLeaderSelector(LeaderSelector leaderSelector) {
        if (!m_stopped) {
            throw new RuntimeException("The leader selector can only be set "
                    + "when the service is stopped.");
        }
        m_leaderSelector = leaderSelector;
    }

    /**
     * <p>
     * getServiceConfigDao
     * </p>
     */
    public ServiceConfigDao getServiceConfigDao() {
        return m_serviceConfigDao;
    }

    /**
     * <p>
     * setServiceConfigDao
     * </p>
     */
    public void setServiceConfigDao(ServiceConfigDao serviceConfigDao) {
        m_serviceConfigDao = serviceConfigDao;
    }

    /**
     * <p>
     * getServiceManager
     * </p>
     */
    public ServiceManager getServiceManager() {
        return m_serviceManager;
    }

    /**
     * <p>
     * setServiceManager
     * </p>
     */
    public void setServiceManager(ServiceManager serviceManager) {
        m_serviceManager = serviceManager;
    }

    /**
     * <p>
     * getLoggingCategory
     * </p>
     */
    public static String getLoggingCategory() {
        return LOG4J_CATEGORY;
    }
}
