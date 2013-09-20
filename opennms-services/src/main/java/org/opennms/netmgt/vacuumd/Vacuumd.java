/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2006-2012 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2012 The OpenNMS Group, Inc.
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

package org.opennms.netmgt.vacuumd;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import org.exolab.castor.xml.MarshalException;
import org.exolab.castor.xml.ValidationException;
import org.opennms.core.db.DataSourceFactory;
import org.opennms.core.grid.DataGridProvider;
import org.opennms.core.grid.DataGridProviderFactory;
import org.opennms.netmgt.EventConstants;
import org.opennms.netmgt.config.VacuumdConfigDao;
import org.opennms.netmgt.config.VacuumdConfigFactory;
import org.opennms.netmgt.config.vacuumd.Action;
import org.opennms.netmgt.config.vacuumd.Automation;
import org.opennms.netmgt.config.vacuumd.Trigger;
import org.opennms.netmgt.daemon.AbstractServiceDaemon;
import org.opennms.netmgt.model.events.EventBuilder;
import org.opennms.netmgt.model.events.EventIpcManager;
import org.opennms.netmgt.model.events.EventListener;
import org.opennms.netmgt.scheduler.DistributedScheduler;
import org.opennms.netmgt.scheduler.Scheduler;
import org.opennms.netmgt.xml.event.Event;
import org.opennms.netmgt.xml.event.Parm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a daemon whose job it is to run periodic updates against the
 * database for database maintenance work.
 * 
 * @author <a href=mailto:brozow@opennms.org>Mathew Brozowski</a>
 * @author <a href=mailto:david@opennms.org>David Hustace</a>
 * @author <a href=mailto:dj@opennms.org>DJ Gregor</a>
 */
public class Vacuumd extends AbstractServiceDaemon implements EventListener {

    private static final Logger LOG = LoggerFactory.getLogger(Vacuumd.class);

    private static final String DAEMON_NAME = "vacuumd";

    private static volatile Vacuumd m_singleton;

    private volatile Scheduler m_scheduler;

    private volatile EventIpcManager m_eventMgr;

    private DataGridProvider m_dataGridProvider;

    private Map<String, Object> m_sharedMap;

    private Lock m_lock;

    /**
     * <p>
     * getSingleton
     * </p>
     * 
     * @return a {@link org.opennms.netmgt.vacuumd.Vacuumd} object.
     */
    public synchronized static Vacuumd getSingleton() {
        if (m_singleton == null) {
            m_singleton = new Vacuumd();
        }
        return m_singleton;
    }

    /**
     * <p>
     * Constructor for Vacuumd.
     * </p>
     */
    public Vacuumd() {
        super(DAEMON_NAME);
    }

    /** {@inheritDoc} */
    @Override
    protected void onInit() {
        try {
            if (m_dataGridProvider == null) {
                m_dataGridProvider = DataGridProviderFactory.getInstance();
            }
            m_sharedMap = m_dataGridProvider.getMap(DAEMON_NAME + "Map");
            m_lock = m_dataGridProvider.getLock(DAEMON_NAME + "Lock");

            LOG.info("Loading the configuration file.");
            VacuumdConfigFactory.init();

            getEventManager().addEventListener(this,
                                               EventConstants.RELOAD_VACUUMD_CONFIG_UEI);
            getEventManager().addEventListener(this,
                                               EventConstants.RELOAD_DAEMON_CONFIG_UEI);

            initializeDataSources();

        } catch (Throwable ex) {
            LOG.error("Failed to load vacuumd configuration", ex);
            throw new UndeclaredThrowableException(ex);
        }

        LOG.info("Vacuumd initialization complete");

        createScheduler();
        safelyScheduleAutomations();
    }

    private void initializeDataSources() throws MarshalException,
            ValidationException, IOException, ClassNotFoundException,
            PropertyVetoException, SQLException {
        for (Trigger trigger : getVacuumdConfig().getTriggers()) {
            DataSourceFactory.init(trigger.getDataSource());
        }

        for (Action action : getVacuumdConfig().getActions()) {
            DataSourceFactory.init(action.getDataSource());
        }
    }

    /** {@inheritDoc} */
    @Override
    protected void onStart() {
        m_scheduler.start();
    }

    /** {@inheritDoc} */
    @Override
    protected void onStop() {
        if (m_scheduler != null) {
            m_scheduler.stop();
        }
    }

    /** {@inheritDoc} */
    @Override
    protected void onPause() {
        m_scheduler.pause();
    }

    /** {@inheritDoc} */
    @Override
    protected void onResume() {
        m_scheduler.resume();
    }

    private void createScheduler() {
        try {
            LOG.debug("Creating Vacuumd scheduler");
            m_scheduler = new DistributedScheduler(DAEMON_NAME, 2);
        } catch (RuntimeException e) {
            LOG.error("Failed to create Vacuumd scheduler", e);
            throw e;
        }
    }

    /**
     * <p>
     * getScheduler
     * </p>
     * 
     * @return a {@link org.opennms.netmgt.scheduler.Scheduler} object.
     */
    public Scheduler getScheduler() {
        return m_scheduler;
    }

    /**
     * Does not schedule the automations if another instance already has.
     */
    private void safelyScheduleAutomations() {
        m_lock.lock();
        try {
            String currentConfigId = getVacuumdConfig().getUniqueId();
            String activeConfigId = (String) m_sharedMap.get("configId");

            LOG.debug("Current config hash: {} vs active config hash: {}",
                      currentConfigId, activeConfigId);
            if (!currentConfigId.equals(activeConfigId)) {
                LOG.debug("Resetting the scheduler.");
                m_scheduler.reset();

                scheduleAutomations();
                m_sharedMap.put("configId", currentConfigId);
            } else {
                LOG.info("Automations are already scheduled.");
            }
        } finally {
            m_lock.unlock();
        }
    }

    private void scheduleAutomations() {
        LOG.info("Scheduling {} automations.",
                 getVacuumdConfig().getAutomations().size());
        for (Automation auto : getVacuumdConfig().getAutomations()) {
            scheduleAutomation(auto);
        }
    }

    private void scheduleAutomation(Automation auto) {
        if (auto.getActive()) {
            AutomationProcessor ap = new AutomationProcessor(auto);
            m_scheduler.schedule(ap.getInterval(), ap);
        }
    }

    /**
     * <p>
     * getEventManager
     * </p>
     * 
     * @return a {@link org.opennms.netmgt.model.events.EventIpcManager}
     *         object.
     */
    public EventIpcManager getEventManager() {
        return m_eventMgr;
    }

    /**
     * <p>
     * setEventManager
     * </p>
     * 
     * @param eventMgr
     *            a {@link org.opennms.netmgt.model.events.EventIpcManager}
     *            object.
     */
    public void setEventManager(EventIpcManager eventMgr) {
        m_eventMgr = eventMgr;
    }

    /** {@inheritDoc} */
    @Override
    public void onEvent(Event event) {
        if (isReloadConfigEvent(event)) {
            handleReloadConfigEvent();
        }
    }

    private void handleReloadConfigEvent() {
        LOG.info("Reloading configuration...");

        EventBuilder ebldr = null;

        try {
            LOG.debug("Number of elements currently scheduled: {}",
                      m_scheduler.getScheduled());

            LOG.debug("Reloading vacuumd configuration.");
            VacuumdConfigFactory.reload();

            LOG.debug("Initializing the data sources...");
            initializeDataSources();

            LOG.debug("Rescheduling the automations...");
            safelyScheduleAutomations();

            LOG.debug("Number of automations currently scheduled: {}",
                      m_scheduler.getScheduled());
            ebldr = new EventBuilder(
                                     EventConstants.RELOAD_DAEMON_CONFIG_SUCCESSFUL_UEI,
                                     getName());
            ebldr.addParam(EventConstants.PARM_DAEMON_NAME, DAEMON_NAME);
        } catch (IOException e) {
            LOG.error("IO problem reading vacuumd configuration", e);
            ebldr = new EventBuilder(
                                     EventConstants.RELOAD_DAEMON_CONFIG_FAILED_UEI,
                                     getName());
            ebldr.addParam(EventConstants.PARM_DAEMON_NAME, DAEMON_NAME);
            ebldr.addParam(EventConstants.PARM_REASON,
                           e.getLocalizedMessage().substring(0, 128));
        } catch (Throwable e) {
            LOG.error("Failed to load vacuumd configuration", e);
            ebldr = new EventBuilder(
                                     EventConstants.RELOAD_DAEMON_CONFIG_FAILED_UEI,
                                     getName());
            ebldr.addParam(EventConstants.PARM_DAEMON_NAME, DAEMON_NAME);
            ebldr.addParam(EventConstants.PARM_REASON,
                           e.getLocalizedMessage().substring(0, 128));
        }

        LOG.info("Configuration reload complete.");

        if (ebldr != null) {
            m_eventMgr.sendNow(ebldr.getEvent());
        }
    }

    private boolean isReloadConfigEvent(Event event) {
        boolean isTarget = false;

        if (EventConstants.RELOAD_DAEMON_CONFIG_UEI.equals(event.getUei())) {
            List<Parm> parmCollection = event.getParmCollection();

            for (Parm parm : parmCollection) {
                if (EventConstants.PARM_DAEMON_NAME.equals(parm.getParmName())
                        && DAEMON_NAME.equalsIgnoreCase(parm.getValue().getContent())) {
                    isTarget = true;
                    break;
                }
            }

            // Depreciating this one...
        } else if (EventConstants.RELOAD_VACUUMD_CONFIG_UEI.equals(event.getUei())) {
            isTarget = true;
        }

        return isTarget;
    }

    private VacuumdConfigDao getVacuumdConfig() {
        return VacuumdConfigFactory.getInstance();
    }

    public long getNumAutomationsGlobal() {
        return m_scheduler.getGlobalTasksExecuted();
    }

    public long getNumAutomationsLocal() {
        return m_scheduler.getLocalTasksExecuted();
    }

    public void setDataGridProvider(DataGridProvider dataGridProvider) {
        m_dataGridProvider = dataGridProvider;
    }

    public DataGridProvider getDataGridProvider() {
        return m_dataGridProvider;
    }
}
