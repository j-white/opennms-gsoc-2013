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
import java.util.concurrent.ExecutorService;

import org.exolab.castor.xml.MarshalException;
import org.exolab.castor.xml.ValidationException;
import org.opennms.core.db.DataSourceFactory;
import org.opennms.netmgt.EventConstants;
import org.opennms.netmgt.config.VacuumdConfigFactory;
import org.opennms.netmgt.config.vacuumd.Action;
import org.opennms.netmgt.config.vacuumd.Automation;
import org.opennms.netmgt.config.vacuumd.Trigger;
import org.opennms.netmgt.daemon.AbstractServiceDaemon;
import org.opennms.netmgt.model.events.EventBuilder;
import org.opennms.netmgt.model.events.EventIpcManager;
import org.opennms.netmgt.model.events.EventListener;
import org.opennms.netmgt.scheduler.LegacyScheduler;
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
    
    private static volatile Vacuumd m_singleton;

    private volatile LegacyScheduler m_scheduler;

    private volatile EventIpcManager m_eventMgr;

    private volatile long m_numAutomationsRan = 0;

    /**
     * <p>getSingleton</p>
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
     * <p>Constructor for Vacuumd.</p>
     */
    public Vacuumd() {
        super("vacuumd");
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.opennms.netmgt.vacuumd.jmx.VacuumdMBean#init()
     */
    /** {@inheritDoc} */
    @Override
    protected void onInit() {
        try {
            LOG.info("Loading the configuration file.");
            VacuumdConfigFactory.init();
            getEventManager().addEventListener(this, EventConstants.RELOAD_VACUUMD_CONFIG_UEI);
            getEventManager().addEventListener(this, EventConstants.RELOAD_DAEMON_CONFIG_UEI);

            initializeDataSources();
        } catch (Throwable ex) {
            LOG.error("Failed to load outage configuration", ex);
            throw new UndeclaredThrowableException(ex);
        }

        LOG.info("Vacuumd initialization complete");

        createScheduler();
        scheduleAutomations();
    }

    private void initializeDataSources() throws MarshalException, ValidationException, IOException, ClassNotFoundException, PropertyVetoException, SQLException {
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
<<<<<<< HEAD
        m_stopped = true;
        if (m_scheduler != null) {
            m_scheduler.stop();
        }
=======
        m_scheduler.stop();
>>>>>>> Vacuumd refactoring: convert statements to automations and auto-events to action-events.
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
            LOG.debug("init: Creating Vacuumd scheduler");
            m_scheduler = new LegacyScheduler("Vacuumd", 2);
        } catch (RuntimeException e) {
            LOG.error("init: Failed to create Vacuumd scheduler", e);
            throw e;
        }
    }

    /**
     * <p>getScheduler</p>
     *
     * @return a {@link org.opennms.netmgt.scheduler.Scheduler} object.
     */
    public Scheduler getScheduler() {
        return m_scheduler;
    }

    private void scheduleAutomations() {
        for (Automation auto : getVacuumdConfig().getAutomations()) {
            scheduleAutomation(auto);
        }
    }

    private void scheduleAutomation(Automation auto) {
        if (auto.getActive()) {
            AutomationProcessor ap = new AutomationProcessor(auto);
            ap.setScheduler(m_scheduler);
            ap.schedule(false);
        }
    }

    /**
     * <p>getEventManager</p>
     *
     * @return a {@link org.opennms.netmgt.model.events.EventIpcManager} object.
     */
    public EventIpcManager getEventManager() {
        return m_eventMgr;
    }

    /**
     * <p>setEventManager</p>
     *
     * @param eventMgr a {@link org.opennms.netmgt.model.events.EventIpcManager} object.
     */
    public void setEventManager(EventIpcManager eventMgr) {
        m_eventMgr = eventMgr;
    }

    /** {@inheritDoc} */
    @Override
    public void onEvent(Event event) {
        
        if (isReloadConfigEvent(event)) {
            handleReloadConifgEvent();
        }
    }

    private void handleReloadConifgEvent() {
        LOG.info("onEvent: reloading configuration...");
        
        EventBuilder ebldr = null;
        
        try {
            LOG.debug("onEvent: Number of elements in schedule:{}; calling stop on scheduler...", m_scheduler.getScheduled());
            stop();
            ExecutorService runner = m_scheduler.getRunner();
            while (!runner.isShutdown() || m_scheduler.getStatus() != STOPPED) {
                LOG.debug("onEvent: waiting for scheduler to stop. Current status of scheduler: {}; Current status of runner: {}", m_scheduler.getStatus(), (runner.isTerminated() ? "TERMINATED" : (runner.isShutdown() ? "SHUTDOWN" : "RUNNING")));
                Thread.sleep(500);
            }
            LOG.debug("onEvent: Current status of scheduler: {}; Current status of runner: {}", m_scheduler.getStatus(), (runner.isTerminated() ? "TERMINATED" : (runner.isShutdown() ? "SHUTDOWN" : "RUNNING")));
            LOG.debug("onEvent: Number of elements in schedule: {}", m_scheduler.getScheduled());
            LOG.debug("onEvent: reloading vacuumd configuration.");

            VacuumdConfigFactory.reload();
            LOG.debug("onEvent: creating new schedule and rescheduling automations.");

            init();
            LOG.debug("onEvent: restarting vacuumd and scheduler.");

            start();
            LOG.debug("onEvent: Number of elements in schedule: {}", m_scheduler.getScheduled());
            
            ebldr = new EventBuilder(EventConstants.RELOAD_DAEMON_CONFIG_SUCCESSFUL_UEI, getName());
            ebldr.addParam(EventConstants.PARM_DAEMON_NAME, "Vacuumd");
        } catch (IOException e) {
            LOG.error("onEvent: IO problem reading vacuumd configuration", e);
            ebldr = new EventBuilder(EventConstants.RELOAD_DAEMON_CONFIG_FAILED_UEI, getName());
            ebldr.addParam(EventConstants.PARM_DAEMON_NAME, "Vacuumd");
            ebldr.addParam(EventConstants.PARM_REASON, e.getLocalizedMessage().substring(0, 128));
        } catch (InterruptedException e) {
            LOG.error("onEvent: Problem interrupting current Vacuumd Thread", e);
            ebldr = new EventBuilder(EventConstants.RELOAD_DAEMON_CONFIG_FAILED_UEI, getName());
            ebldr.addParam(EventConstants.PARM_DAEMON_NAME, "Vacuumd");
            ebldr.addParam(EventConstants.PARM_REASON, e.getLocalizedMessage().substring(0, 128));
		}
        
        LOG.info("onEvent: completed configuration reload.");
        
        if (ebldr != null) {
            m_eventMgr.sendNow(ebldr.getEvent());
        }
    }

    private boolean isReloadConfigEvent(Event event) {
        boolean isTarget = false;
        
        if (EventConstants.RELOAD_DAEMON_CONFIG_UEI.equals(event.getUei())) {
            List<Parm> parmCollection = event.getParmCollection();
            
            for (Parm parm : parmCollection) {
                if (EventConstants.PARM_DAEMON_NAME.equals(parm.getParmName()) && "Vacuumd".equalsIgnoreCase(parm.getValue().getContent())) {
                    isTarget = true;
                    break;
                }
            }
        
        //Depreciating this one...
        } else if (EventConstants.RELOAD_VACUUMD_CONFIG_UEI.equals(event.getUei())) {
            isTarget = true;
        }
        
        return isTarget;
    }

    /**
     * Returns the number of automations that have been executed so far.
     *
     * @return the number of automations that have been executed
     */
    public long getNumAutomations() {
        if (m_scheduler != null) {
            return m_scheduler.getNumTasksExecuted();
        } else {
            return 0L;
        }
    }

    private VacuumdConfigFactory getVacuumdConfig() {
        return VacuumdConfigFactory.getInstance();
    }
    
    public long getNumAutomationsRan() {
        return m_numAutomationsRan;
    }

    protected void incNumAutomationsRan() {
        m_numAutomationsRan++;
    }
}
