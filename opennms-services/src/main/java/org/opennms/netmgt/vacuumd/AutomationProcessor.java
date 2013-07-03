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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import org.opennms.netmgt.config.VacuumdConfigDao;
import org.opennms.netmgt.config.vacuumd.ActionEvent;
import org.opennms.netmgt.config.vacuumd.Automation;
import org.opennms.netmgt.scheduler.ClusterRunnable;
import org.opennms.netmgt.scheduler.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class used to process automations configured in
 * the vacuumd-configuration.xml file.  Automations are
 * identified by a name and they reference
 * Triggers and Actions by name, as well.  Autmations also
 * have an interval attribute that determines how often
 * they run.
 *
 * @author <a href="mailto:david@opennms.org">David Hustace</a>
 * @version $Id: $
 */
public class AutomationProcessor implements ClusterRunnable {
    private static final long serialVersionUID = 3636074664366757157L;

    private static final Logger LOG = LoggerFactory.getLogger(AutomationProcessor.class);

    private Automation m_automation;
    private transient TriggerProcessor m_trigger;
    private transient ActionProcessor m_action;
    
    /** 
     * @deprecated Associate {@link Automation} objects with {@link ActionEvent} instances instead.
     */
    private transient AutoEventProcessor m_autoEvent;
    private transient ActionEventProcessor m_actionEvent;

    private transient Scheduler m_scheduler;

    /**
     * Public constructor.
     *
     * @param automation a {@link org.opennms.netmgt.config.vacuumd.Automation} object.
     */
    public AutomationProcessor(Automation automation) {
        m_automation = automation;
        init();
    }

    @SuppressWarnings("deprecation")
    private void init() {
        VacuumdConfigDao vacuumdConfigDao = Vacuumd.getSingleton().getVacuumdConfig();
        m_trigger = new TriggerProcessor(m_automation.getName(), vacuumdConfigDao.getTrigger(m_automation.getTriggerName()));
        m_action = new ActionProcessor(m_automation.getName(), vacuumdConfigDao.getAction(m_automation.getActionName()));
        m_autoEvent = new AutoEventProcessor(m_automation.getName(), vacuumdConfigDao.getAutoEvent(m_automation.getAutoEventName()));
        m_actionEvent = new ActionEventProcessor(m_automation.getName(), vacuumdConfigDao.getActionEvent(m_automation.getActionEvent()));
    }

    /**
     * <p>getAction</p>
     *
     * @return a {@link org.opennms.netmgt.vacuumd.AutomationProcessor.ActionProcessor} object.
     */
    public ActionProcessor getAction() {
        return m_action;
    }
    
    /**
     * <p>getTrigger</p>
     *
     * @return a {@link org.opennms.netmgt.vacuumd.AutomationProcessor.TriggerProcessor} object.
     */
    public TriggerProcessor getTrigger() {
        return m_trigger;
    }

    /* (non-Javadoc)
     * @see java.lang.Runnable#run()
     */
    /**
     * <p>run</p>
     */
    @Override
    public void run() {
        Date startDate = new Date();
        LOG.debug("Start Scheduled automation {}", this);

        if (getAutomation() != null) {
            try {
                runAutomation();
            } catch (SQLException e) {
                LOG.warn("Error running automation: "+getAutomation().getName()+", "+e.getMessage());
            }
        }

        LOG.debug("run: Finished automation "+m_automation.getName()+", started at "+startDate);
        Vacuumd.getSingleton().incNumAutomationsRan();
        scheduleWith(m_scheduler);
    }

    /**
     * Called by the run method to execute the sql statements
     * of triggers and actions defined for an automation.  An
     * automation may have 0 or 1 trigger and must have 1 action.
     * If the automation doesn't have a trigger than the action
     * must not contain any tokens.
     *
     * @throws java.sql.SQLException if any.
     * @return a boolean.
     */
    public boolean runAutomation() throws SQLException {
        LOG.debug("runAutomation: "+m_automation.getName()+" running...");

        if (hasTrigger()) {
            LOG.debug("runAutomation: "+m_automation.getName()+" trigger statement is: "+ m_trigger.getTriggerSQL());
        }
            
        LOG.debug("runAutomation: "+m_automation.getName()+" action statement is: "+m_action.getActionSQL());

        LOG.debug("runAutomation: Executing trigger: {}", m_automation.getTriggerName());
        
        
        Transaction.begin();
        try {
            LOG.debug("runAutomation: Processing automation: {}", m_automation.getName());

            TriggerResults results = processTrigger();
            
            boolean success = false;
            if (results.isSuccessful()) {
                success = processAction(results);
            }
            
			return success;

        } catch (Throwable e) {
        	Transaction.rollbackOnly();
            LOG.warn("runAutomation: Could not execute automation: "+m_automation.getName(), e);
            return false;
        } finally {

            LOG.debug("runAutomation: Ending processing of automation: {}", m_automation.getName());
            
            Transaction.end();         
        }

    }

    @SuppressWarnings("deprecation")
    private boolean processAction(TriggerResults triggerResults) throws SQLException {
		LOG.debug("runAutomation: running action(s)/actionEvent(s) for : {}", m_automation.getName());
		
        //Verfiy the trigger ResultSet returned the required number of rows and the required columns for the action statement
        m_action.checkForRequiredColumns(triggerResults);
        		
		if (m_action.processAction(triggerResults)) {
		    m_actionEvent.processActionEvent(triggerResults);
		    m_autoEvent.send();
		    return true;
		} else {
			return false;
		}
	}

	private TriggerResults processTrigger() throws SQLException {
		
		if (m_trigger.hasTrigger()) {
			//get a scrollable ResultSet so that we can count the rows and move back to the
            //beginning for processing.
			
            ResultSet triggerResultSet = m_trigger.runTriggerQuery();

            TriggerResults triggerResults = new TriggerResults(m_trigger, triggerResultSet, verifyRowCount(triggerResultSet));

			return triggerResults;
            
        } else {
            return new TriggerResults(m_trigger, null, true);
        }
	}

	/**
	 * <p>verifyRowCount</p>
	 *
	 * @param triggerResultSet a {@link java.sql.ResultSet} object.
	 * @return a boolean.
	 * @throws java.sql.SQLException if any.
	 */
	protected boolean verifyRowCount(ResultSet triggerResultSet) throws SQLException {
        if (!m_trigger.hasTrigger()) {
            return true;
        }
        
        
        int resultRows;
        boolean validRows = true;
        //determine if number of rows required by the trigger row-count and operator were
        //met by the trigger query, if so we'll run the action
        resultRows = countRows(triggerResultSet);
        
        int triggerRowCount = m_trigger.getTrigger().getRowCount();
        String triggerOperator = m_trigger.getTrigger().getOperator();

        LOG.debug("verifyRowCount: Verifying trigger result: "+resultRows+" is "+(triggerOperator == null ? "<null>" : triggerOperator)+" than "+triggerRowCount);

        if (!m_trigger.triggerRowCheck(triggerRowCount, triggerOperator, resultRows))
            validRows = false;

        return validRows;
    }

    /**
     * Method used to count the rows in a ResultSet.  This probably requires
     * that your ResultSet is scrollable.
     *
     * @param rs a {@link java.sql.ResultSet} object.
     * @throws java.sql.SQLException if any.
     * @return a int.
     */
    public int countRows(ResultSet rs) throws SQLException {
        if (rs == null) {
            return 0;
        }

        int rows = 0;
        while (rs.next())
            rows++;
        rs.beforeFirst();
        return rows;
    }

    /**
     * Simple helper method to determine if the targetString contains
     * any '${token}'s.
     *
     * @param targetString a {@link java.lang.String} object.
     * @return a boolean.
     */
    public boolean containsTokens(String targetString) {
        return m_action.getTokenCount(targetString) > 0;
    }

    /**
     * <p>getAutomation</p>
     *
     * @return Returns the automation.
     */
    public Automation getAutomation() {
        return m_automation;
    }
    
    /**
     * <p>isReady</p>
     *
     * @return a boolean.
     */
    @Override
    public boolean isReady() {
        return true;
    }

    private boolean hasTrigger() {
        return m_trigger.hasTrigger();
    }

    @Override
    public void setScheduler(Scheduler scheduler) {
        m_scheduler = scheduler;
    }

    public void scheduleWith(Scheduler scheduler) {
        scheduler.schedule(m_automation.getInterval(), this);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((m_automation == null) ? 0 : m_automation.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        AutomationProcessor other = (AutomationProcessor) obj;
        if (m_automation == null) {
            if (other.m_automation != null)
                return false;
        } else if (!m_automation.equals(other.m_automation))
            return false;
        return true;
    }

    private void readObject(java.io.ObjectInputStream stream)
            throws java.io.IOException, ClassNotFoundException
    {
        stream.defaultReadObject();
        // Reload the transient fields from the configuration
        init();
    }
}
