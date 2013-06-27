/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2013 The OpenNMS Group, Inc.
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

package org.opennms.netmgt.config;

import java.util.Collection;
import java.util.List;

import org.opennms.netmgt.config.vacuumd.Action;
import org.opennms.netmgt.config.vacuumd.ActionEvent;
import org.opennms.netmgt.config.vacuumd.AutoEvent;
import org.opennms.netmgt.config.vacuumd.Automation;
import org.opennms.netmgt.config.vacuumd.Statement;
import org.opennms.netmgt.config.vacuumd.Trigger;

public interface VacuumdConfigDao {

    /**
     * Returns a Collection of automations defined in the config
     * 
     * @return a {@link java.util.Collection} object.
     */
    public Collection<Automation> getAutomations();

    /**
     * Returns a Collection of triggers defined in the config
     * 
     * @return a {@link java.util.Collection} object.
     */
    public Collection<Trigger> getTriggers();

    /**
     * Returns a Collection of actions defined in the config
     * 
     * @return a {@link java.util.Collection} object.
     */
    public Collection<Action> getActions();

    /**
     * Returns a Collection of named events to that may have been configured
     * to be sent after an automation has run.
     * 
     * @return a {@link java.util.Collection} object.
     */
    public Collection<AutoEvent> getAutoEvents();

    /**
     * <p>
     * getActionEvents
     * </p>
     * 
     * @return a {@link java.util.Collection} object.
     */
    public Collection<ActionEvent> getActionEvents();

    /**
     * <p>
     * getPeriod
     * </p>
     * 
     * @return a int.
     */
    public int getPeriod();

    /**
     * Returns a Trigger with a name matching the string parameter
     * 
     * @param triggerName
     *            a {@link java.lang.String} object.
     * @return a {@link org.opennms.netmgt.config.vacuumd.Trigger} object.
     */
    public Trigger getTrigger(String triggerName);

    /**
     * Returns an Action with a name matching the string parmater
     * 
     * @param actionName
     *            a {@link java.lang.String} object.
     * @return a {@link org.opennms.netmgt.config.vacuumd.Action} object.
     */
    public Action getAction(String actionName);

    /**
     * Returns an Automation with a name matching the string parameter
     * 
     * @param autoName
     *            a {@link java.lang.String} object.
     * @return a {@link org.opennms.netmgt.config.vacuumd.Automation} object.
     */
    public Automation getAutomation(String autoName);

    /**
     * Returns the AutoEvent associated with the auto-event-name
     * 
     * @deprecated Use {@link ActionEvent} objects instead. Access these
     *             objects with {@link #getActionEvent(String)}.
     * @param name
     *            a {@link java.lang.String} object.
     * @return a {@link org.opennms.netmgt.config.vacuumd.AutoEvent} object.
     */
    public AutoEvent getAutoEvent(String name);

    /**
     * <p>
     * getSqlStatements
     * </p>
     * 
     * @return an array of {@link java.lang.String} objects.
     */
    public String[] getSqlStatements();

    /**
     * <p>
     * getStatements
     * </p>
     * 
     * @return a {@link java.util.List} object.
     */
    public List<Statement> getStatements();

    /**
     * <p>
     * getActionEvent
     * </p>
     * 
     * @param name
     *            a {@link java.lang.String} object.
     * @return a {@link org.opennms.netmgt.config.vacuumd.ActionEvent} object.
     */
    public ActionEvent getActionEvent(String name);
}
