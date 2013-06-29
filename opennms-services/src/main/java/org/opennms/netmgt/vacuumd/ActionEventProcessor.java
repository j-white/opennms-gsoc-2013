/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2006-2013 The OpenNMS Group, Inc.
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

package org.opennms.netmgt.vacuumd;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.opennms.core.utils.PropertiesUtils.SymbolTable;
import org.opennms.netmgt.config.vacuumd.ActionEvent;
import org.opennms.netmgt.config.vacuumd.Assignment;
import org.opennms.netmgt.model.events.EventBuilder;
import org.opennms.netmgt.model.events.Parameter;
import org.opennms.netmgt.xml.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActionEventProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(ActionEventProcessor.class);

    private final String m_automationName;
    private final ActionEvent m_actionEvent;
    private final List<EventAssignment> m_assignments;

    public ActionEventProcessor(String automationName, ActionEvent actionEvent) {
        m_automationName = automationName;
        m_actionEvent = actionEvent;

        if (actionEvent != null) {

            m_assignments = new ArrayList<EventAssignment>(
                                                           actionEvent.getAssignmentCount());
            for (Assignment assignment : actionEvent.getAssignment()) {
                m_assignments.add(new EventAssignment(assignment));
            }

        } else {
            m_assignments = null;
        }

    }

    public boolean hasEvent() {
        return m_actionEvent != null;
    }

    void send() {

        if (hasEvent()) {
            // the uei will be set by the event assignments
            EventBuilder bldr = new EventBuilder(null, "Automation");
            buildEvent(bldr, new InvalidSymbolTable());
            LOG.debug("ActionEventProcessor: Sending action-event "
                    + bldr.getEvent().getUei() + " for automation "
                    + m_automationName);
            sendEvent(bldr.getEvent());

        } else {
            LOG.debug("ActionEventProcessor: No action-event for automation {}",
                      m_automationName);
        }
    }

    private void buildEvent(EventBuilder bldr, SymbolTable symbols) {
        for (EventAssignment assignment : m_assignments) {
            assignment.assign(bldr, symbols);
        }
    }

    private void sendEvent(Event event) {
        Vacuumd.getSingleton().getEventManager().sendNow(event);
    }

    void processTriggerResults(TriggerResults triggerResults)
            throws SQLException {
        if (!hasEvent()) {
            LOG.debug("processTriggerResults: No action-event for automation {}",
                      m_automationName);
            return;
        }

        ResultSet triggerResultSet = triggerResults.getResultSet();

        triggerResultSet.beforeFirst();

        // Loop through the select results
        while (triggerResultSet.next()) {
            // the uei will be set by the event assignments
            EventBuilder bldr = new EventBuilder(null, "Automation");
            ResultSetSymbolTable symbols = new ResultSetSymbolTable(
                                                                    triggerResultSet);

            try {
                if (m_actionEvent.isAddAllParms()
                        && resultHasColumn(triggerResultSet, "eventParms")) {
                    bldr.setParms(Parameter.decode(triggerResultSet.getString("eventParms")));
                }
                buildEvent(bldr, symbols);
            } catch (SQLExceptionHolder holder) {
                holder.rethrow();
            }
            LOG.debug("processTriggerResults: Sending action-event "
                    + bldr.getEvent().getUei() + " for automation "
                    + m_automationName);
            sendEvent(bldr.getEvent());
        }

    }

    private boolean resultHasColumn(ResultSet resultSet, String columnName) {
        try {
            if (resultSet.findColumn(columnName) > 0) {
                return true;
            }
        } catch (SQLException e) {
        }
        return false;
    }

    public boolean forEachResult() {
        return m_actionEvent == null ? false
                                    : m_actionEvent.getForEachResult();
    }

    void processActionEvent(TriggerResults triggerResults)
            throws SQLException {
        if (triggerResults.hasTrigger() && forEachResult()) {
            processTriggerResults(triggerResults);
        } else {
            send();
        }
    }

}
