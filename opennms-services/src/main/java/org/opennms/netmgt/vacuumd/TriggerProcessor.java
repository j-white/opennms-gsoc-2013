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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.opennms.core.utils.ThreadCategory;
import org.opennms.netmgt.config.vacuumd.Trigger;

public class TriggerProcessor {
    private final Trigger m_trigger;

    public TriggerProcessor(String automationName, Trigger trigger) {
        m_trigger = trigger;
    }

    public ThreadCategory log() {
        return ThreadCategory.getInstance(getClass());
    }

    public Trigger getTrigger() {
        return m_trigger;
    }

    public boolean hasTrigger() {
        return m_trigger != null;
    }

    public String getTriggerSQL() {
        if (hasTrigger()) {
            return getTrigger().getStatement().getContent();
        } else {
            return null;
        }
    }

    public String getName() {
        return getTrigger().getName();
    }

    @Override
    public String toString() {
        return m_trigger == null ? "<No-Trigger>" : m_trigger.getName();
    }

    ResultSet runTriggerQuery() throws SQLException {
        try {
            if (!hasTrigger()) {
                return null;
            }

            Connection conn = Transaction.getConnection(m_trigger.getDataSource());

            Statement triggerStatement = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                                                              ResultSet.CONCUR_READ_ONLY);
            Transaction.register(triggerStatement);

            ResultSet triggerResultSet = triggerStatement.executeQuery(getTriggerSQL());
            Transaction.register(triggerResultSet);

            return triggerResultSet;
        } catch (SQLException e) {
            log().warn("Error executing trigger " + getName(), e);
            throw e;
        }
    }

    /**
     * This method verifies that the number of rows in the result set of
     * the trigger match the defined operation in the config. For example,
     * if the user has specified that the trigger-rows = 5 and the
     * operator ">", the automation will only run if the result rows is
     * greater than 5.
     * 
     * @param trigRowCount
     * @param trigOp
     * @param resultRows
     * @param processor
     *            TODO
     */
    public boolean triggerRowCheck(int trigRowCount, String trigOp,
            int resultRows) {

        if (trigRowCount == 0 || trigOp == null) {
            log().debug("triggerRowCheck: trigger has no row-count restrictions: operator is: "
                                + trigOp
                                + ", row-count is: "
                                + trigRowCount);
            return true;
        }

        log().debug("triggerRowCheck: Verifying trigger resulting row count "
                            + resultRows
                            + " is "
                            + trigOp
                            + " "
                            + trigRowCount);

        boolean runAction = false;
        if ("<".equals(trigOp)) {
            if (resultRows < trigRowCount)
                runAction = true;

        } else if ("<=".equals(trigOp)) {
            if (resultRows <= trigRowCount)
                runAction = true;

        } else if ("=".equals(trigOp)) {
            if (resultRows == trigRowCount)
                runAction = true;

        } else if (">=".equals(trigOp)) {
            if (resultRows >= trigRowCount)
                runAction = true;

        } else if (">".equals(trigOp)) {
            if (resultRows > trigRowCount)
                runAction = true;

        }

        log().debug("Row count verification is: " + runAction);

        return runAction;
    }

}
