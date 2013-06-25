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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.opennms.core.utils.ThreadCategory;
import org.opennms.netmgt.config.vacuumd.Action;

public class ActionProcessor {

    private final String m_automationName;
    private final Action m_action;

    public ActionProcessor(String automationName, Action action) {
        m_automationName = automationName;
        m_action = action;
    }

    public boolean hasAction() {
        return m_action != null;
    }

    public Action getAction() {
        return m_action;
    }

    public ThreadCategory log() {
        return ThreadCategory.getInstance(getClass());
    }

    String getActionSQL() {
        return getAction().getStatement().getContent();
    }

    PreparedStatement createPreparedStatement() throws SQLException {
        String actionJDBC = getActionSQL().replaceAll("\\$\\{\\w+\\}", "?");

        log().debug("createPrepareStatement: This action SQL: "
                            + getActionSQL() + "\nTurned into this: "
                            + actionJDBC);

        Connection conn = Transaction.getConnection(m_action.getDataSource());
        PreparedStatement stmt = conn.prepareStatement(actionJDBC);
        Transaction.register(stmt);
        return stmt;
    }

    /**
     * Returns an ArrayList containing the names of column defined as tokens
     * in the action statement defined in the config. If no tokens are found,
     * an empty list is returned.
     * 
     * @param targetString
     * @return
     */
    public List<String> getActionColumns() {
        return getTokenizedColumns(getActionSQL());
    }

    private List<String> getTokenizedColumns(String targetString) {
        // The \w represents a "word" charactor
        String expression = "\\$\\{(\\w+)\\}";
        Pattern pattern = Pattern.compile(expression);
        Matcher matcher = pattern.matcher(targetString);

        log().debug("getTokenizedColumns: processing string: " + targetString);

        List<String> tokens = new ArrayList<String>();
        int count = 0;
        while (matcher.find()) {
            count++;
            log().debug("getTokenizedColumns: Token " + count + ": "
                                + matcher.group(1));

            tokens.add(matcher.group(1));
        }
        return tokens;
    }

    void assignStatementParameters(PreparedStatement stmt, ResultSet rs)
            throws SQLException {
        List<String> actionColumns = getTokenizedColumns(getActionSQL());
        Iterator<String> it = actionColumns.iterator();
        String actionColumnName = null;
        int i = 0;
        while (it.hasNext()) {
            actionColumnName = (String) it.next();
            stmt.setObject(++i, rs.getObject(actionColumnName));
        }

    }

    /**
     * Counts the number of tokens in an Action Statement.
     * 
     * @param targetString
     * @return
     */
    public int getTokenCount(String targetString) {
        // The \w represents a "word" charactor
        String expression = "(\\$\\{\\w+\\})";
        Pattern pattern = Pattern.compile(expression,
                                          Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(targetString);

        log().debug("getTokenCount: processing string: " + targetString);

        int count = 0;
        while (matcher.find()) {
            count++;
            log().debug("getTokenCount: Token " + count + ": "
                                + matcher.group(1));
        }
        return count;
    }

    boolean execute() throws SQLException {
        // No trigger defined, just running the action.
        if (getTokenCount(getActionSQL()) != 0) {
            log().info("execute: not running action: "
                               + m_action.getName()
                               + ".  Action contains tokens in an automation ("
                               + m_automationName + ") with no trigger.");
            return false;
        } else {
            // Convert the sql to a PreparedStatement
            PreparedStatement actionStatement = createPreparedStatement();
            actionStatement.executeUpdate();
            return true;
        }
    }

    boolean processTriggerResults(TriggerResults triggerResults)
            throws SQLException {
        ResultSet triggerResultSet = triggerResults.getResultSet();

        triggerResultSet.beforeFirst();

        PreparedStatement actionStatement = createPreparedStatement();

        // Loop through the select results
        while (triggerResultSet.next()) {
            // Convert the sql to a PreparedStatement
            assignStatementParameters(actionStatement, triggerResultSet);
            actionStatement.executeUpdate();
        }

        return true;
    }

    boolean processAction(TriggerResults triggerResults) throws SQLException {
        if (triggerResults.hasTrigger()) {
            return processTriggerResults(triggerResults);
        } else {
            return execute();
        }
    }

    public String getName() {
        return m_action.getName();
    }

    @Override
    public String toString() {
        return m_action.getName();
    }

    public void checkForRequiredColumns(TriggerResults triggerResults) {
        ResultSet triggerResultSet = triggerResults.getResultSet();
        if (!resultSetHasRequiredActionColumns(triggerResultSet,
                                               getActionColumns())) {
            throw new AutomationException("Action " + this
                    + " uses column not defined in trigger: "
                    + triggerResults);
        }
    }

    /**
     * Helper method that verifies tokens in a config defined action are
     * available in the ResultSet of the paired trigger
     * 
     * @param rs
     * @param actionColumns
     *            TODO
     * @param actionSQL
     * @param processor
     *            TODO
     * @return
     */
    public boolean resultSetHasRequiredActionColumns(ResultSet rs,
            Collection<String> actionColumns) {

        log().debug("resultSetHasRequiredActionColumns: Verifying required action columns in trigger ResultSet...");

        if (actionColumns.isEmpty()) {
            return true;
        }

        if (rs == null) {
            return false;
        }

        boolean verified = true;
        String actionColumnName = null;

        Iterator<String> it = actionColumns.iterator();

        while (it.hasNext()) {
            actionColumnName = (String) it.next();
            try {
                if (rs.findColumn(actionColumnName) > 0) {
                }
            } catch (SQLException e) {
                log().warn("resultSetHasRequiredActionColumns: Trigger ResultSet does NOT have required action columns.  Missing: "
                                   + actionColumnName);
                log().warn(e.getMessage());
                verified = false;
            }
        }

        return verified;
    }

}
