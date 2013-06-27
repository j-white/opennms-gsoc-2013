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

package org.opennms.netmgt.vacuumd;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.Callable;

import javax.sql.DataSource;

import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opennms.core.db.DataSourceFactory;
import org.opennms.netmgt.config.VacuumdConfigFactory;
import org.opennms.netmgt.config.vacuumd.Action;
import org.opennms.netmgt.config.vacuumd.Actions;
import org.opennms.netmgt.config.vacuumd.AutoEvent;
import org.opennms.netmgt.config.vacuumd.AutoEvents;
import org.opennms.netmgt.config.vacuumd.Automation;
import org.opennms.netmgt.config.vacuumd.Automations;
import org.opennms.netmgt.config.vacuumd.Statement;
import org.opennms.netmgt.config.vacuumd.Trigger;
import org.opennms.netmgt.config.vacuumd.Triggers;
import org.opennms.netmgt.config.vacuumd.Uei;
import org.opennms.netmgt.config.vacuumd.VacuumdConfiguration;
import org.opennms.netmgt.eventd.mock.EventAnticipator;
import org.opennms.netmgt.eventd.mock.MockEventIpcManager;
import org.opennms.netmgt.model.events.EventBuilder;
import org.opennms.test.mock.EasyMockUtils;

import static com.jayway.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;

/**
 * Vacuumd Unit Tests
 * 
 * @author jwhite
 */
public class VacuumdTest {
    private MockEventIpcManager m_mockEventIpcManager;
    private EventAnticipator m_eventAnticipator;
    private EasyMockUtils m_ezMock = new EasyMockUtils();
    private Connection m_conn;
    private DataSource m_ds;
    private boolean m_executeUpdateCalled;

    private static final String SQL_STMT = "UPDATE accounts SET balance=10,000,000 WHERE name=`whoami`";

    @Before
    public void setUp() {
        m_mockEventIpcManager = new MockEventIpcManager();
        m_eventAnticipator = new EventAnticipator();
        m_mockEventIpcManager.setEventAnticipator(m_eventAnticipator);

        m_ds = m_ezMock.createMock(DataSource.class);
        m_conn = m_ezMock.createMock(Connection.class);
        DataSourceFactory.setInstance(m_ds);
    }

    @After
    public void tearDown() {
        m_eventAnticipator.verifyAnticipated();
    }

    @Test
    public void trivialStartStop() {
        VacuumdConfiguration vacuumdConfig = new VacuumdConfiguration();
        useVacuumdConfig(vacuumdConfig);

        Vacuumd vacuumd = new Vacuumd();
        vacuumd.setEventManager(m_mockEventIpcManager);
        vacuumd.init();
        vacuumd.start();
        vacuumd.stop();
    }

    @Test
    public void statementWithPeriod() throws Exception {
        // Build the configuration for a single statement
        VacuumdConfiguration vacuumdConfig = new VacuumdConfiguration();
        // Run the statements every second
        vacuumdConfig.setPeriod(1000);

        Statement statement = new Statement(SQL_STMT, false);
        vacuumdConfig.addStatement(statement);

        useVacuumdConfig(vacuumdConfig);

        // Setup our mock objects.
        EasyMock.expect(m_ds.getConnection()).andReturn(m_conn);
        m_conn.setAutoCommit(true);

        PreparedStatement preparedStatement = m_ezMock.createMock(PreparedStatement.class);
        EasyMock.expect(m_conn.prepareStatement(SQL_STMT)).andReturn(preparedStatement);

        // Set a flag when the prepared statement is called
        m_executeUpdateCalled = false;
        IAnswer<Integer> answer = new IAnswer<Integer>() {
            @Override
            public Integer answer() throws Throwable {
                m_executeUpdateCalled = true;
                return 0;
            }
        };
        EasyMock.expect(preparedStatement.executeUpdate()).andAnswer(answer);
        preparedStatement.close();
        m_conn.close();

        m_ezMock.replayAll();

        assertEquals(false, wasExecuteUpdateCalled().call());

        // Fire up vacuumd
        Vacuumd vacuumd = new Vacuumd();
        Vacuumd.setInstance(vacuumd);
        vacuumd.setEventManager(m_mockEventIpcManager);
        vacuumd.init();
        vacuumd.start();

        // Wait until the flag is set, instead of sleeping for a fixed amount
        // of time. This speeds things up significantly.
        await().until(wasExecuteUpdateCalled());

        // Verify
        vacuumd.stop();
        m_ezMock.verifyAll();
    }

    @Test
    public void automationWithAutoEvent() throws Exception {
        // Build the configuration for a single automation with an auto-event
        Automation automation = new Automation();
        automation.setName("testAutomation");
        automation.setTriggerName("testTrigger");
        automation.setActionName("testAction");
        automation.setAutoEventName("testAutoEvent");
        automation.setInterval(1000);

        Automations automations = new Automations();
        automations.addAutomation(automation);

        Trigger trigger = new Trigger();
        trigger.setName("testTrigger");
        Statement statement = new Statement(SQL_STMT, false);
        trigger.setStatement(statement);

        Triggers triggers = new Triggers();
        triggers.addTrigger(trigger);

        Action action = new Action();
        action.setName("testAction");
        action.setStatement(statement);
        Actions actions = new Actions();
        actions.addAction(action);

        AutoEvent autoEvent = new AutoEvent();
        autoEvent.setFields("not!a*valid_&field");
        autoEvent.setName("testAutoEvent");
        final String testEventUei = "uei.opennms.org/vacuumd/alarmEscalated";
        autoEvent.setUei(new Uei(testEventUei));

        AutoEvents autoEvents = new AutoEvents();
        autoEvents.addAutoEvent(autoEvent);

        VacuumdConfiguration vacuumdConfig = new VacuumdConfiguration();
        vacuumdConfig.setAutomations(automations);
        vacuumdConfig.setTriggers(triggers);
        vacuumdConfig.setAutoEvents(autoEvents);
        vacuumdConfig.setActions(actions);
        useVacuumdConfig(vacuumdConfig);

        // A database connection
        EasyMock.expect(m_ds.getConnection()).andReturn(m_conn);
        m_conn.setAutoCommit(false);
        m_conn.commit();
        m_conn.close();

        // A result set for the trigger query
        ResultSet resultSet = m_ezMock.createMock(ResultSet.class);
        EasyMock.expect(resultSet.next()).andReturn(false).atLeastOnce();
        resultSet.beforeFirst();
        EasyMock.expectLastCall().atLeastOnce();
        resultSet.close();
        EasyMock.expectLastCall().atLeastOnce();

        // An SQL statement for trigger query
        java.sql.Statement sqlStatement = m_ezMock.createMock(java.sql.Statement.class);
        EasyMock.expect(m_conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                                               ResultSet.CONCUR_READ_ONLY)).andReturn(sqlStatement);
        EasyMock.expect(sqlStatement.executeQuery(SQL_STMT)).andReturn(resultSet);
        sqlStatement.close();

        // A prepared statement for the action
        PreparedStatement preparedStatement = m_ezMock.createMock(PreparedStatement.class);
        EasyMock.expect(m_conn.prepareStatement(SQL_STMT)).andReturn(preparedStatement);
        preparedStatement.close();

        m_ezMock.replayAll();

        EventBuilder bldr = new EventBuilder(testEventUei, "Automation");
        m_eventAnticipator.anticipateEvent(bldr.getEvent());

        // Fire up vacuumd
        Vacuumd vacuumd = new Vacuumd();
        Vacuumd.setInstance(vacuumd);
        vacuumd.setEventManager(m_mockEventIpcManager);
        vacuumd.init();
        vacuumd.start();

        // Wait until the flag is set, instead of sleeping for a fixed amount
        // of time. This speeds things up significantly.
        await().until(numAnticipatedEventsReceived(), is(1));

        // Verify
        vacuumd.stop();
        m_ezMock.verifyAll();
    }

    @Test
    public void automationWithAction() throws Exception {
        // Build the configuration for a single automation with a
        // simple action
        Automation automation = new Automation();
        automation.setActionName("payday");
        automation.setInterval(1000);
        automation.setActive(true);

        Automations automations = new Automations();
        automations.addAutomation(automation);

        Statement statement = new Statement(SQL_STMT, false);
        Action action = new Action();
        action.setName("payday");
        action.setStatement(statement);

        Actions actions = new Actions();
        actions.addAction(action);

        VacuumdConfiguration vacuumdConfig = new VacuumdConfiguration();
        vacuumdConfig.setAutomations(automations);
        vacuumdConfig.setActions(actions);
        useVacuumdConfig(vacuumdConfig);

        // Setup our mock objects.
        EasyMock.expect(m_ds.getConnection()).andReturn(m_conn);

        m_conn.setAutoCommit(false);

        PreparedStatement preparedStatement = m_ezMock.createMock(PreparedStatement.class);
        EasyMock.expect(m_conn.prepareStatement(SQL_STMT)).andReturn(preparedStatement);

        // Set a flag when the prepared statement is called
        m_executeUpdateCalled = false;
        IAnswer<Integer> answer = new IAnswer<Integer>() {
            @Override
            public Integer answer() throws Throwable {
                m_executeUpdateCalled = true;
                return 0;
            }
        };
        EasyMock.expect(preparedStatement.executeUpdate()).andAnswer(answer);
        preparedStatement.close();
        m_conn.commit();
        m_conn.close();

        m_ezMock.replayAll();

        assertEquals(false, wasExecuteUpdateCalled().call());

        // Fire up vacuumd
        Vacuumd vacuumd = new Vacuumd();
        Vacuumd.setInstance(vacuumd);
        vacuumd.setEventManager(m_mockEventIpcManager);
        vacuumd.init();
        vacuumd.start();

        // Wait until the flag is set, instead of sleeping for a fixed amount
        // of time. This speeds things up significantly.
        await().until(wasExecuteUpdateCalled());

        // Verify
        vacuumd.stop();
        m_ezMock.verifyAll();
    }

    private Callable<Integer> numAnticipatedEventsReceived() {
        return new Callable<Integer>() {
            public Integer call() {
                return m_eventAnticipator.getAnticipatedEventsRecieved().size();
            }
        };
    }

    private Callable<Boolean> wasExecuteUpdateCalled() {
        return new Callable<Boolean>() {
            public Boolean call() {
                return m_executeUpdateCalled;
            }
        };
    }

    private static void useVacuumdConfig(VacuumdConfiguration vacuumdConfig) {
        VacuumdConfigFactory vacuumdConfigFactory = new VacuumdConfigFactory(
                                                                             vacuumdConfig);
        VacuumdConfigFactory.setInstance(vacuumdConfigFactory);
    }
}
