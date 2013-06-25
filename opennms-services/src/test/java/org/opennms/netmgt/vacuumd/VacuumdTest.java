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
import java.util.concurrent.Callable;

import javax.sql.DataSource;

import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;
import org.opennms.core.db.DataSourceFactory;
import org.opennms.netmgt.config.VacuumdConfigFactory;
import org.opennms.netmgt.config.vacuumd.Action;
import org.opennms.netmgt.config.vacuumd.Actions;
import org.opennms.netmgt.config.vacuumd.Automation;
import org.opennms.netmgt.config.vacuumd.Automations;
import org.opennms.netmgt.config.vacuumd.Statement;
import org.opennms.netmgt.config.vacuumd.VacuumdConfiguration;
import org.opennms.netmgt.eventd.mock.MockEventIpcManager;
import org.opennms.test.mock.EasyMockUtils;

import static com.jayway.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

/**
 * Vacuumd Unit Tests
 * 
 * @author jwhite
 */
public class VacuumdTest {
    private MockEventIpcManager m_mockEventIpcManager;
    private EasyMockUtils m_ezMock = new EasyMockUtils();
    private Connection m_conn;
    private DataSource m_ds;

    private boolean m_executeUpdateCalled;

    @Before
    public void setUp() {
        m_mockEventIpcManager = new MockEventIpcManager();

        m_ds = m_ezMock.createMock(DataSource.class);
        m_conn = m_ezMock.createMock(Connection.class);
        DataSourceFactory.setInstance("ds", m_ds);
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
    public void automationWithAction() throws Exception {
        // Build the configuration for a single automation with a
        // simplebaction
        Automation automation = new Automation();
        automation.setActionName("garbageCollect");

        // Set a large interval so it only runs once one startup during our
        // tests
        automation.setInterval(99999);
        automation.setActive(true);

        Automations automations = new Automations();
        automations.addAutomation(automation);

        String sql = "DELETE FROM alarms WHERE alarmacktime IS NULL";
        Statement statement = new Statement(sql, false);
        Action action = new Action("garbageCollect", "ds", statement);

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
        EasyMock.expect(m_conn.prepareStatement(sql)).andReturn(preparedStatement);

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
