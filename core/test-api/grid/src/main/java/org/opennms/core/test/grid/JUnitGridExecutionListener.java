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

package org.opennms.core.test.grid;

import java.lang.reflect.Method;

import org.apache.curator.test.TestingServer;
import org.opennms.core.grid.DataGridProvider;
import org.opennms.core.grid.DataGridProviderFactory;
import org.opennms.core.grid.hazelcast.HazelcastGridProvider;
import org.opennms.core.grid.zookeeper.ZKConfigFactory;
import org.opennms.core.grid.zookeeper.ZooKeeperGridProvider;
import org.opennms.core.test.grid.annotations.JUnitGrid;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.support.AbstractTestExecutionListener;

public class JUnitGridExecutionListener extends AbstractTestExecutionListener {
    private Class<? extends DataGridProvider> m_gridClass;
    private boolean m_reuseGrid = false;
    private TestingServer m_zkTestServer;

    public void beforeTestClass(TestContext testContext) throws Exception {
        final JUnitGrid jug = findAnnotation(testContext);

        if (jug == null) {
            return;
        }

        m_reuseGrid = jug.reuseGrid();
        m_gridClass = getGridClazz();
    }

    public void beforeTestMethod(TestContext testContext) throws Exception {
        final JUnitGrid jug = findAnnotation(testContext);

        if (jug == null) {
            return;
        }

        startupProvider();
    }

    public void afterTestMethod(TestContext testContext) throws Exception {
        final JUnitGrid jug = findAnnotation(testContext);

        if (jug == null) {
            return;
        }

        if (!m_reuseGrid) {
            shutdownAndResetProvider();
        }
    }

    public void afterTestClass(TestContext testContext) throws Exception {
        shutdownAndResetProvider();
    }

    private void startupProvider() throws Exception {
        if (m_gridClass == null) {
            return;
        }

        if (HazelcastGridProvider.class.isAssignableFrom(m_gridClass)) {
            System.setProperty("hazelcast.logging.type", "slf4j");
            System.setProperty("java.net.preferIPv4Stack", "true");
        }

        if (ZooKeeperGridProvider.class.isAssignableFrom(m_gridClass)) {
            m_zkTestServer = new TestingServer();
            ZKConfigFactory.setInstance(new ZKConfigFactory(m_zkTestServer.getConnectString()));
        }
    }

    private void shutdownAndResetProvider() throws Exception {
        // Shutdown all of the instances
        DataGridProviderFactory.shutdownAll();

        // Reset the default instance - it will no longer work after being
        // shutdown
        DataGridProviderFactory.setInstance(null);
        
        if (m_gridClass == null) {
            return;
        }

        if (ZooKeeperGridProvider.class.isAssignableFrom(m_gridClass)) {
            if (m_zkTestServer != null) {
                m_zkTestServer.close();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static Class<? extends DataGridProvider> getGridClazz() throws ClassNotFoundException {
        String className = System.getProperty("gridClazz");
        if (className == null) {
            return null;
        }
        return (Class<? extends DataGridProvider>) Class.forName(className);
    }

    private static JUnitGrid findAnnotation(final TestContext testContext) {
        JUnitGrid jug = null;
        final Method testMethod = testContext.getTestMethod();
        if (testMethod != null) {
            jug = testMethod.getAnnotation(JUnitGrid.class);
        }
        if (jug == null) {
            final Class<?> testClass = testContext.getTestClass();
            jug = testClass.getAnnotation(JUnitGrid.class);
        }
        return jug;
    }
}
