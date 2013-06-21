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

import static com.jayway.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertNotNull;

import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opennms.core.grid.DataGridProviderFactory;
import org.opennms.core.grid.DataGridProvider;
import org.opennms.core.test.MockLogAppender;
import org.opennms.core.test.OpenNMSJUnit4ClassRunner;
import org.opennms.core.test.grid.annotations.JUnitGrid;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;

/**
 * Run some sanity checks against the default data grid provider.
 * 
 * @author jwhite
 */
@RunWith(OpenNMSJUnit4ClassRunner.class)
@ContextConfiguration(locations = {
        "classpath:/META-INF/opennms/applicationContext-soa.xml",
        "classpath*:/META-INF/opennms/component-dao.xml" })
@JUnitGrid()
public class DefaultDataGridProviderTest implements InitializingBean {
    @Autowired
    private DataGridProvider m_dataGridProvider = null;

    /**
     * The number of cluster members to test with.
     */
    private static int NUM_CLUSTER_MEMBERS = 3;

    /**
     * Ensure that a provider gets auto-wired if requested.
     */
    @Override
    public void afterPropertiesSet() {
        // Make sure that a provider gets auto-wired if requested
        assertNotNull(m_dataGridProvider);
    }

    @Before
    public void setUp() {
        MockLogAppender.setupLogging(true, "INFO");
    }

    /**
     * Ensure the provider does not generate any warnings or higher during
     * normal operations.
     */
    @After
    public void tearDown() throws Exception {
        MockLogAppender.assertNoWarningsOrGreater();
    }

    /**
     * Ensure multiple members are able to join the cluster if multiple
     * providers are instantiated.
     */
    @Test
    public void multipleClusterMembers() {
        DataGridProvider dataGridProvider[] = new DataGridProvider[NUM_CLUSTER_MEMBERS];
        for (int i = 0; i < NUM_CLUSTER_MEMBERS; i++) {
            // Get a new instance as opposed to re-using the same one so that
            // multiple members can be on the cluster
            dataGridProvider[i] = DataGridProviderFactory.getNewInstance();
            dataGridProvider[i].init();
        }

        for (int i = 0; i < NUM_CLUSTER_MEMBERS; i++) {
            await().until(getNumClusterMembers(dataGridProvider[i]),
                          is(NUM_CLUSTER_MEMBERS));
        }
    }

    private Callable<Integer> getNumClusterMembers(
            final DataGridProvider dataGridProvider) {
        return new Callable<Integer>() {
            public Integer call() throws Exception {
                return dataGridProvider.getClusterMembers().size();
            }
        };
    }
}
