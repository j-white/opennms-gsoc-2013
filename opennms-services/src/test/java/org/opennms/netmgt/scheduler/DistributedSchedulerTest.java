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

package org.opennms.netmgt.scheduler;

import static com.jayway.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opennms.core.fiber.Fiber;
import org.opennms.core.grid.DataGridProvider;
import org.opennms.core.grid.DataGridProviderFactory;
import org.opennms.core.test.MockLogAppender;
import org.opennms.core.test.OpenNMSJUnit4ClassRunner;
import org.opennms.core.test.grid.annotations.JUnitGrid;
import org.opennms.netmgt.scheduler.ClusterRunnable;
import org.opennms.netmgt.scheduler.DataGridProviderAware;
import org.opennms.netmgt.scheduler.DistributedScheduler;
import org.opennms.netmgt.scheduler.Scheduler;
import org.springframework.test.context.ContextConfiguration;

/**
 * Distributed scheduler tests
 * 
 * @author jwhite
 * 
 */
@RunWith(OpenNMSJUnit4ClassRunner.class)
@ContextConfiguration(locations = {
        "classpath:/META-INF/opennms/applicationContext-soa.xml",
        "classpath*:/META-INF/opennms/component-dao.xml" })
@JUnitGrid()
public class DistributedSchedulerTest {
    private final static int NUM_CONCURRENT_SCHEDULERS = 3;
    private final static int DEFAULT_TASK_KEY = 87;
    private final static int DEFAULT_SCHEDULE_INTERVAL_MS = 1000;
    private final static String DEFAULT_SCHEDULER_NAME = "distributedSchedulerTest";
    private final static int MAX_EXECUTOR_SIZE = 4;

    @Before
    public void setUp() {
        MockLogAppender.setupLogging(true, "DEBUG");

        // Remove any existing values from the map
        MySingletonMap.getInstance().clearMap();
    }

    @After
    public void tearDown() {
        MockLogAppender.assertNoWarningsOrGreater();
    }

    @Test
    public void scheduleWithOneScheduler() throws Exception {
        // Create a new distributed scheduler
        DistributedScheduler distributedSchedulers[] = new DistributedScheduler[] { newDistributedScheduler() };
        scheduleOnXAndRunOnY(distributedSchedulers, 0, 0);
    }

    @Test
    public void scheduleWithManySchedulers() throws Exception {
        // Create N schedulers using different grid providers
        DistributedScheduler distributedSchedulers[] = new DistributedScheduler[NUM_CONCURRENT_SCHEDULERS];
        for (int i = 0; i < NUM_CONCURRENT_SCHEDULERS; i++) {
            distributedSchedulers[i] = newDistributedScheduler();
        }

        // Iterate through different combinations of schedulers
        for (int i = 0; i < NUM_CONCURRENT_SCHEDULERS; i++) {
            for (int j = 0; j < NUM_CONCURRENT_SCHEDULERS; j++) {
                // Schedule the task on i and run it on j
                scheduleOnXAndRunOnY(distributedSchedulers, i, j);
            }
        }
    }

    /**
     * Schedule a lot of tasks and make sure they don't all run on the same
     * node.
     * 
     * @throws Exception
     */
    @Test
    public void executionDistribution() throws Exception {
        // Create N different grid providers and schedules
        DataGridProvider dataGridProviders[] = new DataGridProvider[NUM_CONCURRENT_SCHEDULERS];
        DistributedScheduler distributedSchedulers[] = new DistributedScheduler[NUM_CONCURRENT_SCHEDULERS];
        for (int i = 0; i < NUM_CONCURRENT_SCHEDULERS; i++) {
            dataGridProviders[i] = DataGridProviderFactory.getNewInstance();
            distributedSchedulers[i] = new DistributedScheduler(
                                                                DEFAULT_SCHEDULER_NAME,
                                                                MAX_EXECUTOR_SIZE,
                                                                dataGridProviders[i]);
            // Start the schedulers immediately
            distributedSchedulers[i].start();
        }

        // Schedule many tasks, each using a unique key, but all using same
        // interval
        int k = 0;
        final int NUM_TASKS = 100;
        final int NUM_RESCHEDULES = 10;
        for (int i = 1; i <= NUM_TASKS; i++) {
            assertEquals(Integer.valueOf(0), getValueFor(i).call());
            //System.out.println(String.format("Scheduling task with key %d on %s",
            //                                 i,
            //                                 dataGridProviders[k].getName()));
            distributedSchedulers[k].schedule(DEFAULT_SCHEDULE_INTERVAL_MS,
                                              new MyRunnable(i,
                                                             NUM_RESCHEDULES,
                                                             DEFAULT_SCHEDULE_INTERVAL_MS));
            k = (k + 1) % NUM_CONCURRENT_SCHEDULERS;
        }

        // Wait until the last task has run through at all its iterations
        await().atMost(NUM_RESCHEDULES * DEFAULT_SCHEDULE_INTERVAL_MS * 2,
                       MILLISECONDS).until(getValueFor(NUM_TASKS),
                                           is(NUM_RESCHEDULES));

        // Stop the schedulers
        for (DistributedScheduler distributedScheduler : distributedSchedulers) {
            distributedScheduler.stop();
            await().until(isStopped(distributedScheduler));
        }

        // Display the distribution statistics
        double minPercentageOfTasksRanOnAnyMember = Double.MAX_VALUE;
        for (DataGridProvider dataGridProvider : dataGridProviders) {
            String gridProviderName = dataGridProvider.getName();
            int totalNumTasksRan = NUM_TASKS * NUM_RESCHEDULES;
            int numTasksRanOnMember = MySingletonMap.getInstance().getValueAt(gridProviderName);
            double percentageOfTasksRanOnMember = 100 * numTasksRanOnMember
                    / (double) totalNumTasksRan;

            System.out.println(String.format("%d/%d or %.2f%% of the tasks were executed on %s",
                                             numTasksRanOnMember,
                                             totalNumTasksRan,
                                             percentageOfTasksRanOnMember,
                                             gridProviderName));

            if (percentageOfTasksRanOnMember < minPercentageOfTasksRanOnAnyMember) {
                minPercentageOfTasksRanOnAnyMember = percentageOfTasksRanOnMember;
            }
        }

        // TODO: Looks a ways to improve the spread
        // Require at least a 15% spread
        assertTrue(minPercentageOfTasksRanOnAnyMember > 15);
    }

    private void scheduleOnXAndRunOnY(
            DistributedScheduler distributedSchedulers[], int x, int y)
            throws Exception {
        // Remove any existing values from the map
        MySingletonMap.getInstance().clearMap();

        // Create a new task
        MyRunnable runnable = new MyRunnable(DEFAULT_TASK_KEY);

        // Verify that no tasks are scheduled on any of the schedulers
        for (int i = 0; i < distributedSchedulers.length; i++) {
            assertEquals(0, distributedSchedulers[i].getScheduled());
        }

        // Schedule the task on scheduler X
        distributedSchedulers[x].schedule(DEFAULT_SCHEDULE_INTERVAL_MS,
                                          runnable);

        // Verify that the task is scheduled on all of the schedulers
        for (int i = 0; i < distributedSchedulers.length; i++) {
            assertEquals(1, distributedSchedulers[i].getScheduled());
        }

        // Make sure the task has not been executed
        assertEquals(Integer.valueOf(0), getValueFor(DEFAULT_TASK_KEY).call());

        // Start scheduler Y
        distributedSchedulers[y].start();

        // Verify that the task gets executed
        await().atMost(DEFAULT_SCHEDULE_INTERVAL_MS * 2000, MILLISECONDS).until(getValueFor(DEFAULT_TASK_KEY),
                                                                             is(1));

        // Stop scheduler Y and block until it's fully stopped
        distributedSchedulers[y].stop();
        await().until(isStopped(distributedSchedulers[y]));
    }

    private static DistributedScheduler newDistributedScheduler() {
        return new DistributedScheduler(
                                        DEFAULT_SCHEDULER_NAME,
                                        MAX_EXECUTOR_SIZE,
                                        DataGridProviderFactory.getNewInstance());
    }

    /**
     * Retrieves the value at the given key.
     */
    private Callable<Integer> getValueFor(final int key) {
        return new Callable<Integer>() {
            public Integer call() {
                return MySingletonMap.getInstance().getValueAt(key);
            }
        };
    }

    /**
     * Checks if a fiber is stopped.
     */
    private Callable<Boolean> isStopped(final Fiber fiber) {
        return new Callable<Boolean>() {
            public Boolean call() {
                return fiber.getStatus() == Fiber.STOPPED;
            }
        };
    }

    /**
     * Adds a unique key the a global hash set when run.
     */
    public static class MyRunnable implements ClusterRunnable, DataGridProviderAware {
        private static final long serialVersionUID = 2352596012788985548L;
        private int m_key;
        private int m_numReschedules;
        private long m_interval;
        private transient DataGridProvider m_dataGridProvider;
        private transient Scheduler m_scheduler;

        public MyRunnable(int key) {
            this(key, 0, 0);
        }

        public MyRunnable(int key, int numReschedules, long interval) {
            m_key = key;
            m_numReschedules = numReschedules;
            m_interval = interval;
        }

        @Override
        public void run() {
            assertNotNull(m_dataGridProvider);
            //System.out.println("Running task with key " + m_key + " on "
            //        + m_dataGridProvider.getName());

            MySingletonMap.getInstance().incValueAt(m_key);
            MySingletonMap.getInstance().incValueAt(m_dataGridProvider.getName());

            if (m_numReschedules > 0) {
                m_scheduler.schedule(m_interval, this);
                m_numReschedules--;
            }
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setScheduler(
                Scheduler scheduler) {
            m_scheduler = scheduler;
        }

        @Override
        public void setDataGridProvider(DataGridProvider dataGridProvider) {
            m_dataGridProvider = dataGridProvider;
        }
    }

    /**
     * Maintains a global map.
     */
    public static class MySingletonMap {
        private static MySingletonMap m_instance = null;

        private Map<String, Integer> m_map = new HashMap<String, Integer>();

        private MySingletonMap() {

        }

        public static synchronized MySingletonMap getInstance() {
            if (m_instance == null) {
                m_instance = new MySingletonMap();
            }
            return m_instance;
        }

        public void incValueAt(int key) {
            incValueAt("" + key);
        }

        public void incValueAt(String key) {
            int val = getValueAt(key);
            m_map.put("" + key, val + 1);
        }

        public int getValueAt(int key) {
            return getValueAt("" + key);
        }

        public int getValueAt(String key) {
            Integer val = m_map.get(key);
            if (val == null) {
                return 0;
            }
            return val;
        }

        public void clearMap() {
            m_map.clear();
        }
    }
}
