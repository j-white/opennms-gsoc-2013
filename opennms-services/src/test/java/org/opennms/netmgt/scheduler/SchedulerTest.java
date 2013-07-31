package org.opennms.netmgt.scheduler;

import static com.jayway.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.hamcrest.Matchers.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Callable;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.opennms.core.fiber.Fiber;
import org.opennms.core.fiber.PausableFiber;
import org.opennms.core.grid.mock.MockGridProvider;

/**
 * Common tests for all Scheduler implementations
 *
 * @author jwhite
 */
@RunWith(value = Parameterized.class)
public class SchedulerTest {
    /**
     * Instance of the scheduler.
     */
    private Scheduler scheduler;

    /**
     * Factory method used to create the scheduler.
     */
    private Callable<Scheduler> createScheduler;

    /**
     * Parameterized test constructor
     * 
     * @param createScheduler
     *            factor method used to create the scheduler before each test
     */
    public SchedulerTest(Callable<Scheduler> createScheduler) {
        this.createScheduler = createScheduler;
    }

    public static Callable<Scheduler> createLegacyScheduler() {
        return new Callable<Scheduler>() {
            public Scheduler call() {
                return new LegacyScheduler("test", 1);
            }
        };
    }

    public static Callable<Scheduler> createDistributedScheduler() {
        return new Callable<Scheduler>() {
            public Scheduler call() {
                MockGridProvider mockGridProvider = new MockGridProvider();
                return new DistributedScheduler("test", 1,
                                                mockGridProvider);
            }
        };
    }

    @Parameters()
    public static Collection<Object[]> data() {
        Object[][] data = new Object[][] { { createLegacyScheduler() },
                { createDistributedScheduler() } };
        return Arrays.asList(data);
    }

    @Before
    public void setUp() throws Exception {
        scheduler = createScheduler.call();
    }

    @Test
    public void basicLifecycle() throws Exception {
        // There should be no tasks scheduled
        assertEquals(0, scheduler.getScheduled());

        // The scheduler should be in a "start pending" state after creation
        assertEquals(Integer.valueOf(PausableFiber.START_PENDING), getStatus().call());

        scheduler.start();
        await().until(getStatus(), is(PausableFiber.RUNNING));

        scheduler.pause();
        await().until(getStatus(), is(PausableFiber.PAUSED));

        // An additional call to pause should have no effect
        scheduler.pause();
        assertEquals(PausableFiber.PAUSED, scheduler.getStatus());

        scheduler.resume();
        await().until(getStatus(), is(PausableFiber.RUNNING));

        // An additional call to resume should have no effect
        scheduler.resume();
        assertEquals(PausableFiber.RUNNING, scheduler.getStatus());

        scheduler.stop();
        await().until(getStatus(), is(PausableFiber.STOPPED));

        // Attempting to restart the scheduler after it has been stopped
        // should result in an exception
        boolean exceptionOnSecondStart = false;
        try {
            scheduler.start();
        } catch (IllegalStateException e) {
            exceptionOnSecondStart = true;
        }
        assertTrue(exceptionOnSecondStart);
    }

    @Test
    public void scheduleOneTask() throws Exception {
        scheduler.start();

        MockSchedulable schedulable = new MockSchedulable();
        assertEquals(Integer.valueOf(0), getRunCount(schedulable).call());

        scheduler.schedule(10, schedulable);
        await().until(getRunCount(schedulable), is(1));

        scheduler.stop();
        await().until(getStatus(), is(Fiber.STOPPED));
    }

    @Test
    public void scheduleManyTasks() throws Exception {
        scheduler.start();

        final int N_TASKS = 5;
        MockSchedulable schedulables[] = new MockSchedulable[N_TASKS];
        for (int i = 0; i < N_TASKS; i++) {
            schedulables[i] = new MockSchedulable();
            scheduler.schedule(10 * (i + 1), schedulables[i]);
        }

        for (int i = N_TASKS - 1; i >= 0; i--) {
            await().until(getRunCount(schedulables[i]), is(1));
        }

        scheduler.stop();
        await().until(getStatus(), is(Fiber.STOPPED));
    }

    @Test
    public void rescheduleViaReschedulable() throws Exception {
        scheduler.start();

        final int N_RESCHEDULES = 3;
        final long INTERVAL_MS = 100;
        MockSchedulable schedulable = new MockSchedulable();
        schedulable.setRescheduleInterval(INTERVAL_MS);
        schedulable.setRescheduleCount(N_RESCHEDULES);

        scheduler.schedule(INTERVAL_MS, schedulable);
        await().until(getRunCount(schedulable), is(N_RESCHEDULES+1));

        scheduler.stop();
        await().until(getStatus(), is(Fiber.STOPPED));
    }

    @Test
    public void resetWithNoTasksRunning() throws Exception {
        scheduler.start();

        MockSchedulable schedulable = new MockSchedulable();
        scheduler.schedule(100, schedulable);

        assertEquals(1, scheduler.getScheduled());
        scheduler.reset();

        assertEquals(0, scheduler.getScheduled());

        Thread.sleep(2000);
        assertEquals(0, schedulable.getRunCount());

        scheduler.stop();
        await().until(getStatus(), is(Fiber.STOPPED));
    }

    @Test
    public void resetWithRunningTask() throws Exception {
        scheduler.start();

        MockSchedulable schedulable = new MockSchedulable();
        schedulable.setSleepMs(1000);
        schedulable.setRescheduleInterval(1000);
        schedulable.setRescheduleCount(1);

        scheduler.schedule(100, schedulable);
        assertEquals(Integer.valueOf(1), getScheduled().call());
        await().until(getScheduled(), is(0));

        // We know we have a task running now, reset the scheduler
        scheduler.reset();

        // The task should finish executing
        await().until(getRunCount(schedulable), is(1));

        // But should not be rescheduled
        Thread.sleep(100);
        assertEquals(0, scheduler.getScheduled());

        scheduler.stop();
        await().until(getStatus(), is(Fiber.STOPPED));
    }

    private static class MockSchedulable implements ReadyRunnable,
        SchedulerAware, Reschedulable {
        private volatile int m_runCount = 0;
        private volatile Scheduler m_scheduler = null;
        private volatile long m_rescheduleInterval;
        private volatile int m_rescheduleCount = 0;
        private volatile long m_sleepMs = 0;

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void run() {
            assertTrue(m_scheduler != null);

            try {                
                if (m_sleepMs > 0) {
                    Thread.sleep(m_sleepMs);
                }
            } catch (InterruptedException e) {
                return;
            }

            m_runCount++;
        }

        @Override
        public void setScheduler(Scheduler scheduler) {
            m_scheduler = scheduler;
        }

        public int getRunCount() {
            return m_runCount;
        }

        public void setSleepMs(long sleepMs) {
            m_sleepMs = sleepMs;
        }

        public void setRescheduleInterval(long interval) {
            m_rescheduleInterval = interval;
        }

        public void setRescheduleCount(int count) {
            m_rescheduleCount = count;
        }

        @Override
        public boolean rescheduleAfterRun() {
            if (m_rescheduleCount > 0) {
                m_rescheduleCount--;
                return true;
            } else {
                return false;
            }
        }

        @Override
        public long getInterval() {
            return m_rescheduleInterval;
        }
    }

    private Callable<Integer> getScheduled() {
        return new Callable<Integer>() {
            public Integer call() {
                return scheduler.getScheduled();
            }
        };
    }

    private Callable<Integer> getStatus() {
        return new Callable<Integer>() {
            public Integer call() {
                return scheduler.getStatus();
            }
        };
    }

    private Callable<Integer> getRunCount(
            final MockSchedulable mockSchedulable) {
        return new Callable<Integer>() {
            public Integer call() {
                return mockSchedulable.getRunCount();
            }
        };
    }
}
