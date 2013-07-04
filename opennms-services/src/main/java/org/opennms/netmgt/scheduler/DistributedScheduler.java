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

import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.opennms.core.concurrent.LogPreservingThreadFactory;
import org.opennms.core.fiber.PausableFiber;
import org.opennms.core.grid.DataGridProvider;
import org.opennms.core.grid.DataGridProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

/**
 * 
 * 
 * @author jwhite
 */
public class DistributedScheduler implements PausableFiber,
        Scheduler {
    /**
     * The map of priority queues that contain the scheduled tasks. The tasks
     * are sorted by target execution time. The queues are mapped according to
     * the interval of scheduling.
     */
    private Map<Long, PriorityQueue<ReadyRunnable>> m_queues;

    /**
     * Queue of tasks that are ready to be executed.
     */
    private BlockingQueue<ReadyRunnable> m_executionQueue;

    /**
     * Set of tasks that are either ready to be executed, or are currently
     * being executed.
     */
    private Set<ReadyRunnable> m_executingSet;

    /**
     * The status for this fiber.
     */
    private int m_status;

    /**
     * The pool of threads that are used to executed the tasks.
     */
    private ExecutorService m_runner;

    private WorkerThread m_workerThread;
    
    private ConsumerThread m_consumerThread;

    /**
     * Data grid provider.
     */
    private DataGridProvider m_dataGridProvider;

    /**
     * Distributed lock.
     */
    private Lock m_lock;

    /**
     * Unique name for this scheduler.
     */
    private String m_schedulerName;

    /**
     * Maximum size of the thread pool.
     */
    private int m_maxNumberOfThreadsInPool;

    /**
     * Prefix.
     */
    public static final String SCHEDULER_GRID_NAME_PREFIX = "Scheduler.";

    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(DistributedScheduler.class);

    public DistributedScheduler(String name, int maxSize) {
        this(name, maxSize, null);
    }

    public DistributedScheduler(String name, int maxSize,
            DataGridProvider dataGridProvider) {
        m_status = START_PENDING;
        m_maxNumberOfThreadsInPool = maxSize;
        m_workerThread = null;
        m_consumerThread = null;

        if (dataGridProvider == null) {
            m_dataGridProvider = DataGridProviderFactory.getInstance();
        } else {
            m_dataGridProvider = dataGridProvider;
        }

        // Initialize the distributed objects
        m_schedulerName = name;
        m_queues = m_dataGridProvider.getMap(SCHEDULER_GRID_NAME_PREFIX
                + m_schedulerName);
        m_executionQueue = m_dataGridProvider.getQueue(SCHEDULER_GRID_NAME_PREFIX
                + m_schedulerName);
        m_executingSet = m_dataGridProvider.getSet(SCHEDULER_GRID_NAME_PREFIX
                + m_schedulerName);
        m_lock = m_dataGridProvider.getLock(SCHEDULER_GRID_NAME_PREFIX
                + m_schedulerName);
    }

    /**
     * This method is used to schedule a ready runnable in the system. The
     * interval is used as the key for determining which queue to add the
     * runnable.
     * 
     * @param runnable
     *            The element to run when interval expires.
     * @param interval
     *            The queue to add the runnable to.
     * @throws java.lang.RuntimeException
     *             Thrown if an error occurs adding the element to the queue.
     */
    private void schedule(ReadyRunnable runnable, long interval, boolean isReschedule) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("schedule: Adding ready runnable " + runnable
                    + " at interval " + interval);
        }

        m_lock.lock();
        try {
            for (PriorityQueue<ReadyRunnable> queue : m_queues.values()) {
                if (queue.contains(runnable)) {
                    LOG.debug("schedule: element already present in queue");
                    return;
                }
            }

            // TODO: Figure out why "contains" does not work instead
            if (!isReschedule) {
                for (ReadyRunnable runnableInExecutingState : m_executingSet) {
                    if (runnableInExecutingState.equals(runnable)) {
                        LOG.debug("schedule: element currently executing");
                        return;
                    }
                }
            }

            Long key = Long.valueOf(interval);
            PriorityQueue<ReadyRunnable> queue = m_queues.get(key);
            if (queue == null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("schedule: interval queue did not exist, a new one has been created");
                }
                queue = new PriorityQueue<ReadyRunnable>();
            }

            queue.add(runnable);
            m_queues.put(key, queue);
            LOG.debug("schedule: queue element added");
        } finally {
            m_lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void schedule(final long interval, ReadyRunnable runnable) {
        schedule(interval, runnable, false);
    }

    @Override
    public void schedule(long interval, ReadyRunnable runnable,
            boolean isReschedule) {
        final long timeToRun = getCurrentTime() + interval;
        schedule(new ScheduleTimeKeeper(runnable, timeToRun), interval, isReschedule);
    }
    
    /**
     * <p>
     * getCurrentTime
     * </p>
     * 
     * @return a long.
     */
    @Override
    public long getCurrentTime() {
        return System.currentTimeMillis();
    }

    /**
     * <p>
     * start
     * </p>
     */
    @Override
    public synchronized void start() {
        Assert.state(m_workerThread == null || m_status == STOPPED,
                     "The fiber is already running or in the process of stopping");

        m_runner = Executors.newFixedThreadPool(m_maxNumberOfThreadsInPool,
                                                new LogPreservingThreadFactory(
                                                                               getClass().getSimpleName(),
                                                                               m_maxNumberOfThreadsInPool,
                                                                               false));
        m_workerThread = new WorkerThread();
        m_workerThread.start();
        m_consumerThread = new ConsumerThread();
        m_consumerThread.start();
        m_status = STARTING;

        LOG.info("start: scheduler started");
    }

    /**
     * <p>
     * stop
     * </p>
     */
    @Override
    public synchronized void stop() {
        Assert.state(m_workerThread != null, "The fiber has never been started");

        m_status = STOP_PENDING;
        m_workerThread.stop();
        m_consumerThread.stop();
        m_runner.shutdown();

        LOG.info("stop: scheduler stopped");
    }

    /**
     * <p>
     * pause
     * </p>
     */
    @Override
    public synchronized void pause() {
        Assert.state(m_workerThread != null, "The fiber has never been started");
        Assert.state(m_status != STOPPED && m_status != STOP_PENDING,
                     "The fiber is not running or a stop is pending");

        if (m_status == PAUSED) {
            return;
        }

        m_status = PAUSE_PENDING;
        notifyAll();
    }

    /**
     * <p>
     * resume
     * </p>
     */
    @Override
    public synchronized void resume() {
        Assert.state(m_workerThread != null, "The fiber has never been started");
        Assert.state(m_status != STOPPED && m_status != STOP_PENDING,
                     "The fiber is not running or a stop is pending");

        if (m_status == RUNNING) {
            return;
        }

        m_status = RESUME_PENDING;
        notifyAll();
    }

    /**
     * <p>
     * getStatus
     * </p>
     * 
     * @return a int.
     */
    @Override
    public synchronized int getStatus() {
        if (m_workerThread != null && m_workerThread.isAlive() == false) {
            m_status = STOPPED;
        }
        return m_status;
    }

    @Override
    public String getName() {
        return m_schedulerName;
    }

    /**
     * Returns total number of tasks currently scheduled.
     * 
     * @return the sum of all the tasks in the various queues
     */
    public int getNumTasksScheduled() {
        int numTasksScheduled = 0;
        m_lock.lock();
        try {
            for (PriorityQueue<ReadyRunnable> queue : m_queues.values()) {
                numTasksScheduled += queue.size();
            }
        } finally {
            m_lock.unlock();
        }
        return numTasksScheduled;
    }

    /**
     * Returns the pool of threads that are used to executed the runnable
     * instances scheduled by the class' instance.
     * 
     * @return thread pool
     */
    public ExecutorService getRunner() {
        return m_runner;
    }

    public DataGridProvider getDataGridProvider() {
        return m_dataGridProvider;
    }

    private class WorkerThread implements Runnable {
        private Thread m_thread;

        public void start() {
            m_thread = new Thread(this, getName());
            m_thread.start();
        }

        public void stop() {
            m_thread.interrupt();
        }

        public boolean isAlive() {
            if (m_thread != null) {
                return m_thread.isAlive();
            }
            return false;
        }

        @Override
        public void run() {
            synchronized (this) {
                m_status = RUNNING;
            }

            LOG.debug("run: scheduler running");

            // Loop until a fatal exception occurs or until the thread is
            // interrupted.
            for (;;) {
                // Verify and maintain the fiber state
                synchronized (this) {
                    if (m_status != RUNNING && m_status != PAUSED
                            && m_status != PAUSE_PENDING
                            && m_status != RESUME_PENDING) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("run: status = " + m_status
                                    + ", time to exit");
                        }
                        break;
                    }

                    // if paused or pause pending then block
                    while (m_status == PAUSE_PENDING || m_status == PAUSED) {
                        if (m_status == PAUSE_PENDING) {
                            LOG.debug("run: pausing.");
                        }
                        m_status = PAUSED;
                        try {
                            wait();
                        } catch (InterruptedException ex) {
                            // exit
                            break;
                        }
                    }

                    // if resume pending then change to running
                    if (m_status == RESUME_PENDING) {
                        LOG.debug("run: resuming.");

                        m_status = RUNNING;
                    }
                }

                // Limit this section to a single distributed scheduler instance
                // at any time
                m_lock.lock();
                try {
                    // Iterate through the queues
                    for (Entry<Long, PriorityQueue<ReadyRunnable>> entry : m_queues.entrySet()) {
                        PriorityQueue<ReadyRunnable> scheduleQueue = entry.getValue();

                        ReadyRunnable readyRun = null;
                        boolean didModifyScheduleQueue = false;
                        do {
                            readyRun = scheduleQueue.peek();
                            if (readyRun != null) {
                                if (readyRun.isReady()) {
                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug("run: pushing ready runnable "
                                                + readyRun);
                                    }

                                    // Remove it from the scheduler queue
                                    scheduleQueue.remove();
                                    didModifyScheduleQueue = true;

                                    // Push it to the execution queue
                                    m_executionQueue.put(readyRun);

                                    // Mark the task as executing
                                    m_executingSet.add(readyRun);
                                }
                            }
                        } while (readyRun != null && readyRun.isReady());

                        if (didModifyScheduleQueue) {
                            // Push the updated queue back into the map
                            m_queues.put(entry.getKey(), scheduleQueue);
                        }
                    }
                } catch (InterruptedException e) {
                    return; // jump all the way out
                } finally {
                    m_lock.unlock();
                }

                // Sleep for 500ms before checking everything again
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    return;
                }
            }

            LOG.debug("run: scheduler exiting, state = STOPPED");
            synchronized (this) {
                m_status = STOPPED;
            }
        }
    }

    private class ConsumerThread implements Runnable {
        private Thread m_thread;

        public void start() {
            m_thread = new Thread(this, getName());
            m_thread.start();
        }

        public void stop() {
            m_thread.interrupt();
        }

        @Override
        public void run() {
            synchronized (this) {
                m_status = RUNNING;
            }

            for(;;) {
                // Verify and maintain the fiber state
                synchronized (this) {
                    if (m_status != RUNNING && m_status != PAUSED
                            && m_status != PAUSE_PENDING
                            && m_status != RESUME_PENDING) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("run: status = " + m_status
                                    + ", time to exit");
                        }
                        break;
                    }

                    // if paused or pause pending then block
                    while (m_status == PAUSE_PENDING || m_status == PAUSED) {
                        if (m_status == PAUSE_PENDING) {
                            LOG.debug("run: pausing.");
                        }
                        m_status = PAUSED;
                        try {
                            wait();
                        } catch (InterruptedException ex) {
                            // exit
                            break;
                        }
                    }

                    // if resume pending then change to running
                    if (m_status == RESUME_PENDING) {
                        LOG.debug("run: resuming.");

                        m_status = RUNNING;
                    }
                }

                // Consume the queue
                try {
                    ReadyRunnable readyRun = null;
                    do {
                        readyRun = m_executionQueue.poll(500, TimeUnit.MILLISECONDS);
                        if (readyRun != null) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("run: got ready runnable " + readyRun);
                            }

                            if (readyRun instanceof ScheduleTimeKeeper) {
                                ((ScheduleTimeKeeper) readyRun).removeFromSetAfterRun(m_executingSet);
                            }

                            // Set the scheduler instance
                            if (readyRun instanceof SchedulerAware) {
                                ((SchedulerAware) readyRun).setScheduler(DistributedScheduler.this);
                            }

                            // Set the data grid instance
                            if (readyRun instanceof DataGridProviderAware) {
                                ((DataGridProviderAware) readyRun).setDataGridProvider(m_dataGridProvider);
                            }

                            // Add runnable to the executor queue
                            m_runner.execute(readyRun);
                        }
                    } while (readyRun != null);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }
}
