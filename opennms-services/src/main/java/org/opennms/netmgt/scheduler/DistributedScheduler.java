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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.Lock;

import org.opennms.core.concurrent.LogPreservingThreadFactory;
import org.opennms.core.grid.AtomicLong;
import org.opennms.core.grid.DataGridProvider;
import org.opennms.core.grid.DataGridProviderFactory;
import org.opennms.core.grid.concurrent.DistributedExecutionVisitor;
import org.opennms.core.grid.concurrent.DistributedExecutors;
import org.opennms.core.grid.concurrent.DistributedThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Distributed scheduler that allows multiple instances with the same name to
 * operate in unison.
 * 
 * @author jwhite
 */
public class DistributedScheduler extends AbstractScheduler implements
        DistributedExecutionVisitor {

    /**
     * Unique name for this scheduler.
     */
    private String m_schedulerName;

    /**
     * Data grid provider.
     */
    private DataGridProvider m_dataGridProvider;

    /**
     * Distributed lock used to coordinate among instance of the same
     * scheduler.
     */
    private Lock m_lock;

    /**
     * Distributed set used to the store the ids of the queues.
     */
    private Set<QueueId> m_queueIds;

    /**
     * Distributed long used to identify the revision of the scheduler.
     * 
     * The revision is increased when a call to reset() is made in order to
     * prevent any executing tasks from re-scheduling themselves via the
     * Reschedulable interface. This logic is performed in the
     * ScheduleTimeKeeper class.
     */
    private AtomicLong m_revision;

    /**
     * Distributed long used to keep track of the total number of tasks that
     * have been executed globally;
     */
    private AtomicLong m_tasksExecutedGlobal;

    /**
     * Counter used to keep track of the number of tasks that have been
     * executed locally.
     */
    private long m_tasksExecutedLocal = 0;

    /**
     * Map used to index the queues by interval.
     */
    private Map<Long, BlockingQueue<ReadyRunnable>> m_queues;

    /**
     * Prefix for distributed object keys.
     */
    public static final String SCHEDULER_GRID_NAME_PREFIX = "Scheduler.";

    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(DistributedScheduler.class);

    /**
     * Constructs a new distributed scheduler.
     * 
     * @param name
     *            name of the scheduler
     * @param maxSize
     *            maximum number of threads per schedule instance
     */
    public DistributedScheduler(String name, int maxSize) {
        this(name, maxSize, null);
    }

    /**
     * Constructs a new distributed scheduler.
     * 
     * @param name
     *            name of the scheduler
     * @param maxSize
     *            maximum number of threads per schedule instance
     * @param dataGridProvider
     *            data grid provider
     */
    public DistributedScheduler(String name, int maxSize,
            DataGridProvider dataGridProvider) {
        m_status = START_PENDING;
        m_queues = new HashMap<Long, BlockingQueue<ReadyRunnable>>();

        if (dataGridProvider == null) {
            m_dataGridProvider = DataGridProviderFactory.getInstance();
        } else {
            m_dataGridProvider = dataGridProvider;
        }

        // Initialize the distributed objects
        m_schedulerName = name;
        m_revision = m_dataGridProvider.getAtomicLong(SCHEDULER_GRID_NAME_PREFIX
                + m_schedulerName + "Revision");
        m_tasksExecutedGlobal = m_dataGridProvider.getAtomicLong(SCHEDULER_GRID_NAME_PREFIX
                + m_schedulerName + "Executed");
        m_lock = m_dataGridProvider.getLock(SCHEDULER_GRID_NAME_PREFIX
                + m_schedulerName + "Lock");
        m_queueIds = m_dataGridProvider.getSet(SCHEDULER_GRID_NAME_PREFIX
                + m_schedulerName + "Set");

        // Populate our local map with the different queues
        m_lock.lock();
        try {
            addMissingQueuesToLocalMap();
        } finally {
            m_lock.unlock();
        }

        // Initialize the distributed executor
        ThreadFactory threadFactory = new LogPreservingThreadFactory(
                                                                     getClass().getSimpleName(),
                                                                     maxSize,
                                                                     true);

        LOG.debug("Creating distributed executor called {} with {} threads",
                  getExecutorName(), maxSize);
        m_runner = DistributedExecutors.newDistributedExecutor(maxSize,
                                                               threadFactory,
                                                               m_dataGridProvider,
                                                               getExecutorName(),
                                                               this);
    }

    /** {@inheritDoc} */
    @Override
    public void schedule(final long interval, ReadyRunnable runnable) {
        // Wrap the runnable in a time keeper and schedule it
        final long timeToRun = getCurrentTime() + interval;
        schedule(interval, new DistributedScheduleTimeKeeper(runnable,
                                                             timeToRun,
                                                             getRevision()));
    }

    /**
     * Schedules a runnable wrapped in a time keeper instance
     * 
     * @param interval
     *            the queue to add the runnable to
     * @param timeKeeper
     *            wrapper for the runnable
     */
    private void schedule(final long interval,
            final ScheduleTimeKeeper timeKeeper) {
        LOG.debug("Adding ready runnable {} at interval {}", timeKeeper,
                  interval);

        m_lock.lock();
        try {
            Long queueInterval = Long.valueOf(interval);
            BlockingQueue<ReadyRunnable> queue = m_queues.get(queueInterval);
            if (queue == null) {
                // Create a new queue if none previously existed
                QueueId queueId = new QueueId(m_schedulerName, queueInterval);
                m_queueIds.add(queueId);

                // Fetch the distributed queue from the grid provider
                queue = m_dataGridProvider.getQueue(queueId.getId());
                m_queues.put(queueInterval, queue);

                LOG.debug("Interval queue did not exist, a new one has been created");
            }

            // Add the element to the queue
            queue.add(timeKeeper);
            LOG.debug("Queue element added");
        } finally {
            m_lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override
    public String getName() {
        return m_schedulerName;
    }

    /** {@inheritDoc} */
    @Override
    public int getScheduled() {
        int numTasksScheduled = 0;
        m_lock.lock();
        try {
            addMissingQueuesToLocalMap();
            for (BlockingQueue<ReadyRunnable> queue : m_queues.values()) {
                numTasksScheduled += queue.size();
            }
        } finally {
            m_lock.unlock();
        }
        return numTasksScheduled;
    }

    /**
     * The main method of the scheduler. This method is responsible for
     * checking the runnable queues for ready objects and then enqueuing them
     * into the thread pool for execution.
     */
    @Override
    public void run() {
        synchronized (this) {
            m_status = RUNNING;
        }

        LOG.debug("Scheduler running");

        // Loop until a fatal exception occurs or until the thread is
        // interrupted.
        for (;;) {
            // Verify and maintain the fiber state
            synchronized (this) {
                if (m_status != RUNNING && m_status != PAUSED
                        && m_status != PAUSE_PENDING
                        && m_status != RESUME_PENDING) {
                    LOG.debug("run: status = {}, time to exit", m_status);
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
                // Retrieve any missing queues that might have been created on
                // other nodes
                addMissingQueuesToLocalMap();

                // Iterate through the queues
                for (Entry<Long, BlockingQueue<ReadyRunnable>> entry : m_queues.entrySet()) {
                    BlockingQueue<ReadyRunnable> scheduleQueue = entry.getValue();

                    ReadyRunnable readyRun = null;
                    do {
                        readyRun = scheduleQueue.peek();
                        if (readyRun != null) {
                            if (readyRun.isReady()) {
                                LOG.debug("run: pushing ready runnable {}",
                                          readyRun);

                                // Remove it from the scheduler queue
                                scheduleQueue.remove();

                                // Add runnable to the executor queue
                                m_runner.execute(readyRun);
                            }
                        }
                    } while (readyRun != null && readyRun.isReady());
                }
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

        LOG.debug("Scheduler exiting, state = STOPPED");
        synchronized (this) {
            m_status = STOPPED;
        }
    }

    /**
     * Sets the required properties on the runnable if requested.
     * 
     * These properties are omitted until the runnable is about to execute
     * (since we don't know where it will run.)
     */
    @Override
    public void beforeExecute(Thread t, Runnable runnable) {
        // Set the scheduler instance
        if (runnable instanceof SchedulerAware) {
            ((SchedulerAware) runnable).setScheduler(DistributedScheduler.this);
        }
    }

    /**
     * Used to gather statistics.
     */
    @Override
    public void afterExecute(Runnable r, Throwable t) {
        // Increment our local and global counters
        m_tasksExecutedLocal++;
        m_tasksExecutedGlobal.incrementAndGet();
    }

    /** {@inheritDoc} */
    @Override
    public void reset() {
        // Prevent any queued tasks from being pushed to the executor
        m_lock.lock();
        try {
            // Remove any pending tasks from the executor queue
            BlockingQueue<Runnable> workQueue = m_dataGridProvider.getQueue(getExecutorQueueName());
            workQueue.clear();

            // Increment the revision - preventing any tasks from being
            // rescheduled via the Reschedulable interface
            m_revision.incrementAndGet();

            // Clear the queues
            addMissingQueuesToLocalMap();
            for (BlockingQueue<ReadyRunnable> queue : m_queues.values()) {
                queue.clear();
            }
        } finally {
            m_lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override
    public long getRevision() {
        return m_revision.get();
    }

    /**
     * Adds any queues that have been created on other instances to the local
     * queue map
     */
    private void addMissingQueuesToLocalMap() {
        BlockingQueue<ReadyRunnable> queue;
        for (QueueId queueId : m_queueIds) {
            if (!m_queues.containsKey(queueId.getInterval())) {
                queue = m_dataGridProvider.getQueue(queueId.getId());
                m_queues.put(queueId.getInterval(), queue);
            }
        }
    }

    /**
     * Returns the name of the distributed queue used by the executor for this
     * scheduler.
     * 
     * @return distributed queue name
     */
    private String getExecutorName() {
        return SCHEDULER_GRID_NAME_PREFIX + m_schedulerName + ".Executor";
    }

    private String getExecutorQueueName() {
        return DistributedThreadPoolExecutor.getQueueName(getExecutorName());
    }

    /**
     * Tuple used to store the queue id and the queue interval in a set.
     * 
     * @author jwhite
     */
    private static class QueueId implements Serializable {
        private static final long serialVersionUID = 2483527172361259087L;
        private String m_schedulerName;
        private Long m_queueInterval;

        public QueueId(String schedulerName, Long queueInterval) {
            m_schedulerName = schedulerName;
            m_queueInterval = queueInterval;
        }

        public Long getInterval() {
            return m_queueInterval;
        }

        public String getId() {
            return String.format("%s%s-%d", SCHEDULER_GRID_NAME_PREFIX,
                                 m_schedulerName, m_queueInterval);
        }

        public String toString() {
            return getId();
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime
                    * result
                    + ((m_queueInterval == null) ? 0
                                                : m_queueInterval.hashCode());
            result = prime
                    * result
                    + ((m_schedulerName == null) ? 0
                                                : m_schedulerName.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            QueueId other = (QueueId) obj;
            if (m_queueInterval == null) {
                if (other.m_queueInterval != null)
                    return false;
            } else if (!m_queueInterval.equals(other.m_queueInterval))
                return false;
            if (m_schedulerName == null) {
                if (other.m_schedulerName != null)
                    return false;
            } else if (!m_schedulerName.equals(other.m_schedulerName))
                return false;
            return true;
        }
    }

    /** {@inheritDoc} */
    @Override
    public long getGlobalTasksExecuted() {
        return m_tasksExecutedGlobal.get();
    }

    /** {@inheritDoc} */
    @Override
    public long getLocalTasksExecuted() {
        return m_tasksExecutedLocal;
    }
}
