/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2006-2012 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2012 The OpenNMS Group, Inc.
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

import java.lang.reflect.UndeclaredThrowableException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;

import org.opennms.core.concurrent.LogPreservingThreadFactory;
import org.opennms.core.queue.FifoQueueImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements a simple scheduler to ensure the polling occurs at
 * the expected intervals. The scheduler employees a dynamic thread pool that
 * adjust to the load until a maximum thread count is reached.
 * 
 * @author <a href="mailto:mike@opennms.org">Mike Davidson </a>
 * @author <a href="mailto:weave@oculan.com">Brian Weaver </a>
 * @author <a href="http://www.opennms.org/">OpenNMS </a>
 */
public class LegacyScheduler extends AbstractScheduler {

    /**
     * The map of queue that contain {@link ReadyRunnable ready runnable}
     * instances. The queues are mapped according to the interval of
     * scheduling.
     */
    private Map<Long, BlockingQueue<ReadyRunnable>> m_queues;

    /**
     * The total number of elements currently scheduled. This should be the
     * sum of all the elements in the various queues.
     */
    private int m_scheduled;

    /**
     * The current revision.
     */
    private volatile long m_revision = 0;

    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(LegacyScheduler.class);

    private int m_status;

    /**
     * Used to keep track of the number of tasks that have been executed.
     */
    private long m_numTasksExecuted = 0;

    /**
     * This queue extends the standard FIFO queue instance so that it is
     * possible to peek at an instance without removing it from the queue.
     * 
     */
    public static final class PeekableFifoQueue<T> extends FifoQueueImpl<T> {
        /**
         * This method allows the caller to peek at the next object that would
         * be returned on a <code>remove</code> call. If the queue is
         * currently empty then the caller is blocked until an object is put
         * into the queue.
         * 
         * @return The object that would be returned on the next call to
         *         <code>remove</code>.
         * 
         * @throws java.lang.InterruptedException
         *             Thrown if the thread is interrupted.
         * @throws org.opennms.core.queue.FifoQueueException
         *             Thrown if an error occurs removing an item from the
         *             queue.
         */
        public T peek() throws InterruptedException {
            return m_delegate.peek();
        }
    }

    /**
     * Constructs a new instance of the scheduler. The maximum number of
     * executable threads is specified in the constructor. The executable
     * threads are part of a runnable thread pool where the scheduled
     * runnables are executed.
     * 
     * @param parent
     *            String prepended to "Scheduler" to create fiber name
     * @param maxSize
     *            The maximum size of the thread pool.
     */
    public LegacyScheduler(final String parent, final int maxSize) {
        m_status = START_PENDING;
        m_runner = Executors.newFixedThreadPool(maxSize, new LogPreservingThreadFactory(parent, maxSize, true));
        m_queues = new ConcurrentSkipListMap<Long, BlockingQueue<ReadyRunnable>>();
        m_scheduled = 0;
    }

    /**
     * Constructs a new instance of the scheduler. The maximum number of
     * executable threads is specified in the constructor. The executable
     * threads are part of a runnable thread pool where the scheduled
     * runnables are executed.
     * 
     * @param parent
     *            String prepended to "Scheduler" to create fiber name
     * @param maxSize
     *            The maximum size of the thread pool.
     * @param lowMark
     *            The low water mark ratios of thread size to threads when
     *            threads are stopped.
     * @param hiMark
     *            The high water mark ratio of thread size to threads when
     *            threads are started.
     */
    public LegacyScheduler(String parent, int maxSize, float lowMark,
            float hiMark) {
        this(parent, maxSize);
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
    public synchronized void schedule(ReadyRunnable runnable, long interval) {
        LOG.debug("schedule: Adding ready runnable {} at interval {}",
                  runnable, interval);

        Long key = Long.valueOf(interval);
        if (!m_queues.containsKey(key)) {
            LOG.debug("schedule: interval queue did not exist, a new one has been created");
            m_queues.put(key, new LinkedBlockingQueue<ReadyRunnable>());
        }

        m_queues.get(key).add(runnable);
        if (m_scheduled++ == 0) {
            LOG.debug("schedule: queue element added, calling notify all since none were scheduled");
            notifyAll();
        } else {
            LOG.debug("schedule: queue element added, notification not performed");
        }
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void schedule(long interval,
            final ReadyRunnable runnable) {
        final long timeToRun = getCurrentTime() + interval;
        schedule(new ScheduleTimeKeeper(runnable, timeToRun, getRevision()),
                 interval);
    }

    /** {@inheritDoc} */
    @Override
    public String getName() {
        return m_runner.toString();
    }

    /** {@inheritDoc} */
    @Override
    public int getScheduled() {
        return m_scheduled;
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

        LOG.debug("run: scheduler running");

        /*
         * Loop until a fatal exception occurs or until the thread is
         * interrupted.
         */
        for (;;) {
            /*
             * Block if there is nothing in the queue(s). When something is
             * added to the queue it signals us to wakeup.
             */
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

                if (m_scheduled == 0) {
                    try {
                        LOG.debug("run: no ready runnables scheduled, waiting...");
                        wait();
                    } catch (InterruptedException ex) {
                        break;
                    }
                }
            }

            /*
             * Cycle through the queues checking for what's ready to run. The
             * queues are keyed by the interval, but the mapped elements are
             * peekable fifo queues.
             */
            int runned = 0;
            synchronized (m_queues) {
                /*
                 * Get an iterator so that we can cycle through the queue
                 * elements.
                 */
                for (Entry<Long, BlockingQueue<ReadyRunnable>> entry : m_queues.entrySet()) {
                    /*
                     * Peak for Runnable objects until there are no more ready
                     * runnables.
                     * 
                     * Also, only go through each queue once! if we didn't add
                     * a count then it would be possible to starve other
                     * queues.
                     */
                    BlockingQueue<ReadyRunnable> in = entry.getValue();
                    ReadyRunnable readyRun = null;
                    int maxLoops = in.size();
                    do {
                        try {
                            readyRun = in.peek();
                            if (readyRun != null && readyRun.isReady()) {
                                LOG.debug("run: found ready runnable {}",
                                          readyRun);

                                /*
                                 * Pop the interface/readyRunnable from the
                                 * queue for execution.
                                 */
                                in.remove();

                                // Set the scheduler if the interface is
                                // implemented
                                if (readyRun instanceof SchedulerAware) {
                                    ((SchedulerAware) readyRun).setScheduler(this);
                                }

                                // Add runnable to the execution queue
                                m_runner.execute(readyRun);
                                ++runned;

                                // Increment the execution counter
                                ++m_numTasksExecuted;
                            }
                        } catch (RejectedExecutionException e) {
                            throw new UndeclaredThrowableException(e);
                        }

                    } while (readyRun != null && readyRun.isReady()
                            && --maxLoops > 0);
                }
            }

            /*
             * Wait for 1 second if there were no runnables executed during
             * this loop, otherwise just start over.
             */
            synchronized (this) {
                m_scheduled -= runned;
                if (runned == 0) {
                    try {
                        wait(1000);
                    } catch (InterruptedException ex) {
                        break; // exit for loop
                    }
                }
            }

        }

        LOG.debug("run: scheduler exiting, state = STOPPED");
        synchronized (this) {
            m_status = STOPPED;
        }
    }

    /** {@inheritDoc} */
    @Override
    public long getRevision() {
        return m_revision;
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void stop() {
        m_revision++;
        super.stop();
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void reset() {
        // Prevent any queued tasks from being pushed to the executor
        synchronized (m_queues) {
            // Increment the revision - preventing any tasks from being
            // rescheduled via the Reschedulable interface
            m_revision++;

            // Clear the queues
            m_queues = new ConcurrentSkipListMap<Long, BlockingQueue<ReadyRunnable>>();
            m_scheduled = 0;
        }
    }

    /** {@inheritDoc} */
    @Override
    public long getGlobalTasksExecuted() {
        return getLocalTasksExecuted();
    }

    /** {@inheritDoc} */
    @Override
    public long getLocalTasksExecuted() {
	return m_numTasksExecuted;
    }
}
