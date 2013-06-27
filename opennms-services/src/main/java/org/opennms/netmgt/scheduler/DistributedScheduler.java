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

import java.lang.reflect.UndeclaredThrowableException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.Lock;

import org.opennms.core.concurrent.LogPreservingThreadFactory;
import org.opennms.core.fiber.PausableFiber;
import org.opennms.core.grid.DataGridProvider;
import org.opennms.core.grid.DataGridProviderFactory;
import org.opennms.core.grid.Message;
import org.opennms.core.grid.MessageListener;
import org.opennms.core.grid.Topic;
import org.opennms.core.queue.FifoQueueImpl;
import org.opennms.core.utils.ThreadCategory;
import org.springframework.util.Assert;

/**
 * This class implements a simple scheduler to ensure the polling occurs at
 * the expected intervals. The scheduler employees a dynamic thread pool that
 * adjust to the load until a maximum thread count is reached.
 * 
 * @author jwhite
 * @author <a href="mailto:mike@opennms.org">Mike Davidson </a>
 * @author <a href="mailto:weave@oculan.com">Brian Weaver </a>
 * @author <a href="http://www.opennms.org/">OpenNMS </a>
 */
public class DistributedScheduler implements Runnable, PausableFiber,
        Scheduler, MessageListener<TaskScheduledEvent> {
    /**
     * The map of queues that contain ReadyRunnable instances. The queues are
     * mapped according to the interval of scheduling.
     */
    private Map<Long, PeekableFifoQueue<ReadyRunnable>> m_queues;

    /**
     * The pool of threads that are used to executed the runnable instances
     * scheduled by the class' instance.
     */
    private ExecutorService m_runner;

    /**
     * The status for this fiber.
     */
    private int m_status;

    /**
     * The worker thread that executes this instance.
     */
    private Thread m_worker;

    /**
     * Data grid provider.
     */
    private DataGridProvider m_dataGridProvider;

    /**
     * Distributed lock.
     */
    private Lock m_lock;

    /**
     * Distributed topic.
     */
    private Topic<TaskScheduledEvent> m_topic;

    /**
     * Unique name.
     */
    private String m_name;

    private int m_maxSize;

    public DataGridProvider getDataGridProvider() {
        return m_dataGridProvider;
    }

    /**
     * This queue extends the standard FIFO queue instance so that it is
     * possible to peek at an instance without removing it from the queue.
     * 
     */
    public static final class PeekableFifoQueue<T> extends FifoQueueImpl<T> {
        private static final long serialVersionUID = -7776794433929593721L;

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

    public DistributedScheduler(String name, int maxSize) {
        this(name, maxSize, null);
    }

    public DistributedScheduler(String name, int maxSize,
            DataGridProvider dataGridProvider) {
        m_status = START_PENDING;
        m_maxSize = maxSize;
        m_worker = null;

        if (dataGridProvider == null) {
            m_dataGridProvider = DataGridProviderFactory.getInstance();
        } else {
            m_dataGridProvider = dataGridProvider;
        }

        m_name = name;
        m_queues = m_dataGridProvider.getMap(name);
        m_lock = m_dataGridProvider.getLock(name);
        m_topic = m_dataGridProvider.getTopic(name);
        m_topic.addMessageListener(this);
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
    private void schedule(ReadyRunnable runnable, long interval) {
        if (log().isDebugEnabled()) {
            log().debug("schedule: Adding ready runnable " + runnable
                                + " at interval " + interval);
        }

        m_lock.lock();
        try {
            Long key = Long.valueOf(interval);
            PeekableFifoQueue<ReadyRunnable> queue = m_queues.get(key);
            if (queue == null) {
                if (log().isDebugEnabled()) {
                    log().debug("schedule: interval queue did not exist, a new one has been created");
                }
                queue = new PeekableFifoQueue<ReadyRunnable>();
            }

            try {
                queue.add(runnable);
                m_queues.put(key, queue);
                m_topic.publish(new TaskScheduledEvent());
                log().debug("schedule: queue element added, notification performed");
            } catch (InterruptedException e) {
                log().info("schedule: failed to add new ready runnable instance "
                                   + runnable + " to scheduler: " + e, e);
                Thread.currentThread().interrupt();
            }
        } finally {
            m_lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void schedule(final long interval, ReadyRunnable runnable) {
        final long timeToRun = getCurrentTime() + interval;
        schedule(new ScheduleTimeKeeper(runnable, timeToRun), interval);
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
        Assert.state(m_worker == null || m_status == STOPPED,
                     "The fiber is already running or in the process of stopping");

        m_runner = Executors.newFixedThreadPool(m_maxSize,
                                                new LogPreservingThreadFactory(
                                                                               getClass().getSimpleName(),
                                                                               m_maxSize,
                                                                               false));
        m_worker = new Thread(this, getName());
        m_worker.start();
        m_status = STARTING;

        log().info("start: scheduler started");
    }

    /**
     * <p>
     * stop
     * </p>
     */
    @Override
    public synchronized void stop() {
        Assert.state(m_worker != null, "The fiber has never been started");

        m_status = STOP_PENDING;
        m_worker.interrupt();
        m_runner.shutdown();

        log().info("stop: scheduler stopped");
    }

    /**
     * <p>
     * pause
     * </p>
     */
    @Override
    public synchronized void pause() {
        Assert.state(m_worker != null, "The fiber has never been started");
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
        Assert.state(m_worker != null, "The fiber has never been started");
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
        if (m_worker != null && m_worker.isAlive() == false) {
            m_status = STOPPED;
        }
        return m_status;
    }

    @Override
    public String getName() {
        return m_name;
    }

    /**
     * Returns total number of elements currently scheduled.
     * 
     * @return the sum of all the elements in the various queues
     */
    public int getScheduled() {
        int numElementsScheduled = 0;
        for (PeekableFifoQueue<ReadyRunnable> queue : m_queues.values()) {
            numElementsScheduled += queue.size();
        }
        return numElementsScheduled;
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

    @Override
    public void onMessage(Message<TaskScheduledEvent> message) {
        synchronized (this) {
            notifyAll();
        }
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

        log().debug("run: scheduler running");

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
                    if (log().isDebugEnabled()) {
                        log().debug("run: status = " + m_status
                                            + ", time to exit");
                    }
                    break;
                }

                // if paused or pause pending then block
                while (m_status == PAUSE_PENDING || m_status == PAUSED) {
                    if (m_status == PAUSE_PENDING) {
                        log().debug("run: pausing.");
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
                    log().debug("run: resuming.");

                    m_status = RUNNING;
                }

                if (getScheduled() == 0) {
                    try {
                        log().debug("run: no ready runnables scheduled, waiting...");
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

            // Limit this section to a single distributed scheduler instance
            // at any time
            m_lock.lock();
            try {
                /*
                 * Get an iterator so that we can cycle through the queue
                 * elements.
                 */
                for (Entry<Long, PeekableFifoQueue<ReadyRunnable>> entry : m_queues.entrySet()) {
                    /*
                     * Peak for Runnable objects until there are no more ready
                     * runnables.
                     * 
                     * Also, only go through each queue once! if we didn't add
                     * a count then it would be possible to starve other
                     * queues.
                     */
                    PeekableFifoQueue<ReadyRunnable> in = entry.getValue();
                    ReadyRunnable readyRun = null;
                    int maxLoops = in.size();
                    do {
                        try {
                            readyRun = in.peek();
                            if (readyRun != null) {
                                if (readyRun.isReady()) {
                                    if (log().isDebugEnabled()) {
                                        log().debug("run: found ready runnable "
                                                            + readyRun);
                                    }

                                    /*
                                     * Pop the interface/readyRunnable from
                                     * the queue for execution.
                                     */
                                    in.remove();

                                    // Push the updated queue back into the
                                    // map
                                    m_queues.put(entry.getKey(), in);

                                    // Set the scheduler instance
                                    if (readyRun instanceof SchedulerAware) {
                                        ((SchedulerAware) readyRun).setScheduler(this);
                                    }

                                    // Set the data grid instance
                                    if (readyRun instanceof DataGridProviderAware) {
                                        ((DataGridProviderAware) readyRun).setDataGridProvider(m_dataGridProvider);
                                    }
                                    // Add runnable to the execution queue
                                    m_runner.execute(readyRun);
                                    ++runned;
                                }
                            }
                        } catch (InterruptedException e) {
                            return; // jump all the way out
                        } catch (RejectedExecutionException e) {
                            throw new UndeclaredThrowableException(e);
                        }

                    } while (readyRun != null && readyRun.isReady()
                            && --maxLoops > 0);
                }
            } finally {
                m_lock.unlock();
            }

            /*
             * Wait for 1 second if there were no runnables executed during
             * this loop, otherwise just start over.
             */
            synchronized (this) {
                if (runned == 0) {
                    try {
                        wait(1000);
                    } catch (InterruptedException ex) {
                        break; // exit for loop
                    }
                }
            }

        }

        log().debug("run: scheduler exiting, state = STOPPED");
        synchronized (this) {
            m_status = STOPPED;
        }

    }

    private ThreadCategory log() {
        return ThreadCategory.getInstance(getClass());
    }
}
