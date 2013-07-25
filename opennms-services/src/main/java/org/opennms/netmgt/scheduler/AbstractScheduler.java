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

import java.util.concurrent.ExecutorService;

import org.opennms.core.fiber.PausableFiber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

/**
 * Methods that are common to different scheduler implementations.
 * 
 * @author jwhite
 */
public abstract class AbstractScheduler implements Runnable, PausableFiber,
        Scheduler {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractScheduler.class);

    /**
     * The worker thread that executes this instance.
     */
    private Thread m_worker;

    /**
     * The status for this fiber.
     */
    protected int m_status;

    /**
     * The pool of threads that are used to executed the runnable instances
     * scheduled by the class' instance.
     */
    protected ExecutorService m_runner;

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
        Assert.state(m_worker == null,
                     "The fiber has already run or is running");

        m_worker = new Thread(this, getName());
        m_worker.start();
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
        Assert.state(m_worker != null, "The fiber has never been started");

        m_status = STOP_PENDING;
        m_worker.interrupt();
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
}
