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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScheduleTimeKeeper implements ClusterRunnable, SchedulerAware, Timer {
    private static final long serialVersionUID = 8488467368898905519L;
    protected final long m_timeToRun;
    protected final ReadyRunnable m_runnable;
    protected final long m_schedulerRevision;
    protected transient Scheduler m_scheduler;

    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(ScheduleTimeKeeper.class);

    public ScheduleTimeKeeper(final ReadyRunnable runnable, final long timeToRun, final long schedulerRevision) {
        m_runnable = runnable;
        m_timeToRun = timeToRun;
        m_schedulerRevision = schedulerRevision;
    }

    @Override
    public boolean isReady() {
        return getCurrentTime() >= m_timeToRun && m_runnable.isReady();
    }

    @Override
    public void run() {
        m_runnable.run();
        rescheduleIfRequested();
    }

    protected void rescheduleIfRequested() {
        if (m_runnable instanceof Reschedulable) {
            Reschedulable re = (Reschedulable) m_runnable;
            if (!re.rescheduleAfterRun()) {
                return;
            }

            if (m_schedulerRevision != m_scheduler.getRevision()) {
                LOG.debug("A reschedule was requested for {}, but the scheduler has changed revisions from {} to {}",
                    re, m_schedulerRevision, m_scheduler.getRevision());
                return;
            }

            LOG.debug("Rescheduling {}.", re);
            m_scheduler.schedule(re.getInterval(), m_runnable);
        }
    }

    @Override
    public long getCurrentTime() {
        return System.currentTimeMillis();
    }

    @Override
    public String toString() {
        return m_runnable.toString() + " (ready in "
                + Math.max(0, m_timeToRun - getCurrentTime()) + "ms)";
    }

    @Override
    public void setScheduler(Scheduler scheduler) {
        m_scheduler = scheduler;
        if (m_runnable instanceof SchedulerAware) {
            ((SchedulerAware) m_runnable).setScheduler(scheduler);
        }
    }
}
