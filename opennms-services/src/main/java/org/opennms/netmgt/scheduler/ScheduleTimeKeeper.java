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

import java.util.Set;

import org.opennms.core.grid.DataGridProvider;

public class ScheduleTimeKeeper implements ClusterRunnable, Timer, DataGridProviderAware, Comparable<ScheduleTimeKeeper> {
    private static final long serialVersionUID = 1073282881016278947L;
    private final long m_timeToRun;
    private final ReadyRunnable m_runnable;
    private transient Set<ReadyRunnable> m_executingSet = null;

    public ScheduleTimeKeeper(final ReadyRunnable runnable, final long timeToRun) {
        m_runnable = runnable;
        m_timeToRun = timeToRun;
    }

    public void removeFromSetAfterRun(final Set<ReadyRunnable> executingSet) {
        m_executingSet = executingSet;
    }

    @Override
    public boolean isReady() {
        return getCurrentTime() >= m_timeToRun && m_runnable.isReady();
    }

    @Override
    public void run() {
        m_runnable.run();
        if (m_executingSet != null) {
            m_executingSet.remove(this);
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
        if (m_runnable instanceof SchedulerAware) {
            ((SchedulerAware) m_runnable).setScheduler(scheduler);
        }
    }

    @Override
    public void setDataGridProvider(DataGridProvider dataGridProvider) {
        if (m_runnable instanceof DataGridProviderAware) {
            ((DataGridProviderAware) m_runnable).setDataGridProvider(dataGridProvider);
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((m_runnable == null) ? 0 : m_runnable.hashCode());
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
        ScheduleTimeKeeper other = (ScheduleTimeKeeper) obj;
        if (m_runnable == null) {
            if (other.m_runnable != null)
                return false;
        } else if (!m_runnable.equals(other.m_runnable))
            return false;
        return true;
    }

    @Override
    public int compareTo(ScheduleTimeKeeper o) {
        return Long.compare(o.m_timeToRun, m_timeToRun);
    }
}
