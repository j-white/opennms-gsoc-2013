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

import org.opennms.core.grid.DataGridProvider;
import org.opennms.core.grid.DataGridProviderAware;

public class DistributedScheduleTimeKeeper extends ScheduleTimeKeeper
        implements ClusterRunnable, DataGridProviderAware,
        Comparable<DistributedScheduleTimeKeeper> {

    private static final long serialVersionUID = 1073282881016278947L;

    public DistributedScheduleTimeKeeper(ReadyRunnable runnable,
            long timeToRun, long schedulerRevision) {
        super(runnable, timeToRun, schedulerRevision);
    }

    @Override
    public void setDataGridProvider(DataGridProvider dataGridProvider) {
        if (m_runnable instanceof DataGridProviderAware) {
            ((DataGridProviderAware) m_runnable).setDataGridProvider(dataGridProvider);
        }
    }

    @Override
    public int compareTo(DistributedScheduleTimeKeeper o) {
        return Long.valueOf(o.m_timeToRun).compareTo(m_timeToRun);
    }
}
