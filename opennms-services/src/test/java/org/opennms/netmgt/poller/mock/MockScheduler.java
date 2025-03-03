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

package org.opennms.netmgt.poller.mock;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.opennms.netmgt.scheduler.ReadyRunnable;
import org.opennms.netmgt.scheduler.Reschedulable;
import org.opennms.netmgt.scheduler.Scheduler;


public class MockScheduler implements Scheduler {
    
    private MockTimer m_timer;
    /*
     * TODO: Use it or loose it.
     * Commented out because it is not currently used in this monitor
     */
    //private long m_currentTime = 0;
    private SortedMap<Long, List<ReadyRunnable>> m_scheduleEntries = new TreeMap<Long, List<ReadyRunnable>>();

    /**
     * Used to keep track of the number of tasks that have been executed.
     */
    private long m_numTasksExecuted = 0;

    public MockScheduler() {
        this(new MockTimer());
    }
    
    public MockScheduler(MockTimer timer) {
        m_timer = timer;
    }

    
    @Override
    public void schedule(long interval, ReadyRunnable schedule) {
        Long nextTime = Long.valueOf(getCurrentTime()+interval);
        //MockUtil.println("Scheduled "+schedule+" for "+nextTime);
        List<ReadyRunnable> entries = m_scheduleEntries.get(nextTime);
        if (entries == null) {
            entries = new LinkedList<ReadyRunnable>();
            m_scheduleEntries.put(nextTime, entries);
        }
            
        entries.add(schedule);
    }
    
    public int getEntryCount() {
        return m_scheduleEntries.size();
    }
    
    public Map<Long, List<ReadyRunnable>> getEntries() {
        return m_scheduleEntries;
    }
    
    public long getNextTime() {
        if (m_scheduleEntries.isEmpty()) {
            throw new IllegalStateException("Nothing scheduled");
        }

        Long nextTime = m_scheduleEntries.firstKey();
        return nextTime.longValue();
    }
    
    public long next() {
        if (m_scheduleEntries.isEmpty()) {
            throw new IllegalStateException("Nothing scheduled");
        }
        
        Long nextTime = m_scheduleEntries.firstKey();
        List<ReadyRunnable> entries = m_scheduleEntries.get(nextTime);
        ReadyRunnable runnable = entries.get(0);
        m_timer.setCurrentTime(nextTime.longValue());
        entries.remove(0);
        if (entries.isEmpty()) {
            m_scheduleEntries.remove(nextTime);
        }
        runnable.run();

        // Reschedule the task automatically if requested
        if (runnable instanceof Reschedulable) {
            Reschedulable reschedulable = (Reschedulable)runnable;
            if (((Reschedulable) runnable).rescheduleAfterRun()) {
                schedule(reschedulable.getInterval(), runnable);
            }
        }

        return getCurrentTime();
    }
    
    public long tick(int step) {
        if (m_scheduleEntries.isEmpty()) {
            throw new IllegalStateException("Nothing scheduled");
        }
        
        long endTime = getCurrentTime()+step;
        while (getNextTime() <= endTime) {
            next();
        }
        
        m_timer.setCurrentTime(endTime);
        return getCurrentTime();
    }

    @Override
    public long getCurrentTime() {
        return m_timer.getCurrentTime();
    }

    @Override
	public void start() {
		
	}

    @Override
	public void stop() {
	}

    @Override
	public void pause() {
	}

    @Override
	public void resume() {
	}

    @Override
	public int getStatus() {
		return 0;
	}

    public void schedule(long interval, ReadyRunnable runnable,
            boolean isReschedule) {
        schedule(interval, runnable);
    }

    public int getScheduled() {
        return m_scheduleEntries.size();
    }

    @Override
    public void reset() {
        m_scheduleEntries.clear();
    }

    public long getRevision() {
        return 0L;
    }

    @Override
    public long getGlobalTasksExecuted() {
        return getLocalTasksExecuted();
    }

    @Override
    public long getLocalTasksExecuted() {
        return 0;
    }
}
