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

import java.io.Serializable;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Represents a Schedule
 *
 * @author brozow
 * @version $Id: $
 */
public class Schedule implements Serializable {

    private static final long serialVersionUID = 625752714440790084L;

    private static final Logger LOG = LoggerFactory.getLogger(Schedule.class);

    /** Constant <code>random</code> */
    private static final Random random = new Random();
    private final ReadyRunnable m_schedulable;
    private final ScheduleInterval m_interval;
    private transient ScheduleTimer m_timer;
    private volatile int m_currentExpirationCode;
    private volatile boolean m_scheduled = false;

    private class ScheduleEntry implements ClusterRunnable, Reschedulable, SchedulerAware {
        private static final long serialVersionUID = -5278243900171236605L;
        private final int m_expirationCode;
        private long m_intervalForReschedule;

        public ScheduleEntry(int expirationCode) {
            m_expirationCode = expirationCode;
        }
        
        /**
         * @return
         */
        private boolean isExpired() {
            return m_expirationCode < m_currentExpirationCode;
        }
        
        @Override
        public boolean isReady() {
            return isExpired() || m_schedulable.isReady();
        }

        @Override
        public void run() {
            if (isExpired()) {
                LOG.debug("Schedule {} expired.  No need to run.", this);
                return;
            }

            // Default to the parents interval
            m_intervalForReschedule = m_interval.getInterval();

            if (!m_interval.scheduledSuspension()) {
                try {
                    Schedule.this.run();
                } catch (PostponeNecessary e) {
                    // Chose a random number of seconds between 5 and 14 to wait before trying again
                    m_intervalForReschedule = random.nextInt(10)*1000+5000;
                }
            }
        }

        @Override
        public String toString() { return "ScheduleEntry[expCode="+m_expirationCode+"] for "+m_schedulable; }

        /**
         * Use the Reschedulable interface to perform rescheduling.
         */
        @Override
        public boolean rescheduleAfterRun() {
            // Determine whether or not we should reschedule
            if (isExpired()) {
                LOG.debug("Schedule {} expired. Will not reschedule.", this);
                return false;
            } else if (!m_scheduled) {
                LOG.debug("Schedule {} no longer scheduled. Will not reschedule.", this);
                return false;
            } else if (m_intervalForReschedule < 0) {
                LOG.debug("Schedule {} has negative interval {}. Will not reschedule.", this, m_intervalForReschedule);
                return false;
            }

            // All checks passed, reschedule
            LOG.debug("Schedule {} has been rescheduled in {}.", this, m_intervalForReschedule);
            return true;
        }

        /**
         * Return the interval determined in the run method.
         */
        @Override
        public long getInterval() {
            return m_intervalForReschedule;
        }

        /**
         * Updates the instance of the scheduler.
         */
        @Override
        public void setScheduler(Scheduler scheduler) {
            m_timer = scheduler;
        }
    }

    /**
     * <p>Constructor for Schedule.</p>
     *
     * @param interval a {@link org.opennms.netmgt.scheduler.ScheduleInterval} object.
     * @param timer a {@link org.opennms.netmgt.scheduler.ScheduleTimer} object.
     * @param schedulable a {@link org.opennms.netmgt.scheduler.ReadyRunnable} object.
     */
    public Schedule(ReadyRunnable schedulable, ScheduleInterval interval, ScheduleTimer timer) {
        m_schedulable = schedulable;
        m_interval = interval;
        m_timer = timer;
        m_currentExpirationCode = 0;
    }

    /**
     * <p>schedule</p>
     */
    public void schedule() {
        m_scheduled = true;
        schedule(0);
    }

    private void schedule(long interval) {
        if (interval >= 0 && m_scheduled)
            m_timer.schedule(interval, new ScheduleEntry(++m_currentExpirationCode));
    }

    /**
     * <p>run</p>
     */
    public void run() {
        m_schedulable.run();
    }

    /**
     * <p>adjustSchedule</p>
     */
    public void adjustSchedule() {
        schedule(m_interval.getInterval());
    }

    /**
     * <p>unschedule</p>
     */
    public void unschedule() {
        m_scheduled = false;
        m_currentExpirationCode++;
    }

}
