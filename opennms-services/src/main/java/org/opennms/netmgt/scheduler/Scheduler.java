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

/**
 * <p>
 * Scheduler interface.
 * </p>
 * 
 * @author ranger
 * @version $Id: $
 */
public interface Scheduler extends ScheduleTimer {

    /*
     * Scheduling
     */

    /**
     * {@inheritDoc}
     * 
     * This method is used to schedule a ready runnable in the system. The
     * interval is used as the key for determining which queue to add the
     * runnable.
     */
    @Override
    public abstract void schedule(long interval, final ReadyRunnable runnable);

    /**
     * This returns the current time for the scheduler
     * 
     * @return a long.
     */
    @Override
    public abstract long getCurrentTime();

    /*
     * Life-cycle
     */

    /**
     * Starts the fiber.
     * 
     * @throws java.lang.IllegalStateException
     *             Thrown if the fiber is already running.
     */
    public abstract void start();

    /**
     * Stops the fiber. If the fiber has never been run then an exception is
     * generated.
     * 
     * @throws java.lang.IllegalStateException
     *             Throws if the fiber has never been started.
     */
    public abstract void stop();

    /**
     * Pauses the scheduler if it is current running. If the fiber has not
     * been run or has already stopped then an exception is generated.
     * 
     * @throws java.lang.IllegalStateException
     *             Throws if the operation could not be completed due to the
     *             fiber's state.
     */
    public abstract void pause();

    /**
     * Resumes the scheduler if it has been paused. If the fiber has not been
     * run or has already stopped then an exception is generated.
     * 
     * @throws java.lang.IllegalStateException
     *             Throws if the operation could not be completed due to the
     *             fiber's state.
     */
    public abstract void resume();

    /**
     * Returns the current of this fiber.
     * 
     * @return The current status.
     */
    public abstract int getStatus();

    /**
     * Remove any queued tasks but allow any tasks that are currently
     * executing to finish.
     * 
     * Tasks that are currently executing who use the Reschedulable interface
     * will not be rescheduled (even if a reschedule is requested).
     */
    public void reset();

    /**
     * Retrieves the current revision of the scheduler.
     * 
     * @return the current revision
     */
    public long getRevision();

    /*
     * Statistics
     */

    /**
     * Returns total number of elements currently scheduled.
     * 
     * @return the sum of all the elements in the various queues
     */
    public int getScheduled();

    /**
     * Returns total number of scheduled tasks that have been
     * executed across all instances of this scheduler.
     *
     * @return the number of executed tasks
     */
    public long getGlobalTasksExecuted();

    /**
     * Returns total number of scheduled tasks that have been
     * executed on this instance of the scheduler.
     *
     * @return the number of executed tasks
     */
    public long getLocalTasksExecuted();

}
