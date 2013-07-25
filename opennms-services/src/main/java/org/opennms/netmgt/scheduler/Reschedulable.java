package org.opennms.netmgt.scheduler;

/**
 * Interface that can be implemented by ReadyRunnable to automatically
 * reschedule themselves after execution.
 *
 * Using this interface has the advantage that the reschedule will not
 * occur if the scheduler is shutting down or has been reset.
 *
 * @author jwhite
 */
public interface Reschedulable {

    /**
     * Do you want to run again?
     *
     * @return
     *          yes or no
     */
    public boolean rescheduleAfterRun();

    /**
     * How long should I wait before running you again?
     *
     * @return
     *          interval in ms
     */
    public long getInterval();
}
