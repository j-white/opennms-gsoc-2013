package org.opennms.features.clustering.utils;

import org.opennms.core.grid.DataGridProvider;
import org.opennms.netmgt.scheduler.Timer;

public class ClusterRunnableTimeKeeper implements ClusterRunnable, Timer {
    private static final long serialVersionUID = -6443009521003467167L;

    private long m_timeToRun;
    private ClusterRunnable m_runnable;

    public ClusterRunnableTimeKeeper(ClusterRunnable runnable, long timeToRun) {
        m_runnable = runnable;
        m_timeToRun = timeToRun;
    }

    @Override
    public boolean isReady() {
        return getCurrentTime() >= m_timeToRun && m_runnable.isReady();
    }

    @Override
    public void run() {
        m_runnable.run();
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
    public void setDataGridProvider(DataGridProvider dataGridProvider) {
        m_runnable.setDataGridProvider(dataGridProvider);
    }

    @Override
    public void setDistributedScheduler(
            DistributedScheduler distributedScheduler) {
        m_runnable.setDistributedScheduler(distributedScheduler);
    }
}
