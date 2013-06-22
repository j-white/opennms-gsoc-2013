package org.opennms.features.clustering.utils;

import java.io.Serializable;

import org.opennms.core.grid.DataGridProvider;
import org.opennms.netmgt.scheduler.ReadyRunnable;

public interface ClusterRunnable extends ReadyRunnable, Serializable {
    /**
     * Sets the instance of the data grid provider used to
     * run the task.
     */
    public void setDataGridProvider(
            DataGridProvider dataGridProvider);

    /**
     * Sets the instance of the distributed scheduler used to
     * run the task.
     */
    public void setDistributedScheduler(
            DistributedScheduler distributedScheduler);
}
