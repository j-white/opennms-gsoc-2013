package org.opennms.netmgt.scheduler;

import org.opennms.core.grid.DataGridProvider;

public interface DataGridProviderAware {
    public void setDataGridProvider(DataGridProvider dataGridProvider);
}
