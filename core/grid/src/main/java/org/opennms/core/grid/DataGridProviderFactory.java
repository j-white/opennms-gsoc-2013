package org.opennms.core.grid;

import org.opennms.core.grid.hazelcast.HazelcastDataGridProvider;

public class DataGridProviderFactory {
    private static DataGridProvider m_instance = null;

    private DataGridProviderFactory() {

    }

    public static synchronized DataGridProvider getInstance() {
        if (m_instance == null) {
            m_instance = new HazelcastDataGridProvider();
        }
        return m_instance;
    }

    public static DataGridProvider getNewInstance() {
        return new HazelcastDataGridProvider();
    }

    public static synchronized void setInstance(DataGridProvider dataGridProvider) {
        m_instance = dataGridProvider;
    }
}
