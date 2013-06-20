package org.opennms.core.grid;

public class DataGridFactory {
    private static DataGridProvider m_instance = null;

    private DataGridFactory() {

    }

    public static synchronized DataGridProvider getInstance() {
        if (m_instance == null) {
            m_instance = new HazelcastDataGridProvider();
        }
        return m_instance;
    }
}
