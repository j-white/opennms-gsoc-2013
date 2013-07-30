package org.opennms.core.grid;

public class DataGridProviderFactory {
    private static DataGridProvider m_instance = null;
    private static Class<? extends DataGridProvider> m_clazz = null;

    private DataGridProviderFactory() {
        // this method is intentionally left blank
    }

    public static synchronized DataGridProvider getInstance() {
        if (m_instance == null) {
            m_instance = getNewInstance();
        }
        return m_instance;
    }

    @SuppressWarnings("unchecked")
    public static synchronized void setType(String type) {
        try {
            setType((Class<? extends DataGridProvider>) Class.forName(type));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static synchronized void setType(Class<? extends DataGridProvider> clazz) {
        m_clazz = clazz;
    }

    public static DataGridProvider getNewInstance() {
        try {
            return m_clazz.newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static synchronized void setInstance(
            DataGridProvider dataGridProvider) {
        m_instance = dataGridProvider;
    }

    public static synchronized void shutdownAll() {
        if (m_clazz == null) {
            return;
        } else if (m_instance == null) {
            getNewInstance().shutdownAll();
        } else {
            m_instance.shutdownAll();
            m_instance = null;
        }
    }
}
