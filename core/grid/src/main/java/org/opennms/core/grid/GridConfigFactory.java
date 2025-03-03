package org.opennms.core.grid;

import org.apache.curator.RetryPolicy;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class GridConfigFactory implements GridConfigDao {
    private static GridConfigFactory m_singleton = null;
    private String m_serverConnectionString;

    private GridConfigFactory() {
        m_serverConnectionString = System.getProperty("zookeeper.server-connection-string");
    }

    public static synchronized void setInstance(GridConfigFactory cf) {
        m_singleton = cf;
    }

    public static synchronized GridConfigFactory getInstance() {
        if (m_singleton == null) {
            m_singleton = new GridConfigFactory();
        }
        return m_singleton;
    }

    public void setServerConnectionString(String serverConnectionString) {
        m_serverConnectionString = serverConnectionString;
    }

    @Override
    public String getServerConnectionString() {
        return m_serverConnectionString;
    }

    @Override
    public RetryPolicy getRetryPolicy() {
        return new ExponentialBackoffRetry(1000, 10);
    }
}
