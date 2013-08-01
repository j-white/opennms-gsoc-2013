package org.opennms.core.grid.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class ZKConfigFactory implements ZKConfigDao {
    private static ZKConfigFactory m_singleton = null;
    private final String m_serverConnectionString;

    private ZKConfigFactory() {
        m_serverConnectionString = "";
    }

    public ZKConfigFactory(String serverConnectionString) {
        m_serverConnectionString = serverConnectionString;
    }

    public static synchronized void setInstance(ZKConfigFactory cf) {
        m_singleton = cf;
    }

    public static synchronized ZKConfigDao getInstance() {
        if (m_singleton == null) {
            m_singleton = new ZKConfigFactory();
        }
        return m_singleton;
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
