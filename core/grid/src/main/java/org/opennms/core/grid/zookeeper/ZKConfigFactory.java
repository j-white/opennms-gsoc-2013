package org.opennms.core.grid.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class ZKConfigFactory implements ZKConfigDao {

    public static ZKConfigDao getInstance() {
        return null;
    }

    @Override
    public String getServerConnectionString() {
        return "";
    }

    @Override
    public RetryPolicy getRetryPolicy() {
        return new ExponentialBackoffRetry(1000, 10);
    }
}
