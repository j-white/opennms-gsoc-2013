package org.opennms.core.grid.zookeeper;

import org.apache.curator.RetryPolicy;

public interface ZKConfigDao {

    public String getServerConnectionString();

    public RetryPolicy getRetryPolicy();
}
