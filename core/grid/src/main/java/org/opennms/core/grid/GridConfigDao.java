package org.opennms.core.grid;

import org.apache.curator.RetryPolicy;

public interface GridConfigDao {

    public String getServerConnectionString();

    public RetryPolicy getRetryPolicy();
}
