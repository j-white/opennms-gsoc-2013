package org.opennms.core.grid;

import java.util.concurrent.locks.Lock;

public interface DataGridProvider {
    public Lock getLock(Object key);
}
