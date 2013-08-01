package org.opennms.core.grid.impl;

import org.opennms.core.grid.DataGridProvider;
import org.opennms.core.grid.zookeeper.ZooKeeperGridProvider;
import org.opennms.core.test.grid.SuiteWithType.GridType;

public class ZooKeeperSuiteTest extends GridSuiteTest {
    @GridType
    public static Class<? extends DataGridProvider> getGridProviderType() {
        return ZooKeeperGridProvider.class;
    }
}
