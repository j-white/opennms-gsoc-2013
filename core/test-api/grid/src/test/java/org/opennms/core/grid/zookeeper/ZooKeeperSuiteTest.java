package org.opennms.core.grid.zookeeper;

import org.junit.Ignore;
import org.opennms.core.grid.DataGridProvider;
import org.opennms.core.grid.GridSuiteTest;
import org.opennms.core.test.grid.SuiteWithType.GridType;

@Ignore
public class ZooKeeperSuiteTest extends GridSuiteTest {
    @GridType
    public static Class<? extends DataGridProvider> getGridProviderType() {
        return ZooKeeperGridProvider.class;
    }
}
