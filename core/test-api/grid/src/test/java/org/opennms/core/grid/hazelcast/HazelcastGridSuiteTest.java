package org.opennms.core.grid.hazelcast;

import org.opennms.core.grid.DataGridProvider;
import org.opennms.core.grid.GridSuiteTest;
import org.opennms.core.test.grid.SuiteWithType.GridType;

public class HazelcastGridSuiteTest extends GridSuiteTest {
    @GridType
    public static Class<? extends DataGridProvider> getGridProviderType() {
        return HazelcastGridProvider.class;
    }
}
