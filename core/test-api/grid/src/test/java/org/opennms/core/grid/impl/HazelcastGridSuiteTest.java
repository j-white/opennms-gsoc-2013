package org.opennms.core.grid.impl;

import org.opennms.core.grid.DataGridProvider;
import org.opennms.core.grid.hazelcast.HazelcastGridProvider;
import org.opennms.core.test.grid.SuiteWithType.GridType;

public class HazelcastGridSuiteTest extends GridSuiteTest {
    @GridType
    public static Class<? extends DataGridProvider> getGridProviderType() {
        return HazelcastGridProvider.class;
    }
}
