package org.opennms.core.grid.mock;

import org.opennms.core.grid.DataGridProvider;
import org.opennms.core.grid.GridSuiteTest;
import org.opennms.core.test.grid.SuiteWithType.GridType;

public class MockSuiteTest extends GridSuiteTest {
    @GridType
    public static Class<? extends DataGridProvider> getGridProviderType() {
        return MockGridProvider.class;
    }
}
