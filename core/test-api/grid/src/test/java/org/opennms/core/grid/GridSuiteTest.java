package org.opennms.core.grid;

import org.junit.runner.RunWith;
import org.junit.runners.Suite.SuiteClasses;
import org.opennms.core.test.grid.SuiteWithType;

@RunWith(SuiteWithType.class)
@SuiteClasses({
    DistributedExecutorTest.class,
    LeaderSelectorTest.class,
    MemberTest.class,
    QueueTest.class
})
public abstract class GridSuiteTest {
    
}
