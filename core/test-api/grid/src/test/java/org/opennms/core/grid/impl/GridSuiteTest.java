package org.opennms.core.grid.impl;

import org.junit.runner.RunWith;
import org.junit.runners.Suite.SuiteClasses;
import org.opennms.core.grid.test.AutowireTest;
import org.opennms.core.grid.test.DistributedExecutorTest;
import org.opennms.core.grid.test.LeaderSelectorTest;
import org.opennms.core.grid.test.MemberTest;
import org.opennms.core.grid.test.MultiClientQueueTest;
import org.opennms.core.grid.test.primitives.AtomicLongTest;
import org.opennms.core.grid.test.primitives.LockTest;
import org.opennms.core.grid.test.primitives.MapTest;
import org.opennms.core.grid.test.primitives.QueueTest;
import org.opennms.core.grid.test.primitives.SetTest;
import org.opennms.core.grid.test.primitives.ThreadPoolExecutorTest;
import org.opennms.core.test.grid.SuiteWithType;

@RunWith(SuiteWithType.class)
@SuiteClasses({
    //
    // Primitives
    //
    AtomicLongTest.class,
    LockTest.class,
    MapTest.class,
    QueueTest.class,
    SetTest.class,
    ThreadPoolExecutorTest.class,

    //
    // Integration tests
    //
    DistributedExecutorTest.class,
    LeaderSelectorTest.class,
    MemberTest.class,
    MultiClientQueueTest.class,
    AutowireTest.class
})
public abstract class GridSuiteTest {
    // this class is intentionally left blank
}
