package org.opennms.netmgt.eventd.camel;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opennms.core.test.MockLogAppender;

public class CamelEventBroadcasterIntegrationTest {

    @Before
    public void setUp() {
        MockLogAppender.setupLogging(true);
    }

    @After
    public void tearDown() {
        MockLogAppender.assertNoWarningsOrGreater();
    }
    
    @Test
    public void doTest() throws Exception {
        CamelEventBroadcaster eventBroadcaster = new CamelEventBroadcaster();
        eventBroadcaster.afterPropertiesSet();
        eventBroadcaster.destroy();
    }
}
