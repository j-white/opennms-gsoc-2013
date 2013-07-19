package org.opennms.netmgt.eventd.camel;

import static com.jayway.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;

import java.util.concurrent.Callable;

import org.apache.camel.CamelContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opennms.core.grid.DataGridProvider;
import org.opennms.core.grid.DataGridProviderFactory;
import org.opennms.core.test.OpenNMSJUnit4ClassRunner;
import org.opennms.core.test.grid.annotations.JUnitGrid;
import org.opennms.core.utils.BeanUtils;
import org.opennms.netmgt.EventConstants;
import org.opennms.netmgt.model.events.EventBuilder;
import org.opennms.netmgt.xml.event.Event;
import org.opennms.netmgt.xml.event.Header;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;

@RunWith(OpenNMSJUnit4ClassRunner.class)
@ContextConfiguration(locations = {
        "classpath:/META-INF/opennms/applicationContext-soa.xml",
        "classpath*:/META-INF/opennms/component-dao.xml",
        "classpath:META-INF/opennms/applicationContext-camelEventd.xml",
        "classpath:META-INF/opennms/applicationContext-camelEventdTest.xml" })
@JUnitGrid(reuseGrid=true)
public class CamelEventdIntegrationTest {
    @Autowired
    protected CamelContext camelContext;

    @Autowired
    protected CamelEventReceiver camelEventReceiver;

    @Autowired
    protected CamelEventBroadcaster camelEventBroadcaster;

    @Autowired
    protected MockCamelBroadcastEventHandler mockBroadcastHandler;

    @Autowired
    protected DataGridProvider dataGridProvider;

    @Before
    public void setUp() {
        BeanUtils.assertAutowiring(this);
        camelEventReceiver.start();
        mockBroadcastHandler.reset();
    }

    @Test
    public void testEventBroadcasterAndReceiver() throws Exception {
        assertEquals(Integer.valueOf(1), getNumClusterMembers(dataGridProvider).call());

        // Add a second member to the cluster, the member listener should trigger
        // the addition of a new route
        DataGridProvider anotherDataGridProvider = DataGridProviderFactory.getNewInstance();
        anotherDataGridProvider.init();

        await().until(getNumClusterMembers(dataGridProvider), is(2));

        // Create a mock event
        EventBuilder eventBuilder = new EventBuilder(
                                                     EventConstants.EVENTSCONFIG_CHANGED_EVENT_UEI,
                                                     "maple bacon doughnut");
        Event expectedEvent = eventBuilder.getEvent();

        assertEquals(0, mockBroadcastHandler.getNumEventsProcessed());
        assertEquals(null, mockBroadcastHandler.getLastEvent());

        // Broadcast the event
        camelEventBroadcaster.process(new Header(), expectedEvent);
        
        // Verify that the event handler is invoked
        await().until(getNumEventsProcessed(), is(1));
        
        // Compare the events
        assertEquals(expectedEvent.getUei(), mockBroadcastHandler.getLastEvent().getUei());
    }

    private Callable<Integer> getNumClusterMembers(
           final DataGridProvider dataGridProvider) {
       return new Callable<Integer>() {
           public Integer call() throws Exception {
               return dataGridProvider.getClusterMembers().size();
           }
       };
   }

    private Callable<Integer> getNumEventsProcessed() {
        return new Callable<Integer>() {
            public Integer call() {
                return mockBroadcastHandler.getNumEventsProcessed();
            }
        };
    }
}
