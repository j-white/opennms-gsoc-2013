package org.opennms.netmgt.eventd.camel;

import static com.jayway.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;

import java.util.concurrent.Callable;

import org.apache.camel.CamelContext;
import org.apache.camel.EndpointInject;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.mock.MockEndpoint;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opennms.core.test.OpenNMSJUnit4ClassRunner;
import org.opennms.core.utils.BeanUtils;
import org.opennms.netmgt.EventConstants;
import org.opennms.netmgt.model.events.EventBuilder;
import org.opennms.netmgt.xml.event.Event;
import org.opennms.netmgt.xml.event.Header;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;

@RunWith(OpenNMSJUnit4ClassRunner.class)
@ContextConfiguration(locations = {
        "classpath:META-INF/opennms/applicationContext-camelEventd.xml",
        "classpath:META-INF/opennms/applicationContext-camelEventdTest.xml" })
public class CamelEventdIntegrationTest {
    @Autowired
    protected CamelContext camelContext;

    @Autowired
    protected CamelEventReceiver camelEventReceiver;

    @Autowired
    protected CamelEventBroadcaster camelEventBroadcaster;

    @Autowired
    protected MockCamelBroadcastEventHandler mockBroadcastHandler;

    @Produce(uri = "direct:receiver-input")
    protected ProducerTemplate receiverInput;

    @EndpointInject(uri = "mock:broadcaster-output")
    protected MockEndpoint broadcasterOutput;

    @Before
    public void setUp() {
        BeanUtils.assertAutowiring(this);
        mockBroadcastHandler.resetNumEventsProcessed();
    }

    @Test
    public void testEventBroadcaster() throws Exception {
        // Create a mock event
        EventBuilder eventBuilder = new EventBuilder(
                                                     EventConstants.EVENTSCONFIG_CHANGED_EVENT_UEI,
                                                     "maple bacon doughnut");
        Event expectedEvent = eventBuilder.getEvent();

        // Expect the mock event to be sent to route endpoint
        broadcasterOutput.expectedBodiesReceived(expectedEvent);

        // Manually invoke process with the event
        camelEventBroadcaster.process(new Header(), expectedEvent);

        // Verify that the event made it to the route
        broadcasterOutput.assertIsSatisfied();
    }

    @Test
    public void testEventReceiver() throws Exception {
        // Create a mock event
        EventBuilder eventBuilder = new EventBuilder(
                                                     EventConstants.EVENTSCONFIG_CHANGED_EVENT_UEI,
                                                     "maple bacon doughnut");
        Event expectedEvent = eventBuilder.getEvent();

        // There should be no events processed
        assertEquals(Integer.valueOf(0), getNumEventsProcessed().call());
        int prevNumReceived = broadcasterOutput.getReceivedCounter();

        // Send an event to the receiver
        camelEventReceiver.start();
        receiverInput.sendBody(expectedEvent);

        // Verify that the event handler is invoked
        await().until(getNumEventsProcessed(), is(1));

        // Verify that the broadcast processor ignores the event
        assertEquals(prevNumReceived, broadcasterOutput.getReceivedCounter());
    }

    private Callable<Integer> getNumEventsProcessed() {
        return new Callable<Integer>() {
            public Integer call() {
                return mockBroadcastHandler.getNumEventsProcessed();
            }
        };
    }
}
