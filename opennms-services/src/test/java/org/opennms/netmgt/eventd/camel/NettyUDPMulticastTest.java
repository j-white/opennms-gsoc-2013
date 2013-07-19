package org.opennms.netmgt.eventd.camel;

import org.apache.camel.EndpointInject;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.converter.jaxb.JaxbDataFormat;
import org.junit.Test;
import org.opennms.netmgt.EventConstants;
import org.opennms.netmgt.model.events.EventBuilder;
import org.apache.camel.spi.DataFormat;
import org.apache.camel.test.junit4.CamelTestSupport;

public class NettyUDPMulticastTest extends CamelTestSupport {
    private static final String multicastGroup = "230.0.0.1";

    @EndpointInject(uri = "mock:result")
    protected MockEndpoint endpoit;
 
    @Produce(uri = "direct:start")
    protected ProducerTemplate template;

    @Test
    public void testEventMulticast() throws Exception {
        endpoit.expectedMessageCount(2);

        EventBuilder eventBuilder = new EventBuilder(EventConstants.RTC_SUBSCRIBE_EVENT_UEI, "");
        template.sendBody(eventBuilder.getEvent());
        
        eventBuilder = new EventBuilder(EventConstants.EVENTSCONFIG_CHANGED_EVENT_UEI, "");
        template.sendBody(eventBuilder.getEvent());
        
        Thread.sleep(1000);

        endpoit.assertIsSatisfied();
    }

    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            public void configure() {
                DataFormat jaxb = new JaxbDataFormat("org.opennms.netmgt.xml.event");

                // send packets to the multicast group using the loopback interface
                from("direct:start")
                    .marshal(jaxb)
                    .to("netty:udp://" + multicastGroup + ":5818?sync=false&localAddress=localhost");

                // join the multicast group using the loopback interface
                from("netty:udp://" + multicastGroup + ":5818?sync=false&multicastGroup=" + multicastGroup + "&localAddress=localhost")
                    .unmarshal(jaxb)
                    .to("mock:result");
            }
        };
    }
}
