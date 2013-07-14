package org.opennms.netmgt.eventd.camel;

import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.opennms.netmgt.model.events.EventProcessor;
import org.opennms.netmgt.xml.event.Event;
import org.opennms.netmgt.xml.event.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

public class CamelEventBroadcaster implements EventProcessor,
        InitializingBean, DisposableBean {
    /**
     * Camel context.
     */
    private CamelContext m_context;

    /**
     * Camel producer used to inject events.
     */
    private ProducerTemplate m_producer;

    /**
     * Input.
     */
    private static final String ROUTE_INPUT_URI = "direct:event";

    /**
     * Output.
     */
    private static final String ROUTE_ENDPOINT_URI = "netty:udp://224.2.2.3:5818/?broadcast=true";

    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(CamelEventBroadcaster.class);

    @Override
    public void process(Header eventHeader, Event event) {
        if (event.getLogmsg() != null
                && event.getLogmsg().getDest().equals("suppress")) {
            LOG.debug("process: skip sending event {} to other daemons because is marked as suppress",
                      event.getUei());
        } else {
            LOG.debug("process: sending event {}", event);
            m_producer.sendBody("direct:event", event);
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        m_context = new DefaultCamelContext();
        m_context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from(ROUTE_INPUT_URI).to(ROUTE_ENDPOINT_URI);
            }
        });
        m_context.start();
        m_producer = m_context.createProducerTemplate();     
    }

    @Override
    public void destroy() throws Exception {
        if (m_context != null) {
            m_context.stop();
        }
    }
}
