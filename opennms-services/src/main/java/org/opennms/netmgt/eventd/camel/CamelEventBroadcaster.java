package org.opennms.netmgt.eventd.camel;

import org.apache.camel.CamelContext;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.opennms.netmgt.model.events.EventProcessor;
import org.opennms.netmgt.xml.event.Event;
import org.opennms.netmgt.xml.event.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

public class CamelEventBroadcaster implements EventProcessor {
    /**
     * Camel context.
     */
    @Autowired
    private CamelContext m_camelContext;

    /**
     * Camel producer used to inject events.
     */
    @Produce(uri = PRODUCER_URI)
    private ProducerTemplate m_producer;

    /**
     * Producer.
     */
    private static final String PRODUCER_URI = "seda:event";

    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(CamelEventBroadcaster.class);

    /**
     * Broadcasts the event.
     */
    @Override
    public void process(Header eventHeader, Event event) {
        if (event.getLogmsg() != null
                && event.getLogmsg().getDest().equals("suppress")) {
            LOG.error("process: skip sending event {} broadcast because it is marked as suppress",
                      event.getUei());
        } else if (event.getLocal()) {
            LOG.error("process: skip sending event {} broadcast because it is marked as local",
                      event.getUei());
        } else  {
            LOG.error("process: broadcasting event {}", event);
            m_producer.sendBody(PRODUCER_URI, event);
        }
    }
}
