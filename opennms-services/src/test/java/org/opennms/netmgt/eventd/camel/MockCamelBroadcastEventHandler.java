package org.opennms.netmgt.eventd.camel;

import org.opennms.netmgt.eventd.adaptors.EventHandler;
import org.opennms.netmgt.xml.event.Event;
import org.opennms.netmgt.xml.event.EventReceipt;
import org.opennms.netmgt.xml.event.Header;
import org.springframework.beans.factory.annotation.Autowired;

public class MockCamelBroadcastEventHandler implements EventHandler {
    @Autowired
    protected CamelEventBroadcaster m_camelEventBroadcaster;

    private int m_numEventsProcessed = 0;

    @Override
    public boolean processEvent(Event event) {
        m_camelEventBroadcaster.process(new Header(), event);
        m_numEventsProcessed++;
        return true;
    }

    @Override
    public void receiptSent(EventReceipt receipt) {
        // This method is intentionally left blank
    }

    public void resetNumEventsProcessed() {
        m_numEventsProcessed = 0;
    }

    public int getNumEventsProcessed() {
        return m_numEventsProcessed;
        
    }
}
