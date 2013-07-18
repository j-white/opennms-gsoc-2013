package org.opennms.netmgt.eventd.camel;

import java.util.ArrayList;
import java.util.List;

import org.apache.camel.CamelContext;
import org.opennms.netmgt.eventd.adaptors.EventHandler;
import org.opennms.netmgt.eventd.adaptors.EventReceiver;
import org.opennms.netmgt.xml.event.Event;
import org.opennms.netmgt.xml.event.Logmsg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

public class CamelEventReceiver implements EventReceiver {
    private List<EventHandler> m_eventHandlers = new ArrayList<EventHandler>(3);
    private static final Logger LOG = LoggerFactory.getLogger(CamelEventReceiver.class);

    /**
     * Camel context.
     */
    @Autowired
    private CamelContext m_camelContext;

    /**
     * The fiber's status.
     */
    private volatile int m_status = STOPPED;

    public void onEventReceived(Event event) {
        if (m_status != RUNNING) {
            return;
        }

        LOG.error("Received event with uei {} and dbid {}", event.getUei(), event.getDbid());

        synchronized (m_eventHandlers) {
            // Mark the event as "local" so it does not get re-broadcasted
            event.setLocal(true);

            // Set the destination to donotpersist to avoid writing the event to the database multiple times
            if (event.getLogmsg() == null) {
                event.setLogmsg(new Logmsg());
            }
            event.getLogmsg().setDest("donotpersist");

            // Pass the event to all of the event handlers
            for (final EventHandler eventHandler : m_eventHandlers) {
                try {
                    LOG.error("Processing event with uei {} and dbid {} via {}", event.getUei(), event.getDbid(), eventHandler.getClass());
                    eventHandler.processEvent(event);
                } catch (final Throwable t) {
                    LOG.warn("An exception occured while processing an event.", t);
                }
            }
        }
    }

    @Override
    public void addEventHandler(EventHandler handler) {
        synchronized (m_eventHandlers) {
            if (!m_eventHandlers.contains(handler)) {
                LOG.error("Adding event handler {}", handler);
                m_eventHandlers.add(handler);
            }
        }
    }

    @Override
    public void removeEventHandler(EventHandler handler) {
        synchronized (m_eventHandlers) {
            LOG.debug("Removing event handler {}", handler);
            m_eventHandlers.remove(handler);
        }
    }

    /**
     * <p>getEventHandlers</p>
     *
     * @return a {@link java.util.List} object.
     */
    public List<EventHandler> getEventHandlers() {
        return m_eventHandlers;
    }

    /**
     * <p>setEventHandlers</p>
     *
     * @param eventHandlers a {@link java.util.List} object.
     */
    public void setEventHandlers(List<EventHandler> eventHandlers) {
        m_eventHandlers = eventHandlers;
    }
 
    @Override
    public void init() {
        // Init is not always called before start by eventd - so we do nothing here
    }

    @Override
    public synchronized void start() {
        m_status = RUNNING;
    }

    @Override
    public synchronized void stop() {
        m_status = STOPPED;
    }

    @Override
    public synchronized int getStatus() {
        return m_status;
    }
    
    @Override
    public String getName() {
        return "Camel Event Receiver";
    }

    @Override
    public void destroy() {
        // This method is intentionally left blank
    }
}
