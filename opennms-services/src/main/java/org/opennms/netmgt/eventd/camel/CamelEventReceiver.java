package org.opennms.netmgt.eventd.camel;

import java.util.ArrayList;
import java.util.List;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.opennms.netmgt.eventd.adaptors.EventHandler;
import org.opennms.netmgt.eventd.adaptors.EventReceiver;
import org.opennms.netmgt.xml.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CamelEventReceiver implements EventReceiver {
    private List<EventHandler> m_eventHandlers = new ArrayList<EventHandler>(3);
    private static final Logger LOG = LoggerFactory.getLogger(CamelEventReceiver.class);

    /**
     * Camel context.
     */
    private CamelContext m_context;

    /**
     * Input.
     */
    private static final String ROUTE_INPUT_URI = "netty:udp://224.2.2.3:5818/";

    /**
     * Output.
     */
    private static final String ROUTE_ENDPOINT_URI = "bean:camelEventReceiver?method=onEventReceived";

    /**
     * The fiber's status.
     */
    private volatile int m_status;

    @Override
    public void init() {
        m_status = START_PENDING;

        try {
            m_context = new DefaultCamelContext();
            m_context.addRoutes(new RouteBuilder() {
                @Override
                public void configure() {
                    from(ROUTE_INPUT_URI).to(ROUTE_ENDPOINT_URI);
                }
            });
        } catch (Exception e) {
            LOG.error("Initialization the context failed", e);
            m_context = null;
        }
    }

    @Override
    public synchronized void start() {
        if (m_status != START_PENDING) {
            return;
        }

        m_status = STARTING;

        try {
            m_context.start();
        } catch (Exception e) {
            LOG.error("Error while starting the context", e);
            m_status = STOPPED;
        }

        m_status = RUNNING;
    }

    @Override
    public synchronized void stop() {
        if (m_status != RUNNING) {
            return;
        }

        m_status = STOP_PENDING;

        try {
            m_context.stop();
        } catch (Exception e) {
            LOG.error("Error occured while stopping the context", e);
        }

        m_status = STOPPED;
    }

    @Override
    public synchronized int getStatus() {
        return m_status;
    }
    
    @Override
    public String getName() {
        return "Event Camel Receiver[" + ROUTE_INPUT_URI + "]";
    }

    @Override
    public void destroy() {
        // This method is intentionally left blank
    }

    public void onEventReceived(Event event) {
        synchronized (m_eventHandlers) {
            // Pass the event to all of the event handlers
            for (final EventHandler eventHandler : m_eventHandlers) {
                try {
                    LOG.debug("Handling event: {}", event);
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
                LOG.debug("Adding event handler {}", handler);
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
}
