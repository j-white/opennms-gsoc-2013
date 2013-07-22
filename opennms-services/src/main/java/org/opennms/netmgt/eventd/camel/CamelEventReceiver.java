/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2006-2013 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2013 The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is a registered trademark of The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * OpenNMS(R) is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with OpenNMS(R).  If not, see:
 *      http://www.gnu.org/licenses/
 *
 * For more information contact:
 *     OpenNMS(R) Licensing <license@opennms.org>
 *     http://www.opennms.org/
 *     http://www.opennms.com/
 *******************************************************************************/

package org.opennms.netmgt.eventd.camel;

import java.util.ArrayList;
import java.util.List;

import org.apache.camel.CamelContext;
import org.opennms.netmgt.eventd.adaptors.EventHandler;
import org.opennms.netmgt.eventd.adaptors.EventReceiver;
import org.opennms.netmgt.xml.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Receives events from Camel, via a bean call, and forwards them to the
 * registered event handlers.
 *
 * @author jwhite
 */
public class CamelEventReceiver implements EventReceiver {

    /**
     * Event handlers.
     */
    private List<EventHandler> m_eventHandlers = new ArrayList<EventHandler>(3);

    /**
     * Logger.
     */
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

        LOG.info("Received event with uei {} and dbid {}", event.getUei(), event.getDbid());

        synchronized (m_eventHandlers) {
            // Pass the event to all of the event handlers
            for (final EventHandler eventHandler : m_eventHandlers) {
                try {
                    LOG.debug("Processing event with uei {} and dbid {} via {}", event.getUei(), event.getDbid(), eventHandler.getClass());
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
