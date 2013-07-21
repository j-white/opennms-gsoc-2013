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

import org.apache.camel.CamelContext;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.opennms.netmgt.model.events.EventProcessor;
import org.opennms.netmgt.xml.event.Event;
import org.opennms.netmgt.xml.event.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * An event processor that forwards events to a Camel producer.
 *
 * Events marked as "local" or "suppress" are not forwarded.
 *
 * @author jwhite
 */
public class CamelEventProducer implements EventProcessor {
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
    private static final Logger LOG = LoggerFactory.getLogger(CamelEventProducer.class);

    /**
     * Forwards the event.
     */
    @Override
    public void process(Header eventHeader, Event event) {
        if (event.getLogmsg() != null
                && event.getLogmsg().getDest().equals("suppress")) {
            LOG.debug("Skip sending event {} broadcast because it is marked as suppress",
                      event.getUei());
        } else if (event.getLocal()) {
            LOG.debug("Skip sending event {} broadcast because it is marked as local",
                      event.getUei());
        } else {
            LOG.info("Broadcasting event with uei {} and dbid {}",
                     event.getUei(), event.getDbid());
            m_producer.sendBody(event);
        }
    }
}
