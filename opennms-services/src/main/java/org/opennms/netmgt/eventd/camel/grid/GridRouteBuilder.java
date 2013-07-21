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

package org.opennms.netmgt.eventd.camel.grid;

import org.apache.camel.builder.RouteBuilder;
import org.opennms.core.grid.Member;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds a route from the event topic to a grid member.
 *
 * @author jwhite
 */
public class GridRouteBuilder extends RouteBuilder {
    private Member m_member;

    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(GridRouteBuilder.class);

    public GridRouteBuilder(Member member) {
        m_member = member;
    }

    public String getRouteId() {
        return "GridEventRoute[" + m_member.getUuid() + "]";
    }

    @Override
    public void configure() throws Exception {
        String ipAddr = m_member.getInetSocketAddress().getAddress().getHostAddress();
        LOG.info("Adding route for {}", ipAddr);

        from("{{event.topic.uri}}").routeId(getRouteId()).to("netty:tcp://"
                                                                     + ipAddr
                                                                     + ":{{tcpServerPort}}?sync=false");
    }
}
