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

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.opennms.core.grid.DataGridProvider;
import org.opennms.core.grid.Member;
import org.opennms.core.grid.MembershipEvent;
import org.opennms.core.grid.MembershipListener;
import org.opennms.netmgt.eventd.camel.grid.GridRouteBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

/**
 * Dynamically builds route for members and they join and leave the grid.
 * 
 * @author jwhite
 */
public class GridRouteManager implements InitializingBean, DisposableBean,
        MembershipListener {

    /**
     * Data grid.
     */
    @Autowired
    private DataGridProvider m_dataGridProvider;

    /**
     * Camel context.
     */
    @Autowired
    private CamelContext m_camelContext;

    /**
     * Membership listener registration ID.
     */
    private String m_registrationId;

    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(GridRouteManager.class);

    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(m_dataGridProvider);

        Set<Member> gridMembers = m_dataGridProvider.getGridMembers();
        LOG.info("There are currently {} members in the grid.",
                 gridMembers.size());

        Member localMember = m_dataGridProvider.getLocalMember();
        for (Member member : gridMembers) {
            if (localMember.getUuid() == member.getUuid()) {
                LOG.info("Skipping route for {} @ {}", member.getUuid(),
                         member.getInetSocketAddress());
                continue;
            }

            addRouteForMember(member);
        }

        //FIXME: Possible race condition here. The membership listener 
        // should be added before enumerating the members manually.
        LOG.info("Adding membership listener.");
        m_registrationId = m_dataGridProvider.addMembershipListener(this);
    }

    @Override
    public void destroy() throws Exception {
        LOG.info("Removing membership listener.");
        m_dataGridProvider.removeMembershipListener(m_registrationId);
    }

    public void addRouteForMember(Member member) {
        LOG.info("Adding route for {}", member);

        GridRouteBuilder routeBuilder = new GridRouteBuilder(member);
        try {
            m_camelContext.addRoutes(routeBuilder);
            m_camelContext.startRoute(routeBuilder.getRouteId());
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    @Override
    public void memberAdded(MembershipEvent membershipEvent) {
        //TODO: Set log prefix - this will be invoked from the grid provider's thread
        LOG.info("A member was added to the cluster.");
        addRouteForMember(membershipEvent.getMember());
    }

    @Override
    public void memberRemoved(MembershipEvent membershipEvent) {
        //TODO: Set log prefix - this will be invoked from the grid provider's thread
        LOG.info("A member was removed from the cluster");

        Member member = membershipEvent.getMember();
        GridRouteBuilder routeBuilder = new GridRouteBuilder(member);
        try {
            LOG.info("Stopping route for {}.", member);
            m_camelContext.stopRoute(routeBuilder.getRouteId(), 5,
                                     TimeUnit.SECONDS);
            m_camelContext.removeRoute(routeBuilder.getRouteId());
        } catch (Exception e) {
            LOG.error("Failed to stop the route for {}: {}", member,
                      e.getMessage());
        }
    }
}
