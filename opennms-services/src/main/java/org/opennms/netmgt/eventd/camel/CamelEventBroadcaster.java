package org.opennms.netmgt.eventd.camel;

import org.apache.camel.CamelContext;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.opennms.core.grid.DataGridProvider;
import org.opennms.core.grid.Member;
import org.opennms.core.grid.MembershipEvent;
import org.opennms.core.grid.MembershipListener;
import org.opennms.netmgt.model.events.EventProcessor;
import org.opennms.netmgt.xml.event.Event;
import org.opennms.netmgt.xml.event.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;

public class CamelEventBroadcaster implements EventProcessor, InitializingBean, DisposableBean, MembershipListener {
    /**
     * Data grid.
     */
    @Autowired
    private DataGridProvider m_dataGridProvider = null;

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
     * Membership listener registration ID.
     */
    private String m_registrationId;

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
            LOG.error("Skip sending event {} broadcast because it is marked as suppress",
                      event.getUei());
        } else if (event.getLocal()) {
            LOG.error("Skip sending event {} broadcast because it is marked as local",
                      event.getUei());
        } else  {
            LOG.error("Broadcasting event with uei {} and dbid {}", event.getUei(), event.getDbid());
            m_producer.sendBody(event);
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Member localMember = m_dataGridProvider.getLocalMember();
        for (Member member : m_dataGridProvider.getClusterMembers()) {
            if (localMember.getUuid() != member.getUuid()) {
                addRouteForMember(member);
            }
        }

        m_registrationId = m_dataGridProvider.addMembershipListener(this);
    }

    @Override
    public void destroy() throws Exception {
        m_dataGridProvider.removeMembershipListener(m_registrationId);
    }

    public void addRouteForMember(Member member) {
        ClusterRouteBuilder routeBuilder = new ClusterRouteBuilder(member.getUuid(), member);
        try {
            m_camelContext.addRoutes(routeBuilder);
            m_camelContext.startRoute(routeBuilder.getRouteId());
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    @Override
    public void memberAdded(MembershipEvent membershipEvent) {
        Member member = membershipEvent.getMember();
        addRouteForMember(member);
    }

    @Override
    public void memberRemoved(MembershipEvent membershipEvent) {
        Member member = membershipEvent.getMember();
        try {
            m_camelContext.removeRoute(member.getUuid());
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }
}
