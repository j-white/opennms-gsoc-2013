package org.opennms.netmgt.eventd.camel;

import org.apache.camel.builder.RouteBuilder;
import org.opennms.core.grid.Member;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterRouteBuilder extends RouteBuilder {
    private Member m_member;

    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(ClusterRouteBuilder.class);

    public ClusterRouteBuilder(Member member) {
        m_member = member;
    }

    public String getRouteId() {
        return "HzEventRoute[" + m_member.getUuid() + "]";
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
