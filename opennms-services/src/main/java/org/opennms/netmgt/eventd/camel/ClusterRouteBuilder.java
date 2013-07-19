package org.opennms.netmgt.eventd.camel;

import org.apache.camel.builder.RouteBuilder;
import org.opennms.core.grid.Member;

public class ClusterRouteBuilder extends RouteBuilder {
    private String m_routeId;
    private Member m_member;

    public ClusterRouteBuilder(String routeId, Member member) {
        m_routeId = routeId;
        m_member = member;
    }

    public String getRouteId() {
        return m_routeId;
    }

    @Override
    public void configure() throws Exception {
        String ipAddr = m_member.getInetSocketAddress().getAddress().getHostAddress();
        from("{{event.topic.uri}}")
            .to("netty:tcp://" + ipAddr + ":{{tcpServerPort}}?sync=false");
    }
}
