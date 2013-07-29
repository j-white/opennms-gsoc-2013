package org.opennms.netmgt.poller.pollables;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.Callable;

import org.opennms.netmgt.model.PollStatus;
import org.opennms.netmgt.poller.MonitoredService;
import org.opennms.netmgt.poller.NetworkInterface;
import org.opennms.netmgt.poller.ServiceMonitor;

/**
 * Wraps everything we need to perform a polling operation in
 * a serializable class.
 *
 * @author jwhite
 */
public class PollableTask implements Callable<PollStatus>, Serializable {

    private static final long serialVersionUID = -6310155373739997954L;

    private final ServiceMonitor m_monitor;
    private final MonitoredService m_service;
    private final Map<String, Object> m_parameters;

    public PollableTask(ServiceMonitor monitor, PollableService service, Map<String, Object> parameters) {
        m_monitor = monitor;
        m_service = new SerializableMonitoredService(service);
        m_parameters = parameters;
    }

    @Override
    public PollStatus call() throws Exception {
        return  m_monitor.poll(m_service, m_parameters);
    }

    private static class SerializableMonitoredService implements MonitoredService, Serializable {
        private static final long serialVersionUID = 3237064334690225829L;

        private final String m_svcUrl;
        private final String m_svcName;
        private final String m_ipAddr;
        private final int m_nodeId;
        private final String m_nodeLabel;
        private final NetworkInterface<InetAddress> m_netInterface;
        private final InetAddress m_address;

        public SerializableMonitoredService(PollableService pollableService) {
            m_svcUrl = pollableService.getSvcUrl();
            m_svcName = pollableService.getSvcName();
            m_ipAddr = pollableService.getIpAddr();
            m_nodeId = pollableService.getNodeId();
            m_nodeLabel = pollableService.getNodeLabel();
            m_netInterface = pollableService.getNetInterface();
            m_address = pollableService.getAddress();
        }

        @Override
        public String getSvcUrl() {
            return m_svcUrl;
        }

        @Override
        public String getSvcName() {
            return m_svcName;
        }

        @Override
        public String getIpAddr() {
            return m_ipAddr;
        }

        @Override
        public int getNodeId() {
            return m_nodeId;
        }

        @Override
        public String getNodeLabel() {
            return m_nodeLabel;
        }

        @Override
        public NetworkInterface<InetAddress> getNetInterface() {
            return m_netInterface;
        }

        @Override
        public InetAddress getAddress() {
            return m_address;
        }
    }
}
