package org.opennms.core.grid.zookeeper;

import java.io.IOException;
import java.io.Serializable;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.UUID;

import org.opennms.core.grid.Member;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKMember implements Member, Serializable {
    private static final long serialVersionUID = 313557070922688014L;
    private final String m_uuid = UUID.randomUUID().toString();
    private final InetSocketAddress m_address;

    private static final Logger LOG = LoggerFactory.getLogger(ZKMember.class);

    public ZKMember() throws UnknownHostException, IOException {
        m_address = getPublicAddress();
        LOG.info("Using public address: {}", m_address);
    }

    private InetSocketAddress getPublicAddress() throws SocketException {
        final Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
        while (networkInterfaces.hasMoreElements()) {
            final NetworkInterface ni = networkInterfaces.nextElement();
            final Enumeration<InetAddress> e = ni.getInetAddresses();
            while (e.hasMoreElements()) {
                final InetAddress inetAddress = e.nextElement();
                if (inetAddress instanceof Inet6Address) {
                    continue;
                }
                if (inetAddress.isLoopbackAddress()) {
                    continue;
                }
                return new InetSocketAddress(inetAddress, 0);
            }
        }
        return null;
    }

    @Override
    public String getUuid() {
        return m_uuid;
    }

    @Override
    public InetSocketAddress getInetSocketAddress() {
        return m_address;
    }
}
