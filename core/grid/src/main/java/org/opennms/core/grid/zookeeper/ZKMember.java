package org.opennms.core.grid.zookeeper;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.UUID;

import org.opennms.core.grid.Member;

public class ZKMember implements Member, Serializable {
    private static final long serialVersionUID = 313557070922688014L;
    private final String m_uuid = UUID.randomUUID().toString();
    private final InetSocketAddress m_address;

    public ZKMember() {
        m_address = getAddress();
    }

    /**
     * Determines the local address used to connect to the first ZK server
     * @return
     */
    private InetSocketAddress getAddress() {
        /*
         * Socket s = new Socket("zookeeper", zkport);
           System.out.println(s.getLocalAddress());
           s.close();
         */
        return new InetSocketAddress(0);
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
