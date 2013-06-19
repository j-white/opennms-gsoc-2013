package org.opennms.netmgt.vmmgr;

import java.util.LinkedList;
import java.util.List;

import org.opennms.netmgt.config.service.Service;

/**
 * Used to keep track of which services were started inside and outside the manager.
 * The manager then uses this list of services when responding to "status"
 * queries and when stopping all services on shutdown.
 *
 * @author jwhite
 */
public class ServiceRegistry {
    private static ServiceRegistry m_instance = new ServiceRegistry();
    private LinkedList<Service> m_services = new LinkedList<Service>();
    private boolean m_bootstrapComplete = false;

    private ServiceRegistry() {

    }

    public static ServiceRegistry getInstance() {
        return m_instance;
    }

    public void addService(Service service) {
        if (!m_services.contains(service)) {
            m_services.add(service);
        }
    }

    public void removeService(Service service) {
        m_services.remove(service);
    }

    public List<Service> getServices() {
        return m_services;
    }

    public void setBootstrapComplete(boolean val) {
        m_bootstrapComplete = val;
    }

    public boolean getBootstrapComplete() {
        return m_bootstrapComplete;
    }
}
