package org.opennms.netmgt.vmmgr;

import java.util.LinkedList;
import java.util.List;

import org.opennms.netmgt.config.service.Service;

/**
 * Used to keep track of which services were started inside and outside the
 * manager. The manager then uses this list of services when responding to
 * "status" queries and when stopping all services on shutdown.
 * 
 * @author jwhite
 */
public class ServiceRegister {
    private static ServiceRegister m_instance = new ServiceRegister();
    private LinkedList<Service> m_services = new LinkedList<Service>();

    /**
     * Singleton
     */
    private ServiceRegister() {
        // This method is intentionally left blank
    }

    /**
     * Retrieves the singleton instance.
     * 
     * @return The singleton instance
     */
    protected static ServiceRegister getInstance() {
        return m_instance;
    }

    /**
     * Adds a new service to the register.
     * 
     * Should be called after a service was started by the ServiceManager.
     * 
     * @param service
     *            The service to add to the register
     */
    protected void addService(Service service) {
        if (!m_services.contains(service)) {
            m_services.add(service);
        }
    }

    /**
     * Removes a service from the register.
     * 
     * Should be called after a service was successfully stopped by the
     * ServiceManager.
     * 
     * @param service
     *            The service to remove from the register
     */
    protected void removeService(Service service) {
        m_services.remove(service);
    }

    /**
     * Retrieves the list of services currently in the register.
     * 
     * @return The list of services which are current started
     */
    protected List<Service> getServices() {
        return m_services;
    }
}
