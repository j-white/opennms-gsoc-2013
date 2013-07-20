package org.opennms.netmgt.config;

import java.util.List;

import org.opennms.netmgt.config.service.Service;
import org.opennms.netmgt.config.service.types.ServiceType;

public interface ServiceConfigDao {

    /**
     * Returns an array of all the defined configuration information for the
     * <em>Services</em>. If there are no defined services an array of length
     * zero is returned to the caller.
     * 
     * @return An array holding a reference to all the Service configuration
     *         instances.
     */
    public Service[] getServices();

    /**
     * Returns a list containing all of the Service definitions. If there are
     * no services define a list of size zero zero is returned to the caller.
     * 
     * @return A List holding a reference to all the Service configuration
     *         instances.
     */
    public List<Service> getServiceList();

    public List<Service> getServicesOfType(ServiceType type);

    public List<Service> getServicesWithoutType(ServiceType type);
}
