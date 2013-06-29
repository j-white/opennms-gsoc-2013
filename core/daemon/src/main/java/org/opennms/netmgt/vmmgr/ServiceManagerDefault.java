/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2007-2013 The OpenNMS Group, Inc.
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

package org.opennms.netmgt.vmmgr;

import java.lang.management.ManagementFactory;
import java.util.List;

import javax.management.MBeanServer;

import org.opennms.netmgt.config.service.Service;
import org.opennms.netmgt.config.service.types.InvokeAtType;
import org.opennms.netmgt.vmmgr.Invoker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Allows services to be started or stopped.
 * 
 * Services are added to a registry when started, and removed when stopped.
 * The registry is used to iterate over all the services that have been
 * started when a "status" query is issued.
 * 
 * @author jwhite
 * 
 */
public class ServiceManagerDefault implements ServiceManager {
    private static final Logger LOG = LoggerFactory.getLogger(ServiceManagerDefault.class);

    private MBeanServer m_mbeanServer;

    /**
     * Default constructor
     */
    public ServiceManagerDefault() {
        m_mbeanServer = ManagementFactory.getPlatformMBeanServer();
    }

    /** {@inheritDoc} */
    public void start(final List<Service> servicesToStart) {
        start(servicesToStart, true);
    }

    /**
     * Starts all of the services in the list.
     * 
     * The services are started in they same order as they appear.
     * 
     * @param servicesToStart
     *            The list of services to start
     * @param exitOnFailure
     *            True if Manager.doSystemExit() should be called when a
     *            service fails to start, false otherwise.
     */
    public void start(final List<Service> servicesToStart,
            boolean exitOnFailure) {
        Invoker invoker = new Invoker();
        invoker.setServer(m_mbeanServer);
        invoker.setAtType(InvokeAtType.START);
        List<InvokerService> services = InvokerService.createServiceList(servicesToStart);
        invoker.setServices(services);
        invoker.instantiateClasses();

        List<InvokerResult> resultInfo = invoker.invokeMethods();

        for (InvokerResult result : resultInfo) {
            if (result != null && result.getThrowable() != null) {
                Service svc = result.getService();
                String name = svc.getName();
                String className = svc.getClassName();

                String message = String.format("An error occurred while attempting to "
                                                       + "start the \"%s\" service (class %s)%s.",
                                               name,
                                               className,
                                               exitOnFailure ? " Shutting down and exiting."
                                                            : "");
                LOG.error(message, result.getThrowable());
                System.err.println(message);
                result.getThrowable().printStackTrace();

                if (exitOnFailure) {
                    Manager manager = new Manager();
                    manager.stop();
                    manager.doSystemExit();

                    // Shouldn't get here
                    return;
                }

                continue;
            }

            // The service was successfully started, add it to the registry
            LOG.debug("Succesfully started "
                                + result.getService().getName());
            ServiceRegister.getInstance().addService(result.getService());
        }
    }

    /** {@inheritDoc} */
    public void stop(List<Service> servicesToStop) {
        Invoker invoker = new Invoker();
        invoker.setServer(m_mbeanServer);
        invoker.setAtType(InvokeAtType.STOP);
        invoker.setReverse(true);
        invoker.setFailFast(false);

        List<InvokerService> services = InvokerService.createServiceList(servicesToStop);
        invoker.setServices(services);
        invoker.getObjectInstances();

        List<InvokerResult> resultInfo = invoker.invokeMethods();

        for (InvokerResult result : resultInfo) {
            if (result != null) {
                // The service was stopped, remove it from the registry
                LOG.debug("Succesfully stopped "
                                    + result.getService().getName());
                ServiceRegister.getInstance().removeService(result.getService());
            }
        }
    }

    /** {@inheritDoc} */
    public boolean isStarted(Service service) {
        return ServiceRegister.getInstance().getServices().contains(service);
    }
}
