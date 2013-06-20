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
import java.util.ArrayList;
import java.util.List;

import javax.management.MBeanServer;

import org.opennms.core.utils.ThreadCategory;
import org.opennms.netmgt.config.service.Service;
import org.opennms.netmgt.config.service.types.InvokeAtType;
import org.opennms.netmgt.vmmgr.Invoker;

/**
 * Used to start/stop services.
 * 
 * @author jwhite
 * 
 */
public class ServiceManagerDefault implements ServiceManager {
    private MBeanServer m_mbeanServer;

    public ServiceManagerDefault() {
        m_mbeanServer = ManagementFactory.getPlatformMBeanServer();
    }

    /** {@inheritDoc} */
    public void start(Service service) {
        List<Service> servicesToStart = new ArrayList<Service>(1);
        servicesToStart.add(service);
        start(servicesToStart);
    }

    /** {@inheritDoc} */
    public void start(List<Service> servicesToStart) {
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

                String message = "An error occurred while attempting to start the \""
                        + name + "\" service (class " + className + ").";
                log().fatal(message, result.getThrowable());
                System.err.println(message);
                result.getThrowable().printStackTrace();

                continue;
            }

            // The service was successfully started, add it to the registry
            log().debug("Succesfully started "
                                + result.getService().getName());
            ServiceRegistry.getInstance().addService(result.getService());
        }
    }

    /** {@inheritDoc} */
    public void stop(Service service) {
        List<Service> servicesToStop = new ArrayList<Service>(1);
        servicesToStop.add(service);
        stop(servicesToStop);
    }

    /** {@inheritDoc} */
    public void stop(List<Service> servicesToStop) {
        Invoker invoker = new Invoker();
        invoker.setServer(m_mbeanServer);
        invoker.setAtType(InvokeAtType.STOP);
        List<InvokerService> services = InvokerService.createServiceList(servicesToStop);
        invoker.setServices(services);
        invoker.instantiateClasses();

        List<InvokerResult> resultInfo = invoker.invokeMethods();

        for (InvokerResult result : resultInfo) {
            if (result != null) {
                // The service was stopped, remove it from the registry
                log().debug("Succesfully stopped "
                                    + result.getService().getName());
                ServiceRegistry.getInstance().removeService(result.getService());
            }
        }
    }

    /** {@inheritDoc} */
    public boolean isStarted(Service service) {
        return ServiceRegistry.getInstance().getServices().contains(service);
    }

    private ThreadCategory log() {
        return ThreadCategory.getInstance(getClass());
    }
}
