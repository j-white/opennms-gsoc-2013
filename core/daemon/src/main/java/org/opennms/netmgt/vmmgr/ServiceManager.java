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

import java.util.List;

import org.opennms.netmgt.config.service.Service;

/**
 * Allows services to be started/stopped after the manager has been started.
 * 
 * TODO: Move this in with manager code and have the manager use this same
 * interface.
 * 
 * @author jwhite
 * 
 */
public interface ServiceManager {

    /**
     * Starts a single service.
     * 
     * @param service
     *            The service to start
     */
    public void start(Service service);

    /**
     * Starts all of the services in the list. The services are started in
     * they same order as they appear.
     * 
     * @param servicesToStart
     *            The list of services to start
     */
    public void start(List<Service> servicesToStart);

    /**
     * Stops a single service.
     * 
     * @param service
     *            The service to stop
     */
    public void stop(Service service);

    /**
     * Stops all of the services in the list. The services are stopped in they
     * same order as they appear.
     * 
     * @param servicesToStop
     *            The list of services to stop
     */
    public void stop(List<Service> servicesToStop);

    /**
     * Checks if a service is started or not.
     * 
     * @param service
     *            The service to check
     * @return True if the service is started, false otherwise
     */
    boolean isStarted(Service service);
}
