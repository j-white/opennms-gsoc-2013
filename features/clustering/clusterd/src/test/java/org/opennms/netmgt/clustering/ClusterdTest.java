/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2006-2013 The OpenNMS Group, Inc.
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

package org.opennms.netmgt.clustering;

import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opennms.core.test.MockLogAppender;
import org.opennms.core.test.OpenNMSJUnit4ClassRunner;
import org.opennms.netmgt.config.ServiceConfigDao;
import org.opennms.netmgt.config.service.Service;
import org.opennms.netmgt.config.service.types.ServiceType;
import org.opennms.netmgt.vmmgr.ServiceManager;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;

@RunWith(OpenNMSJUnit4ClassRunner.class)
@ContextConfiguration(locations = {
        "classpath:/META-INF/opennms/applicationContext-soa.xml",
        "classpath*:/META-INF/opennms/component-dao.xml",
        "classpath:META-INF/opennms/applicationContext-clusterd.xml" })
public class ClusterdTest implements InitializingBean {
    @Autowired
    private Clusterd m_clusterd = null;

    @Override
    public void afterPropertiesSet() {
        assertNotNull(m_clusterd);
    }

    @Before
    public void setUp() {
        MockLogAppender.setupLogging(true);
    }

    @After
    public void tearDown() {
        MockLogAppender.assertNoWarningsOrGreater();
    }

    @Test
    public void servicesAreStartedOnElect() throws InterruptedException {
        // Invoke the leader function immediately when started
        MockLeaderSelector mockLeaderSelector = new MockLeaderSelector(
                                                                       m_clusterd);
        m_clusterd.setLeaderSelector(mockLeaderSelector);

        // Return a fixed set of services
        ServiceConfigDao mockServiceConfigDao = EasyMock.createMock(ServiceConfigDao.class);
        List<Service> services = new ArrayList<Service>();
        Service svc1 = new Service();
        svc1.setName("svc1");
        svc1.setType(ServiceType.VANILLA);
        services.add(svc1);

        Service svc2 = new Service();
        svc2.setName("svc2");
        svc2.setType(ServiceType.VANILLA);
        services.add(svc2);

        EasyMock.expect(mockServiceConfigDao.getServicesOfType(ServiceType.VANILLA)).andReturn(services);
        EasyMock.replay(mockServiceConfigDao);
        m_clusterd.setServiceConfigDao(mockServiceConfigDao);

        // Expect a start call for the service that is not marked as started
        ServiceManager mockServiceManager = EasyMock.createMock(ServiceManager.class);
        EasyMock.expect(mockServiceManager.isStarted(svc1)).andReturn(true);
        EasyMock.expect(mockServiceManager.isStarted(svc2)).andReturn(false);

        List<Service> servicesThatShouldBeStarted = new ArrayList<Service>();
        servicesThatShouldBeStarted.add(svc2);
        mockServiceManager.start(servicesThatShouldBeStarted);
        EasyMock.expectLastCall();
        EasyMock.replay(mockServiceManager);
        m_clusterd.setServiceManager(mockServiceManager);

        // Start the daemon and stop it immediately
        m_clusterd.start();
        m_clusterd.stop();

        // Verify the mock calls
        EasyMock.verify(mockServiceConfigDao);
        EasyMock.verify(mockServiceManager);
    }
}
