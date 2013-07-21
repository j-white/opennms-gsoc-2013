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

package org.opennms.netmgt.eventd.camel.grid;

import static com.jayway.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;

import java.util.concurrent.Callable;

import org.apache.camel.CamelContext;
import org.apache.camel.ServiceStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opennms.core.grid.DataGridProvider;
import org.opennms.core.grid.DataGridProviderFactory;
import org.opennms.core.test.MockLogAppender;
import org.opennms.core.test.OpenNMSJUnit4ClassRunner;
import org.opennms.core.test.grid.annotations.JUnitGrid;
import org.opennms.core.utils.BeanUtils;
import org.opennms.netmgt.EventConstants;
import org.opennms.netmgt.eventd.camel.CamelEventProducer;
import org.opennms.netmgt.eventd.camel.CamelEventReceiver;
import org.opennms.netmgt.eventd.camel.MockCamelBroadcastEventHandler;
import org.opennms.netmgt.eventd.camel.grid.GridRouteBuilder;
import org.opennms.netmgt.model.events.EventBuilder;
import org.opennms.netmgt.xml.event.Event;
import org.opennms.netmgt.xml.event.Header;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;

@RunWith(OpenNMSJUnit4ClassRunner.class)
@ContextConfiguration(locations = {
        "classpath:/META-INF/opennms/applicationContext-soa.xml",
        "classpath*:/META-INF/opennms/component-dao.xml",
        "classpath:META-INF/opennms/applicationContext-camelEventd.xml",
        "classpath:META-INF/opennms/applicationContext-camelEventdTest.xml" })
@JUnitGrid(reuseGrid = true)
public class EventdCamelGridIntegrationTest {
    @Autowired
    protected CamelContext camelContext;

    @Autowired
    protected CamelEventReceiver camelEventReceiver;

    @Autowired
    protected CamelEventProducer camelEventProducer;

    @Autowired
    protected MockCamelBroadcastEventHandler mockBroadcastHandler;

    @Autowired
    protected DataGridProvider dataGridProvider;

    @Autowired
    protected GridRouteManager gridRouteManager;

    @Before
    public void setUp() {
        BeanUtils.assertAutowiring(this);

        MockLogAppender.setupLogging(true, "INFO");

        camelEventReceiver.start();
        mockBroadcastHandler.reset();
    }

    @After
    public void tearDown() throws Exception {
        MockLogAppender.assertNoErrorOrGreater();
    }

    @Test
    public void testEventBroadcasterAndReceiver() throws Exception {
        // Make sure the cluster only has one member
        assertEquals(Integer.valueOf(1),
                     getNumClusterMembers(dataGridProvider).call());

        // Add a second member to the cluster, the member listener should
        // trigger the addition of a new route
        DataGridProvider anotherDataGridProvider = DataGridProviderFactory.getNewInstance();
        anotherDataGridProvider.init();

        // Wait until the second member joins the cluster
        await().until(getNumClusterMembers(dataGridProvider), is(2));

        // Ensure there are no events processed
        assertEquals(0, mockBroadcastHandler.getNumEventsProcessed());

        // Create a mock event
        EventBuilder eventBuilder = new EventBuilder(
                                                     EventConstants.EVENTSCONFIG_CHANGED_EVENT_UEI,
                                                     "maple bacon doughnut");
        Event expectedEvent = eventBuilder.getEvent();

        // Broadcast our mock event
        camelEventProducer.process(new Header(), expectedEvent);

        // Verify that the event handler gets invoked
        await().until(getNumEventsProcessed(), is(1));

        // Compare the received event to the mock
        Event event = mockBroadcastHandler.getLastEvent();
        assertEquals(expectedEvent.getUei(), event.getUei());

        // Make sure the processor was run
        assertEquals(true, event.getLocal());
        assertEquals("donotpersist", event.getLogmsg().getDest());

        // Make sure we can access the route's state
        GridRouteBuilder routeBuilder = new GridRouteBuilder(
                                                             anotherDataGridProvider.getLocalMember());
        assertEquals(ServiceStatus.Started,
                     getRouteStatus(routeBuilder.getRouteId()).call());

        // Shutdown the second member
        anotherDataGridProvider.shutdown();

        // Verify that the route gets removed
        await().until(getRouteStatus(routeBuilder.getRouteId()), nullValue());
    }

    private Callable<Integer> getNumClusterMembers(
            final DataGridProvider dataGridProvider) {
        return new Callable<Integer>() {
            public Integer call() throws Exception {
                return dataGridProvider.getGridMembers().size();
            }
        };
    }

    private Callable<ServiceStatus> getRouteStatus(final String routeId) {
        return new Callable<ServiceStatus>() {
            public ServiceStatus call() throws Exception {
                return camelContext.getRouteStatus(routeId);
            }
        };
    }

    private Callable<Integer> getNumEventsProcessed() {
        return new Callable<Integer>() {
            public Integer call() {
                return mockBroadcastHandler.getNumEventsProcessed();
            }
        };
    }
}
