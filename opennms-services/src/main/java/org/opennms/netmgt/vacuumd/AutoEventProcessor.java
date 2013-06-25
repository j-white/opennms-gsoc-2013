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

package org.opennms.netmgt.vacuumd;

import org.opennms.core.utils.ThreadCategory;
import org.opennms.netmgt.config.vacuumd.AutoEvent;
import org.opennms.netmgt.model.events.EventBuilder;
import org.opennms.netmgt.xml.event.Event;

/**
 * @deprecated Use {@link ActionEventProcessor} instead.
 */
public class AutoEventProcessor {

    private final String m_automationName;
    private final AutoEvent m_autoEvent;

    /**
     * @deprecated Use {@link ActionEventProcessor} instead.
     */
    public AutoEventProcessor(String automationName, AutoEvent autoEvent) {
        m_automationName = automationName;
        m_autoEvent = autoEvent;
    }

    public ThreadCategory log() {
        return ThreadCategory.getInstance(getClass());
    }

    public boolean hasEvent() {
        return m_autoEvent != null;
    }

    public AutoEvent getAutoEvent() {
        return m_autoEvent;
    }

    String getUei() {
        if (hasEvent()) {
            return getAutoEvent().getUei().getContent();
        } else {
            return null;
        }
    }

    void send() {

        if (hasEvent()) {
            // create and send event
            log().debug("AutoEventProcessor: Sending auto-event " + getUei()
                                + " for automation " + m_automationName);

            EventBuilder bldr = new EventBuilder(getUei(), "Automation");
            sendEvent(bldr.getEvent());
        } else {
            log().debug("AutoEventProcessor: No auto-event for automation "
                                + m_automationName);
        }
    }

    private void sendEvent(Event event) {
        Vacuumd.getSingleton().getEventManager().sendNow(event);
    }

}
