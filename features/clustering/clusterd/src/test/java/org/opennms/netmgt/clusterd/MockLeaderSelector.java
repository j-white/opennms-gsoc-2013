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

package org.opennms.netmgt.clusterd;

import org.opennms.netmgt.clusterd.LeaderSelector;
import org.opennms.netmgt.clusterd.LeaderSelectorListener;

/**
 * Mock leader selector which automatically grants
 * leadership when started.
 *
 * @author jwhite
 */
public class MockLeaderSelector extends LeaderSelector {
    /**
     * Thread for calling the the listener.
     */
    private Thread m_thread = null;

    private long m_msBeforeCall;

    public MockLeaderSelector(LeaderSelectorListener listener) {
        this(listener, 0);
    }

    public MockLeaderSelector(LeaderSelectorListener listener, long msBeforeCall) {
        setListener(listener);
        m_msBeforeCall = msBeforeCall;
    }

    public void start() {
        Thread m_thread = new Thread(this);
        m_thread.start();
    }

    public void stop() {
        if (m_thread != null) {
            m_thread.interrupt();
        }
    }

    @Override
    public void run() {
        if (m_msBeforeCall > 0) {
            try {
                Thread.sleep(m_msBeforeCall);
            } catch(InterruptedException e) {
                return;
            }
        }
        getListener().takeLeadership();
    }
}
