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

import java.util.regex.Pattern;

import org.opennms.core.utils.PropertiesUtils;
import org.opennms.core.utils.ThreadCategory;
import org.opennms.netmgt.config.vacuumd.Assignment;
import org.opennms.netmgt.model.events.EventBuilder;

public class EventAssignment {

    static final Pattern s_pattern = Pattern.compile("\\$\\{(\\w+)\\}");
    private final Assignment m_assignment;

    public EventAssignment(Assignment assignment) {
        m_assignment = assignment;
    }

    public ThreadCategory log() {
        return ThreadCategory.getInstance(getClass());
    }

    public void assign(EventBuilder bldr, PropertiesUtils.SymbolTable symbols) {

        String val = PropertiesUtils.substitute(m_assignment.getValue(),
                                                symbols);

        if (m_assignment.getValue().equals(val)
                && s_pattern.matcher(val).matches()) {
            // no substitution was made the value was a token pattern so
            // skip it
            return;
        }

        if ("field".equals(m_assignment.getType())) {
            bldr.setField(m_assignment.getName(), val);
        } else {
            bldr.addParam(m_assignment.getName(), val);
        }
    }

}
