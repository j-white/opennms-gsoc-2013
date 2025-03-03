/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2012 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2012 The OpenNMS Group, Inc.
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

package org.opennms.features.topology.api;

import org.opennms.features.topology.api.topo.VertexRef;

import java.util.List;
import java.util.Set;

/**
 * A VerticesUpdateManager is responsible to publish all nodeIds
 * which are currently in focus (e.g. selected or displayable).
 */
public interface VerticesUpdateManager extends SelectionListener, GraphContainer.ChangeListener {

    /**
     * If you want to listen to {@link VerticesUpdateEvent}s, implement this interface.
     */
    interface VerticesUpdateListener {
        void verticesUpdated(VerticesUpdateEvent event);
    }

    class VerticesUpdateEvent {

        private final Set<VertexRef> m_vertexRefs;
        private final boolean m_displayingAllVertices;

        public VerticesUpdateEvent(Set<VertexRef> vertexRefs) {
            this(vertexRefs, false);
        }

        public VerticesUpdateEvent(Set<VertexRef> vertexRefs, boolean displayingAllVertices){
            m_vertexRefs = vertexRefs;
            m_displayingAllVertices = displayingAllVertices;
        }

        public Set<VertexRef> getVertexRefs() {
            return m_vertexRefs;
        }

        public boolean allVerticesSelected(){
            return m_displayingAllVertices;
        }
    }
}
