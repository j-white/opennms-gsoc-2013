package org.opennms.core.grid.activemq;

import java.util.concurrent.BlockingQueue;

import org.opennms.core.grid.DataGridProvider;

public abstract class ActiveMQGridProvider implements DataGridProvider {

    /**
     * 
     */
    public <T> BlockingQueue<T> getQueue(String name) {
        return null;
    }
}
