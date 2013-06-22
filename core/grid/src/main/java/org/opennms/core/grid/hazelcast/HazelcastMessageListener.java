package org.opennms.core.grid.hazelcast;

import org.opennms.core.grid.Message;
import org.opennms.core.grid.MessageListener;

public class HazelcastMessageListener<T> implements com.hazelcast.core.MessageListener<T>  {

    private MessageListener<T> m_gridMessageListener;
    
    public HazelcastMessageListener(MessageListener<T> gridMessageListener) {
        m_gridMessageListener = gridMessageListener;
    }

    @Override
    public void onMessage(com.hazelcast.core.Message<T> message) {
        m_gridMessageListener.onMessage(new Message<T>("", message.getMessageObject()));
    }
}
