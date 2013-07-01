package org.opennms.core.grid.hazelcast;

import org.opennms.core.grid.MessageListener;
import org.opennms.core.grid.Topic;

import com.hazelcast.core.ITopic;

public class HazcelcastTopic<T> implements Topic<T> {
    ITopic<T> m_hazelcastTopic;

    public HazcelcastTopic(ITopic<T> topic) {
        m_hazelcastTopic = topic;
    }

    @Override
    public String getName() {
        return m_hazelcastTopic.getName();
    }

    @Override
    public void publish(T message) {
        m_hazelcastTopic.publish(message);
    }

    @Override
    public void addMessageListener(MessageListener<T> listener) {
        m_hazelcastTopic.addMessageListener(new HazelcastMessageListener<T>(
                                                                            listener));
    }

    @Override
    public void removeMessageListener(MessageListener<T> listener) {
        // TODO: FIXME
    }
}
