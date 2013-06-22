package org.opennms.core.grid;


public interface Topic<T> {

    String getName();

    void publish(T message);

    void addMessageListener(MessageListener<T> listener);

    void removeMessageListener(MessageListener<T> listener);
}
