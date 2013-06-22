package org.opennms.core.grid;

import java.util.EventListener;

public interface MessageListener<T> extends EventListener {
    void onMessage(Message<T> message);
}
