package org.opennms.core.grid;

import java.util.EventObject;

public class Message<T> extends EventObject {
    private static final long serialVersionUID = -7297563759836498729L;

    private final T messageObject;

    public Message(String topicName, T messageObject) {
        super(topicName);
        this.messageObject = messageObject;
    }

    public T getMessageObject() {
        return messageObject;
    }
}
