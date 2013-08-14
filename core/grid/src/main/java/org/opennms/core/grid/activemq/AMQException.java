package org.opennms.core.grid.activemq;

public class AMQException extends RuntimeException {
    private static final long serialVersionUID = 9028179921093296405L;

    public AMQException(Throwable t) {
        super(t);
    }
}
