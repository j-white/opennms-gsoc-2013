package org.opennms.core.grid.zookeeper;

public class ZKException extends RuntimeException {
    private static final long serialVersionUID = 4823798834874882209L;

    public ZKException(Throwable t) {
        super(t);
    }
}
