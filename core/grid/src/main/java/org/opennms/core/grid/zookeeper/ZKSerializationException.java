package org.opennms.core.grid.zookeeper;

public class ZKSerializationException extends RuntimeException {
    private static final long serialVersionUID = 9058622645650793042L;

    public ZKSerializationException(Throwable t) {
        super(t);
    }
}
