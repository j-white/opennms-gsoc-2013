package org.opennms.core.grid.zookeeper;

public class ZKExceptionHandler {
    public static void handle(Exception ex) {
        if (ex instanceof RuntimeException) {
            throw (RuntimeException) ex;
        }

        throw new ZKException(ex);
    }
}
