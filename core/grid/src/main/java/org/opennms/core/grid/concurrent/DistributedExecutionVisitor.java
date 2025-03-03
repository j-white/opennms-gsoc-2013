package org.opennms.core.grid.concurrent;

public interface DistributedExecutionVisitor {

    public void beforeExecute(Thread t, Runnable r);

    public void afterExecute(Runnable r, Throwable t);

}
