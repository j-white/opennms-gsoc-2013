package org.opennms.core.grid.zookeeper;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import org.apache.curator.framework.CuratorFramework;

/**
 * TODO
 *
 * @author jwhite
 */
public class ZKCondition implements Condition {

    public ZKCondition(CuratorFramework client, ZKLock lock, String name) {
        // TODO Auto-generated method stub
    }

    @Override
    public void await() throws InterruptedException {
        // TODO Auto-generated method stub
    }

    @Override
    public void awaitUninterruptibly() {
        // TODO Auto-generated method stub
    }

    @Override
    public long awaitNanos(long nanosTimeout) throws InterruptedException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean await(long time, TimeUnit unit)
            throws InterruptedException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean awaitUntil(Date deadline) throws InterruptedException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void signal() {
        // TODO Auto-generated method stub
    }

    @Override
    public void signalAll() {
        // TODO Auto-generated method stub
    }
}
