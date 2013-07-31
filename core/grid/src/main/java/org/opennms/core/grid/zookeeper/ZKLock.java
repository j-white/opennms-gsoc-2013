package org.opennms.core.grid.zookeeper;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

public class ZKLock extends InterProcessMutex implements Lock {

    public static final String PATH_PREFIX = "/onms/locks/";

    public ZKLock(CuratorFramework client, String name) {
        super(client, PATH_PREFIX + name);
    }

    @Override
    public void lock() {
        try {
            acquire();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException();
        }
        lock();
    }

    @Override
    public boolean tryLock() {
        try {
            return tryLock(10, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignore) {
            Thread.interrupted();
            return false;
        }
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit)
            throws InterruptedException {
        try {
            return acquire(time, unit);
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void unlock() {
        try {
            release();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }
}
