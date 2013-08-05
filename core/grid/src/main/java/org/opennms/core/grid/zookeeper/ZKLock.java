package org.opennms.core.grid.zookeeper;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.apache.curator.RetryLoop;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

/**
 * @see org.apache.curator.framework.recipes.locks.InterProcessMutex
 * @author jwhite
 */
public class ZKLock extends InterProcessMutex implements Lock {
    public static final String PATH_PREFIX = "/onms/locks/";

    private final CuratorFramework m_client;

    public ZKLock(CuratorFramework client, String name) {
        super(client, PATH_PREFIX + name);
        m_client = client;
    }

    @Override
    public void lock() {
        try {
            UninterruptibleRetryLoop retryLoop = new UninterruptibleRetryLoop(m_client);
            while (retryLoop.shouldContinue()) {
                try {
                    acquire();
                    retryLoop.markComplete();
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            ZKExceptionHandler.handle(e);
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
            // Ignore the interrupted exception
            Thread.interrupted();
            return false;
        }
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit)
            throws InterruptedException {
        try {
            RetryLoop retryLoop = m_client.getZookeeperClient().newRetryLoop();
            while (retryLoop.shouldContinue()) {
                try {
                    return acquire(time, unit);
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (InterruptedException rethrow) {
            throw rethrow;
        } catch (Exception e) {
            ZKExceptionHandler.handle(e);
        }

        // Should never get here
        return false;
    }

    @Override
    public void unlock() {
        try {
            UninterruptibleRetryLoop retryLoop = new UninterruptibleRetryLoop(m_client);
            while (retryLoop.shouldContinue()) {
                try {
                    release();
                    retryLoop.markComplete();
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            ZKExceptionHandler.handle(e);
        }
    }

    @Override
    public Condition newCondition() {
        return newCondition("default");
    }

    public Condition newCondition(final String name) {
        return new ZKCondition(m_client, this, name);
    }
}
