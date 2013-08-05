package org.opennms.core.grid.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.opennms.core.grid.AtomicLong;

public class ZKAtomicLong implements AtomicLong {
    public static final String PATH_PREFIX = "/onms/atomic/";

    private final CuratorFramework m_client;
    private final DistributedAtomicLong m_atomicLong;

    public ZKAtomicLong(CuratorFramework client, String name) {
        m_client = client;
        m_atomicLong = new DistributedAtomicLong(client, PATH_PREFIX + name, ZKConfigFactory.getInstance().getRetryPolicy());
    }

    @Override
    public long addAndGet(long delta) {
        try {
            UninterruptibleRetryLoop retryLoop = new UninterruptibleRetryLoop(m_client);
            while (retryLoop.shouldContinue()) {
                try {
                    return m_atomicLong.add(delta).postValue();
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            ZKExceptionHandler.handle(e);
        }

        // Should never get here
        return 0L;
    }

    @Override
    public boolean compareAndSet(long expect, long update) {
        try {
            UninterruptibleRetryLoop retryLoop = new UninterruptibleRetryLoop(m_client);
            while (retryLoop.shouldContinue()) {
                try {
                    return m_atomicLong.compareAndSet(expect, update).postValue() == update;
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            ZKExceptionHandler.handle(e);
        }

        // Should never get here
        return false;
    }

    @Override
    public long decrementAndGet() {
        try {
            UninterruptibleRetryLoop retryLoop = new UninterruptibleRetryLoop(m_client);
            while (retryLoop.shouldContinue()) {
                try {
                    return m_atomicLong.decrement().postValue();
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            ZKExceptionHandler.handle(e);
        }

        // Should never get here
        return 0L;
    }

    @Override
    public long getAndDecrement() {
        try {
            UninterruptibleRetryLoop retryLoop = new UninterruptibleRetryLoop(m_client);
            while (retryLoop.shouldContinue()) {
                try {
                    return m_atomicLong.decrement().preValue();
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            ZKExceptionHandler.handle(e);
        }

        // Should never get here
        return 0L;
    }

    @Override
    public long get() {
        try {
            UninterruptibleRetryLoop retryLoop = new UninterruptibleRetryLoop(m_client);
            while (retryLoop.shouldContinue()) {
                try {
                    return m_atomicLong.get().postValue();
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            ZKExceptionHandler.handle(e);
        }

        // Should never get here
        return 0L;
    }

    @Override
    public long getAndAdd(long delta) {
        try {
            UninterruptibleRetryLoop retryLoop = new UninterruptibleRetryLoop(m_client);
            while (retryLoop.shouldContinue()) {
                try {
                    return m_atomicLong.add(delta).preValue();
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            ZKExceptionHandler.handle(e);
        }

        // Should never get here
        return 0L;
    }

    @Override
    public long getAndSet(long newValue) {
        try {
            UninterruptibleRetryLoop retryLoop = new UninterruptibleRetryLoop(m_client);
            while (retryLoop.shouldContinue()) {
                try {
                    return m_atomicLong.trySet(newValue).preValue();
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            ZKExceptionHandler.handle(e);
        }

        // Should never get here
        return 0L;
    }

    @Override
    public long incrementAndGet() {
        try {
            UninterruptibleRetryLoop retryLoop = new UninterruptibleRetryLoop(m_client);
            while (retryLoop.shouldContinue()) {
                try {
                    return m_atomicLong.increment().postValue();
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            ZKExceptionHandler.handle(e);
        }

        // Should never get here
        return 0L;
    }

    @Override
    public long getAndIncrement() {
        try {
            UninterruptibleRetryLoop retryLoop = new UninterruptibleRetryLoop(m_client);
            while (retryLoop.shouldContinue()) {
                try {
                    return m_atomicLong.increment().preValue();
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            ZKExceptionHandler.handle(e);
        }

        // Should never get here
        return 0L;
    }

    @Override
    public void set(long newValue) {
        try {
            UninterruptibleRetryLoop retryLoop = new UninterruptibleRetryLoop(m_client);
            while (retryLoop.shouldContinue()) {
                try {
                    m_atomicLong.trySet(newValue);
                    retryLoop.markComplete();
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            ZKExceptionHandler.handle(e);
        }
    }

    public String toString() {
        return Long.valueOf(get()).toString();
    }
}
