package org.opennms.core.grid.test.primitives;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.Test;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 * Other contributors include Andrew Wright, Jeffrey Hayes, 
 * Pat Fisher, Mike Judd.
 * 
 * Adapted by Jesse White to work with the primitives provided
 * via the grid provider interface.
 */
public class LockTest extends JSR166TestCase {
    public static final int LOCK_TEST_TIMEOUT = 3000;

    private Lock getNewLock() {
        return gridProvider.getLock("lock" + ROLLING_ID++);
    }

    private Condition getNewCondition(Lock lock) {
        return gridProvider.getCondition(lock, "condition" + ROLLING_ID++);
    }

    /**
     * A runnable calling lockInterruptibly
     */
    class InterruptibleLockRunnable implements Runnable {
        final Lock lock;
        InterruptibleLockRunnable(Lock l) { lock = l; }
        public void run() {
            try {
                lock.lockInterruptibly();
            } catch(InterruptedException success){}
        }
    }

    /**
     * A runnable calling lockInterruptibly that expects to be
     * interrupted
     */
    class InterruptedLockRunnable implements Runnable {
        final Lock lock;
        InterruptedLockRunnable(Lock l) { lock = l; }
        public void run() {
            try {
                lock.lockInterruptibly();
                threadShouldThrow();
            } catch(InterruptedException success){}
        }
    }

    /**
     * Subclass to expose protected methods
     */
    static class PublicReentrantLock extends ReentrantLock {
        private static final long serialVersionUID = -1389981798845256344L;
        PublicReentrantLock() { super(); }
        public Collection<Thread> getQueuedThreads() { 
            return super.getQueuedThreads(); 
        }
        public Collection<Thread> getWaitingThreads(Condition c) { 
            return super.getWaitingThreads(c); 
        }
    }

    /**
     * locking an unlocked lock succeeds
     */
    @Test(timeout = LOCK_TEST_TIMEOUT)
    public void testLock() { 
        Lock lock = getNewLock();
        lock.lock();
        lock.unlock();
    }

    /**
     * tryLock on an unlocked lock succeeds
     */
    @Test(timeout = LOCK_TEST_TIMEOUT)
    public void testTryLock() { 
        Lock lock = getNewLock();
        assertTrue(lock.tryLock());
        lock.unlock();
    }

    /**
     * timed tryLock is interruptible.
     */
    @Test(timeout = LOCK_TEST_TIMEOUT)
    public void testInterruptedException2() { 
        final Lock lock = getNewLock();
        lock.lock();
        Thread t = new Thread(new Runnable() {
                public void run() {
                    try {
                        lock.tryLock(MEDIUM_DELAY_MS,TimeUnit.MILLISECONDS);
                        threadShouldThrow();
                    } catch(InterruptedException success){}
                }
            });
        try {
            t.start();
            t.interrupt();
        } catch(Exception e){
            unexpectedException();
        }
    }

    /**
     * TryLock on a locked lock fails
     */
    @Test(timeout = LOCK_TEST_TIMEOUT)
    public void testTryLockWhenLocked() { 
        final Lock lock = getNewLock();
        lock.lock();
        Thread t = new Thread(new Runnable() {
                public void run() {
                    threadAssertFalse(lock.tryLock());
                }
            });
        try {
            t.start();
            t.join();
            lock.unlock();
        } catch(Exception e){
            unexpectedException();
        }
    } 

    /**
     * Timed tryLock on a locked lock times out
     */
    @Test(timeout = LOCK_TEST_TIMEOUT)
    public void testTryLock_Timeout() { 
        final Lock lock = getNewLock();
        lock.lock();
        Thread t = new Thread(new Runnable() {
                public void run() {
                    try {
                        threadAssertFalse(lock.tryLock(1, TimeUnit.MILLISECONDS));
                    } catch (Exception ex) {
                        threadUnexpectedException();
                    }
                }
            });
        try {
            t.start();
            t.join();
            lock.unlock();
        } catch(Exception e){
            unexpectedException();
        }
    } 

    /**
     *  timed await without a signal times out
     */
    @Test(timeout = LOCK_TEST_TIMEOUT)
    public void testAwait_Timeout() {
        final Lock lock = getNewLock();
        final Condition c = getNewCondition(lock);
        try {
            lock.lock();
            assertFalse(c.await(SHORT_DELAY_MS, TimeUnit.MILLISECONDS));
            lock.unlock();
        }
        catch (Exception ex) {
            unexpectedException();
        }
    }

    /**
     * awaitUntil without a signal times out
     */
    @Test(timeout = LOCK_TEST_TIMEOUT)
    public void testAwaitUntil_Timeout() {
        final Lock lock = getNewLock();
        final Condition c = getNewCondition(lock);
        try {
            lock.lock();
            java.util.Date d = new java.util.Date();
            assertFalse(c.awaitUntil(new java.util.Date(d.getTime() + 10)));
            lock.unlock();
        }
        catch (Exception ex) {
            unexpectedException();
        }
    }

    /**
     * await returns when signalled
     */
    @Test(timeout = LOCK_TEST_TIMEOUT)
    public void testAwait() {
        final Lock lock = getNewLock();
        final Condition c = getNewCondition(lock);
        Thread t = new Thread(new Runnable() { 
                public void run() {
                    try {
                        lock.lock();
                        c.await();
                        lock.unlock();
                    }
                    catch(InterruptedException e) {
                        threadUnexpectedException();
                    }
                }
            });

        try {
            t.start();
            Thread.sleep(SHORT_DELAY_MS);
            lock.lock();
            c.signal();
            lock.unlock();
            t.join(SHORT_DELAY_MS);
            assertFalse(t.isAlive());
        }
        catch (Exception ex) {
            unexpectedException();
        }
    }

    /**
     * awaitUninterruptibly doesn't abort on interrupt
     */
    @Test(timeout = LOCK_TEST_TIMEOUT)
    public void testAwaitUninterruptibly() {
        final Lock lock = getNewLock();
        final Condition c = getNewCondition(lock);
        Thread t = new Thread(new Runnable() { 
                public void run() {
                    lock.lock();
                    c.awaitUninterruptibly();
                    lock.unlock();
                }
            });

        try {
            t.start();
            Thread.sleep(SHORT_DELAY_MS);
            t.interrupt();
            lock.lock();
            c.signal();
            lock.unlock();
            assert(t.isInterrupted());
            t.join(SHORT_DELAY_MS);
            assertFalse(t.isAlive());
        }
        catch (Exception ex) {
            unexpectedException();
        }
    }

    /**
     * signalAll wakes up all threads
     */
    @Test(timeout = LOCK_TEST_TIMEOUT)
    public void testSignalAll() {
        final Lock lock = getNewLock();
        final Condition c = getNewCondition(lock);
        Thread t1 = new Thread(new Runnable() { 
                public void run() {
                    try {
                        lock.lock();
                        c.await();
                        lock.unlock();
                    }
                    catch(InterruptedException e) {
                        threadUnexpectedException();
                    }
                }
            });

        Thread t2 = new Thread(new Runnable() { 
                public void run() {
                    try {
                        lock.lock();
                        c.await();
                        lock.unlock();
                    }
                    catch(InterruptedException e) {
                        threadUnexpectedException();
                    }
                }
            });

        try {
            t1.start();
            t2.start();
            Thread.sleep(SHORT_DELAY_MS);
            lock.lock();
            c.signalAll();
            lock.unlock();
            t1.join(SHORT_DELAY_MS);
            t2.join(SHORT_DELAY_MS);
            assertFalse(t1.isAlive());
            assertFalse(t2.isAlive());
        }
        catch (Exception ex) {
            unexpectedException();
        }
    }

    /**
     * toString indicates current lock state
     */
    @Test(timeout = LOCK_TEST_TIMEOUT)
    public void testToString() {
        final Lock lock = getNewLock();
        String us = lock.toString();
        assertTrue(us.indexOf("Unlocked") >= 0);
        lock.lock();
        String ls = lock.toString();
        assertTrue(ls.indexOf("Locked") >= 0);
    }
}
