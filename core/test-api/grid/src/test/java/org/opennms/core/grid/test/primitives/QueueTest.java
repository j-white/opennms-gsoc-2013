package org.opennms.core.grid.test.primitives;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
@SuppressWarnings({"rawtypes", "unchecked"})
public class QueueTest extends JSR166TestCase {
    public static final int QUEUE_TEST_TIMEOUT = 3000;

    private static BlockingQueue getNewQueue() {
        return gridProvider.getQueue("queue" + ROLLING_ID++);
    }

    /**
     * offer(null) throws NPE
     */
    @Test
    public void testOfferNull() {
        try {
            BlockingQueue q = getNewQueue();
            q.offer(null);
            shouldThrow();
        } catch (NullPointerException success) { }   
    }

    /**
     * add(null) throws NPE
     */
    @Test
    public void testAddNull() {
        try {
            BlockingQueue q = getNewQueue();
            q.add(null);
            shouldThrow();
        } catch (NullPointerException success) { }   
    }

    /**
     * addAll(null) throws NPE
     */
    @Test
    public void testAddAll1() {
        try {
            BlockingQueue q = getNewQueue();
            q.addAll(null);
            shouldThrow();
        }
        catch (NullPointerException success) {}
    }

    /**
     * addAll(this) throws IAE
     */
    @Test
    public void testAddAllSelf() {
        try {
            BlockingQueue q = getNewQueue();
            q.addAll(q);
            shouldThrow();
        }
        catch (IllegalArgumentException success) {}
    }

    /**
     * addAll of a collection with null elements throws NPE
     */
    @Test
    public void testAddAll2() {
        try {
            BlockingQueue q = getNewQueue();
            Integer[] ints = new Integer[1];
            q.addAll(Arrays.asList(ints));
            shouldThrow();
        }
        catch (NullPointerException success) {}
    }

    /**
     * put(null) throws NPE
     */
    @Test
    public void testPutNull() {
        try {
            BlockingQueue q = getNewQueue();
            q.put(null);
            shouldThrow();
        } 
        catch (NullPointerException success){
        }   
        catch (InterruptedException ie) {
            unexpectedException();
        }
     }

    /**
     * take blocks interruptibly when empty
     */
    @Test(timeout=QUEUE_TEST_TIMEOUT)
    public void testTakeFromEmpty() {
        final BlockingQueue q = getNewQueue();
        Thread t = new Thread(new Runnable() {
                public void run() {
                    try {
                        q.take();
                        threadShouldThrow();
                    } catch (InterruptedException success){ }                
                }
            });
        try {
            t.start();
            Thread.sleep(SHORT_DELAY_MS);
            t.interrupt();
            t.join();
        } catch (Exception e){
            unexpectedException();
        }
    }

    /**
     * take blocks interruptibly when empty
     */
    @Test(timeout=QUEUE_TEST_TIMEOUT)
    public void testFairTakeFromEmpty() {
        final BlockingQueue q = getNewQueue();
        Thread t = new Thread(new Runnable() {
                public void run() {
                    try {
                        q.take();
                        threadShouldThrow();
                    } catch (InterruptedException success){ }                
                }
            });
        try {
            t.start();
            Thread.sleep(SHORT_DELAY_MS);
            t.interrupt();
            t.join();
        } catch (Exception e){
            unexpectedException();
        }
    }

    /**
     * poll fails unless active taker
     */
    @Test
    public void testPoll() {
        BlockingQueue q = getNewQueue();
        assertNull(q.poll());
    }

    /**
     * timed pool with zero timeout times out if no active taker
     */
    @Test
    public void testTimedPoll0() {
        try {
            BlockingQueue q = getNewQueue();
            assertNull(q.poll(0, TimeUnit.MILLISECONDS));
        } catch (InterruptedException e){
            unexpectedException();
        }   
    }

    /**
     * timed pool with nonzero timeout times out if no active taker
     */
    @Test
    public void testTimedPoll() {
        try {
            BlockingQueue q = getNewQueue();
            assertNull(q.poll(SHORT_DELAY_MS, TimeUnit.MILLISECONDS));
        } catch (InterruptedException e){
            unexpectedException();
        }   
    }

    /**
     * Interrupted timed poll throws InterruptedException instead of
     * returning timeout status
     */
    @Test(timeout=QUEUE_TEST_TIMEOUT)
    public void testInterruptedTimedPoll() {
        Thread t = new Thread(new Runnable() {
                public void run() {
                    try {
                        BlockingQueue q = getNewQueue();
                        assertNull(q.poll(SHORT_DELAY_MS, TimeUnit.MILLISECONDS));
                    } catch (InterruptedException success){
                    }   
                }});
        t.start();
        try { 
           Thread.sleep(SHORT_DELAY_MS); 
           t.interrupt();
           t.join();
        }
        catch (InterruptedException ie) {
            unexpectedException();
        }
    }

    /**
     *  timed poll before a delayed offer fails; after offer succeeds;
     *  on interruption throws
     */
    @Test(timeout=QUEUE_TEST_TIMEOUT)
    public void testTimedPollWithOffer() {
        final BlockingQueue q = getNewQueue();
        Thread t = new Thread(new Runnable() {
                public void run() {
                    try {
                        threadAssertNull(q.poll(SHORT_DELAY_MS, TimeUnit.MILLISECONDS));
                        q.poll(LONG_DELAY_MS, TimeUnit.MILLISECONDS);
                        q.poll(LONG_DELAY_MS, TimeUnit.MILLISECONDS);
                        threadShouldThrow();
                    } catch (InterruptedException success) { }                
                }
            });
        try {
            t.start();
            Thread.sleep(SMALL_DELAY_MS);
            assertTrue(q.offer(zero, SHORT_DELAY_MS, TimeUnit.MILLISECONDS));
            t.interrupt();
            t.join();
        } catch (Exception e){
            unexpectedException();
        }
    }  

    /**
     * Interrupted timed poll throws InterruptedException instead of
     * returning timeout status
     */
    @Test(timeout=QUEUE_TEST_TIMEOUT)
    public void testFairInterruptedTimedPoll() {
        Thread t = new Thread(new Runnable() {
                public void run() {
                    try {
                        BlockingQueue q = getNewQueue();
                        assertNull(q.poll(SHORT_DELAY_MS, TimeUnit.MILLISECONDS));
                    } catch (InterruptedException success){
                    }   
                }});
        t.start();
        try { 
           Thread.sleep(SHORT_DELAY_MS); 
           t.interrupt();
           t.join();
        }
        catch (InterruptedException ie) {
            unexpectedException();
        }
    }

    /**
     *  timed poll before a delayed offer fails; after offer succeeds;
     *  on interruption throws
     */
    @Test(timeout=QUEUE_TEST_TIMEOUT)
    public void testFairTimedPollWithOffer() {
        final BlockingQueue q = getNewQueue();
        Thread t = new Thread(new Runnable() {
                public void run() {
                    try {
                        threadAssertNull(q.poll(SHORT_DELAY_MS, TimeUnit.MILLISECONDS));
                        q.poll(LONG_DELAY_MS, TimeUnit.MILLISECONDS);
                        q.poll(LONG_DELAY_MS, TimeUnit.MILLISECONDS);
                        threadShouldThrow();
                    } catch (InterruptedException success) { }                
                }
            });
        try {
            t.start();
            Thread.sleep(SMALL_DELAY_MS);
            assertTrue(q.offer(zero, SHORT_DELAY_MS, TimeUnit.MILLISECONDS));
            t.interrupt();
            t.join();
        } catch (Exception e){
            unexpectedException();
        }
    }  


    /**
     * peek returns null
     */
    @Test
    public void testPeek() {
        BlockingQueue q = getNewQueue();
        assertNull(q.peek());
    }

    /**
     * element throws NSEE
     */
    @Test
    public void testElement() {
        BlockingQueue q = getNewQueue();
        try {
            q.element();
            shouldThrow();
        }
        catch (NoSuchElementException success) {}
    }

    /**
     * remove throws NSEE if no active taker
     */
    @Test
    public void testRemove() {
        BlockingQueue q = getNewQueue();
        try {
            q.remove();
            shouldThrow();
        } catch (NoSuchElementException success){
        }   
    }

    /**
     * remove(x) returns false
     */
    @Test
    public void testRemoveElement() {
        BlockingQueue q = getNewQueue();
        assertFalse(q.remove(zero));
        assertTrue(q.isEmpty());
    }
        
    /**
     * contains returns false
     */
    @Test
    public void testContains() {
        BlockingQueue q = getNewQueue();
        assertFalse(q.contains(zero));
    }

    /**
     * clear ensures isEmpty
     */
    @Test
    public void testClear() {
        BlockingQueue q = getNewQueue();
        q.clear();
        assertTrue(q.isEmpty());
    }

    /**
     * containsAll returns false unless empty
     */
    @Test
    public void testContainsAll() {
        BlockingQueue q = getNewQueue();
        Integer[] empty = new Integer[0];
        assertTrue(q.containsAll(Arrays.asList(empty)));
        Integer[] ints = new Integer[1]; ints[0] = zero;
        assertFalse(q.containsAll(Arrays.asList(ints)));
    }

    /**
     * retainAll returns false
     */
    @Test
    public void testRetainAll() {
        BlockingQueue q = getNewQueue();
        Integer[] empty = new Integer[0];
        assertFalse(q.retainAll(Arrays.asList(empty)));
        Integer[] ints = new Integer[1]; ints[0] = zero;
        assertFalse(q.retainAll(Arrays.asList(ints)));
    }

    /**
     * removeAll returns false
     */
    @Test
    public void testRemoveAll() {
        BlockingQueue q = getNewQueue();
        Integer[] empty = new Integer[0];
        assertFalse(q.removeAll(Arrays.asList(empty)));
        Integer[] ints = new Integer[1]; ints[0] = zero;
        assertFalse(q.containsAll(Arrays.asList(ints)));
    }


    /**
     * toArray is empty
     */
    @Test
    public void testToArray() {
        BlockingQueue q = getNewQueue();
        Object[] o = q.toArray();
        assertEquals(o.length, 0);
    }

    /**
     * toArray(a) is nulled at position 0
     */
    @SuppressWarnings("unused")
    @Test
    public void testToArray2() {
        BlockingQueue q = getNewQueue();
        Integer[] ints = new Integer[1];
        assertNull(ints[0]);
    }
    
    /**
     * toArray(null) throws NPE
     */
    @SuppressWarnings("unused")
    @Test
    public void testToArray_BadArg() {
        try {
            BlockingQueue q = getNewQueue();
            Object o[] = q.toArray(null);
            shouldThrow();
        } catch(NullPointerException success){}
    }


    /**
     * iterator does not traverse any elements
     */
    @SuppressWarnings("unused")
    @Test
    public void testIterator() {
        BlockingQueue q = getNewQueue();
        Iterator it = q.iterator();
        assertFalse(it.hasNext());
        try {
            Object x = it.next();
            shouldThrow();
        }
        catch (NoSuchElementException success) {}
    }

    /**
     * iterator remove throws ISE
     */
    @Test
    public void testIteratorRemove() {
        BlockingQueue q = getNewQueue();
        Iterator it = q.iterator();
        try {
            it.remove();
            shouldThrow();
        }
        catch (IllegalStateException success) {}
    }

    /**
     * toString returns a non-null string
     */
    @Test
    public void testToString() {
        BlockingQueue q = getNewQueue();
        String s = q.toString();
        assertNotNull(s);
    }        

    /**
     * poll retrieves elements across Executor threads
     */
    @Test(timeout=QUEUE_TEST_TIMEOUT)
    public void testPollInExecutor() {
        final BlockingQueue q = getNewQueue();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        executor.execute(new Runnable() {
            public void run() {
                threadAssertNull(q.poll());
                try {
                    threadAssertTrue(null != q.poll(MEDIUM_DELAY_MS, TimeUnit.MILLISECONDS));
                    threadAssertTrue(q.isEmpty());
                }
                catch (InterruptedException e) {
                    threadUnexpectedException();
                }
            }
        });

        executor.execute(new Runnable() {
            public void run() {
                try {
                    Thread.sleep(SMALL_DELAY_MS);
                    q.put(new Integer(1));
                }
                catch (InterruptedException e) {
                    threadUnexpectedException();
                }
            }
        });
        
        joinPool(executor);
    }

    /**
     * drainTo(null) throws NPE
     */ 
    @Test
    public void testDrainToNull() {
        BlockingQueue q = getNewQueue();
        try {
            q.drainTo(null);
            shouldThrow();
        } catch(NullPointerException success) {
        }
    }

    /**
     * drainTo(this) throws IAE
     */
    @Test
    public void testDrainToSelf() {
        BlockingQueue q = getNewQueue();
        try {
            q.drainTo(q);
            shouldThrow();
        } catch(IllegalArgumentException success) {
        }
    }

    /**
     * drainTo(c) of empty queue doesn't transfer elements
     */
    @Test
    public void testDrainTo() {
        BlockingQueue q = getNewQueue();
        ArrayList l = new ArrayList();
        q.drainTo(l);
        assertEquals(q.size(), 0);
        assertEquals(l.size(), 0);
    }

    /**
     * drainTo empties queue, unblocking a waiting put.
     */
    @Test(timeout=QUEUE_TEST_TIMEOUT)
    public void testDrainToWithActivePut() {
        final BlockingQueue q = getNewQueue();
        Thread t = new Thread(new Runnable() {
                public void run() {
                    try {
                        q.put(new Integer(1));
                    } catch (InterruptedException ie){ 
                        threadUnexpectedException();
                    }
                }
            });
        try {
            t.start();
            ArrayList l = new ArrayList();
            Thread.sleep(SHORT_DELAY_MS);
            q.drainTo(l);
            assertTrue(l.size() <= 1);
            if (l.size() > 0)
                assertEquals(l.get(0), new Integer(1));
            t.join();
            assertTrue(l.size() <= 1);
        } catch(Exception e){
            unexpectedException();
        }
    }

    /**
     * drainTo(null, n) throws NPE
     */
    @Test
    public void testDrainToNullN() {
        BlockingQueue q = getNewQueue();
        try {
            q.drainTo(null, 0);
            shouldThrow();
        } catch(NullPointerException success) {
        }
    }

    /**
     * drainTo(this, n) throws IAE
     */
    @Test
    public void testDrainToSelfN() {
        BlockingQueue q = getNewQueue();
        try {
            q.drainTo(q, 0);
            shouldThrow();
        } catch(IllegalArgumentException success) {
        }
    }

    /**
     * drainTo(c, n) empties up to n elements of queue into c
     */
    @Test(timeout=QUEUE_TEST_TIMEOUT)
    public void testDrainToN() {
        final BlockingQueue q = getNewQueue();
        Thread t1 = new Thread(new Runnable() {
                public void run() {
                    try {
                        q.put(one);
                    } catch (InterruptedException ie){ 
                        threadUnexpectedException();
                    }
                }
            });
        Thread t2 = new Thread(new Runnable() {
                public void run() {
                    try {
                        q.put(two);
                    } catch (InterruptedException ie){ 
                        threadUnexpectedException();
                    }
                }
            });

        try {
            t1.start();
            t2.start();
            ArrayList l = new ArrayList();
            Thread.sleep(SHORT_DELAY_MS);
            q.drainTo(l, 1);
            assertTrue(l.size() == 1);
            q.drainTo(l, 1);
            assertTrue(l.size() == 2);
            assertTrue(l.contains(one));
            assertTrue(l.contains(two));
            t1.join();
            t2.join();
        } catch(Exception e){
            unexpectedException();
        }
    }
}
