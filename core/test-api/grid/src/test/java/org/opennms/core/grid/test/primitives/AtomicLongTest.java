package org.opennms.core.grid.test.primitives;

import org.junit.Test;
import org.opennms.core.grid.AtomicLong;

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
public class AtomicLongTest extends JSR166TestCase {
    /**
     * creates a new atomic long using the grid provider
     * and sets the initial value to 1
     */
    private AtomicLong getNewAtomicLong() {
        AtomicLong al = gridProvider.getAtomicLong("atomic" + ROLLING_ID++);
        al.set(1);
        return al;
    }

    /**
     * get returns the last value set
     */
    @Test
    public void testGetSet(){
        AtomicLong ai = getNewAtomicLong();
        ai.set(1);
	assertEquals(1,ai.get());
	ai.set(2);
	assertEquals(2,ai.get());
	ai.set(-3);
	assertEquals(-3,ai.get());
	
    }
    /**
     * compareAndSet succeeds in changing value if equal to expected else fails
     */
    @Test
    public void testCompareAndSet(){
        AtomicLong ai = getNewAtomicLong();
        ai.set(1);
	assertTrue(ai.compareAndSet(1,2));
	assertTrue(ai.compareAndSet(2,-4));
	assertEquals(-4,ai.get());
	assertFalse(ai.compareAndSet(-5,7));
	assertFalse((7 == ai.get()));
	assertTrue(ai.compareAndSet(-4,7));
	assertEquals(7,ai.get());
    }

    /**
     * compareAndSet in one thread enables another waiting for value
     * to succeed
     */
    @Test
    public void testCompareAndSetInMultipleThreads() {
        final AtomicLong ai = getNewAtomicLong();
        Thread t = new Thread(new Runnable() {
                public void run() {
                    while(!ai.compareAndSet(2, 3)) Thread.yield();
                }});
        try {
            t.start();
            assertTrue(ai.compareAndSet(1, 2));
            t.join(LONG_DELAY_MS);
            assertFalse(t.isAlive());
            assertEquals(ai.get(), 3);
        }
        catch(Exception e) {
            unexpectedException();
        }
    }

    /**
     * getAndSet returns previous value and sets to given value
     */
    @Test
    public void testGetAndSet(){
        AtomicLong ai = getNewAtomicLong();
	assertEquals(1,ai.getAndSet(0));
	assertEquals(0,ai.getAndSet(-10));
	assertEquals(-10,ai.getAndSet(1));
    }

    /**
     * getAndAdd returns previous value and adds given value
     */
    @Test
    public void testGetAndAdd(){
        AtomicLong ai = getNewAtomicLong();
	assertEquals(1,ai.getAndAdd(2));
	assertEquals(3,ai.get());
	assertEquals(3,ai.getAndAdd(-4));
	assertEquals(-1,ai.get());
    }

    /**
     * getAndDecrement returns previous value and decrements
     */
    @Test
    public void testGetAndDecrement(){
        AtomicLong ai = getNewAtomicLong();
	assertEquals(1,ai.getAndDecrement());
	assertEquals(0,ai.getAndDecrement());
	assertEquals(-1,ai.getAndDecrement());
    }

    /**
     * getAndIncrement returns previous value and increments
     */
    @Test
    public void testGetAndIncrement(){
        AtomicLong ai = getNewAtomicLong();
	assertEquals(1,ai.getAndIncrement());
	assertEquals(2,ai.get());
	ai.set(-2);
	assertEquals(-2,ai.getAndIncrement());
	assertEquals(-1,ai.getAndIncrement());
	assertEquals(0,ai.getAndIncrement());
	assertEquals(1,ai.get());
    }

    /**
     * addAndGet adds given value to current, and returns current value
     */
    @Test
    public void testAddAndGet(){
        AtomicLong ai = getNewAtomicLong();
	assertEquals(3,ai.addAndGet(2));
	assertEquals(3,ai.get());
	assertEquals(-1,ai.addAndGet(-4));
	assertEquals(-1,ai.get());
    }

    /**
     * decrementAndGet decrements and returns current value
     */
    @Test
    public void testDecrementAndGet(){
        AtomicLong ai = getNewAtomicLong();
	assertEquals(0,ai.decrementAndGet());
	assertEquals(-1,ai.decrementAndGet());
	assertEquals(-2,ai.decrementAndGet());
	assertEquals(-2,ai.get());
    }

    /**
     * incrementAndGet increments and returns current value
     */
    @Test
    public void testIncrementAndGet(){
        AtomicLong ai = getNewAtomicLong();
	assertEquals(2,ai.incrementAndGet());
	assertEquals(2,ai.get());
	ai.set(-2);
	assertEquals(-1,ai.incrementAndGet());
	assertEquals(0,ai.incrementAndGet());
	assertEquals(1,ai.incrementAndGet());
	assertEquals(1,ai.get());
    }

    /**
     * toString returns current value.
     */
    @Test
    public void testToString() {
        AtomicLong ai = getNewAtomicLong();
        for (long i = -12; i < 6; ++i) {
            ai.set(i);
            assertEquals(Long.toString(i), ai.toString());
        }
    }
}