package org.opennms.core.grid.test.primitives;

import java.util.concurrent.*;
import java.security.*;

import org.opennms.core.grid.GridCallable;
import org.opennms.core.grid.GridRunnable;
import org.opennms.core.test.grid.GridTest;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 * Other contributors include Andrew Wright, Jeffrey Hayes, 
 * Pat Fisher, Mike Judd.
 * 
 * Adapted by Jesse White to work with the primitives provided
 * via the grid provider interface.
 * 
 * Pulled from http://svn.apache.org/repos/asf/harmony/standard/classlib/trunk/modules/concurrent/src/test/java/
 */

/**
 * Base class for JSR166 Junit TCK tests.  Defines some constants,
 * utility methods and classes, as well as a simple framework for
 * helping to make sure that assertions failing in generated threads
 * cause the associated test that generated them to itself fail (which
 * JUnit doe not otherwise arrange).  The rules for creating such
 * tests are:
 *
 * <ol>
 *
 * <li> All assertions in code running in generated threads must use
 * the forms {@link #threadFail} , {@link #threadAssertTrue} {@link
 * #threadAssertEquals}, or {@link #threadAssertNull}, (not
 * <tt>fail</tt>, <tt>assertTrue</tt>, etc.) It is OK (but not
 * particularly recommended) for other code to use these forms too.
 * Only the most typically used JUnit assertion methods are defined
 * this way, but enough to live with.</li>
 *
 * <li> If you override {@link #setUp} or {@link #tearDown}, make sure
 * to invoke <tt>super.setUp</tt> and <tt>super.tearDown</tt> within
 * them. These methods are used to clear and check for thread
 * assertion failures.</li>
 *
 * <li>All delays and timeouts must use one of the constants <tt>
 * SHORT_DELAY_MS</tt>, <tt> SMALL_DELAY_MS</tt>, <tt> MEDIUM_DELAY_MS</tt>,
 * <tt> LONG_DELAY_MS</tt>. The idea here is that a SHORT is always
 * discriminable from zero time, and always allows enough time for the
 * small amounts of computation (creating a thread, calling a few
 * methods, etc) needed to reach a timeout point. Similarly, a SMALL
 * is always discriminable as larger than SHORT and smaller than
 * MEDIUM.  And so on. These constants are set to conservative values,
 * but even so, if there is ever any doubt, they can all be increased
 * in one spot to rerun tests on slower platforms</li>
 *
 * <li> All threads generated must be joined inside each test case
 * method (or <tt>fail</tt> to do so) before returning from the
 * method. The <tt> joinPool</tt> method can be used to do this when
 * using Executors.</li>
 *
 * </ol>
 *
 * <p> <b>Other notes</b>
 * <ul>
 *
 * <li> Usually, there is one testcase method per JSR166 method
 * covering "normal" operation, and then as many exception-testing
 * methods as there are exceptions the method can throw. Sometimes
 * there are multiple tests per JSR166 method when the different
 * "normal" behaviors differ significantly. And sometimes testcases
 * cover multiple methods when they cannot be tested in
 * isolation.</li>
 * 
 * <li> The documentation style for testcases is to provide as javadoc
 * a simple sentence or two describing the property that the testcase
 * method purports to test. The javadocs do not say anything about how
 * the property is tested. To find out, read the code.</li>
 *
 * <li> These tests are "conformance tests", and do not attempt to
 * test throughput, latency, scalability or other performance factors
 * (see the separate "jtreg" tests for a set intended to check these
 * for the most central aspects of functionality.) So, most tests use
 * the smallest sensible numbers of threads, collection sizes, etc
 * needed to check basic conformance.</li>
 *
 * <li>The test classes currently do not declare inclusion in
 * any particular package to simplify things for people integrating
 * them in TCK test suites.</li>
 *
 * <li> As a convenience, the <tt>main</tt> of this class (JSR166TestCase)
 * runs all JSR166 unit tests.</li>
 *
 * </ul>
 */
public abstract class JSR166TestCase extends GridTest {
    public static long SHORT_DELAY_MS;
    public static long SMALL_DELAY_MS;
    public static long MEDIUM_DELAY_MS;
    public static long LONG_DELAY_MS;

    public static long ROLLING_ID = 0;

    /**
     * Return the shortest timed delay. This could
     * be reimplemented to use for example a Property.
     */ 
    protected long getShortDelay() {
        return 50;
    }


    /**
     * Set delays as multiples of SHORT_DELAY.
     */
    protected  void setDelays() {
        SHORT_DELAY_MS = getShortDelay();
        SMALL_DELAY_MS = SHORT_DELAY_MS * 5;
        MEDIUM_DELAY_MS = SHORT_DELAY_MS * 10;
        LONG_DELAY_MS = SHORT_DELAY_MS * 50;
    }

    /**
     * Flag set true if any threadAssert methods fail
     */
    protected static volatile boolean threadFailed;

    protected static volatile boolean threadAssertFailed;
    protected static volatile String threadAssertMessage;

    /**
     * Initialize test to indicate that no thread assertions have failed
     */
    public void setUp() {
        setDelays();
        threadFailed = false;
        threadAssertFailed = false;
        threadAssertMessage = "";
    }

    /**
     * Trigger test case failure if any thread assertions have failed
     */
    public void tearDown() throws Exception { 
        assertFalse(threadFailed);  
        assertFalse(threadAssertMessage, threadAssertFailed);
    }

    /**
     * Fail, also setting status to indicate current testcase should fail
     */ 
    public static void threadFail(String reason) {
        threadFailed = true;
        threadAssertFailed = true;
        threadAssertMessage = reason;
    }

    /**
     * If expression not true, set status to indicate current testcase
     * should fail
     */ 
    public static void threadAssertTrue(boolean b) {
        if (!b) {
            threadFailed = true;
            threadAssertFailed = true;
            threadAssertMessage = b + " != true";
        }
    }

    /**
     * If expression not false, set status to indicate current testcase
     * should fail
     */ 
    public static void threadAssertFalse(boolean b) {
        if (b) {
            threadFailed = true;
            threadAssertFailed = true;
            threadAssertMessage = b + " != false";
        }
    }

    /**
     * If argument not null, set status to indicate current testcase
     * should fail
     */ 
    public static void threadAssertNull(Object x) {
        if (x != null) {
            threadFailed = true;
            threadAssertFailed = true;
            threadAssertMessage = x + " != null";
        }
    }

    /**
     * If arguments not equal, set status to indicate current testcase
     * should fail
     */ 
    public static void threadAssertEquals(long x, long y) {
        if (x != y) {
            threadFailed = true;
            threadAssertFailed = true;
            threadAssertMessage = x + " != " + y;
        }
    }

    /**
     * If arguments not equal, set status to indicate current testcase
     * should fail
     */ 
    public static void threadAssertEquals(Object x, Object y) {
        if (x != y && (x == null || !x.equals(y))) {
            threadFailed = true;
            threadAssertFailed = true;
            threadAssertMessage = x + " != " + y;
        }
    }

    /**
     * threadFail with message "should throw exception"
     */ 
    public static void threadShouldThrow() {
        threadFailed = true;
        threadAssertFailed = true;
        threadAssertMessage = "should throw exception";
    }

    /**
     * threadFail with message "Unexpected exception"
     */
    public static void threadUnexpectedException() {
        threadFailed = true;
        threadAssertFailed = true;
        threadAssertMessage = "Unexpected exception";
    }

    /**
     * Wait out termination of a thread pool or fail doing so
     */
    public void joinPool(ExecutorService exec) {
        try {
            exec.shutdown();
            assertTrue(exec.awaitTermination(LONG_DELAY_MS, TimeUnit.MILLISECONDS));
        } catch(SecurityException ok) {
            // Allowed in case test doesn't have privs
        } catch(InterruptedException ie) {
            fail("Unexpected exception");
        }
    }


    /**
     * fail with message "should throw exception"
     */ 
    public void shouldThrow() {
        fail("Should throw exception");
    }

    /**
     * fail with message "Unexpected exception"
     */
    public void unexpectedException() {
        fail("Unexpected exception");
    }


    /**
     * The number of elements to place in collections, arrays, etc.
     */
    static final int SIZE = 20;

    // Some convenient Integer constants

    static final Integer zero = new Integer(0);
    static final Integer one = new Integer(1);
    static final Integer two = new Integer(2);
    static final Integer three  = new Integer(3);
    static final Integer four  = new Integer(4);
    static final Integer five  = new Integer(5);
    static final Integer six = new Integer(6);
    static final Integer seven = new Integer(7);
    static final Integer eight = new Integer(8);
    static final Integer nine = new Integer(9);
    static final Integer m1  = new Integer(-1);
    static final Integer m2  = new Integer(-2);
    static final Integer m3  = new Integer(-3);
    static final Integer m4 = new Integer(-4);
    static final Integer m5 = new Integer(-5);
    static final Integer m10 = new Integer(-10);


    /**
     * A security policy where new permissions can be dynamically added
     * or all cleared.
     */
    static class AdjustablePolicy extends java.security.Policy {
        Permissions perms = new Permissions();
        AdjustablePolicy() { }
        void addPermission(Permission perm) { perms.add(perm); }
        void clearPermissions() { perms = new Permissions(); }
        public PermissionCollection getPermissions(CodeSource cs) {
            return perms;
        }
        public PermissionCollection getPermissions(ProtectionDomain pd) {
            return perms;
        }
        public boolean implies(ProtectionDomain pd, Permission p) {
            return perms.implies(p);
        }
        public void refresh() {}
    }


    // Some convenient Runnable classes

    static class NoOpRunnable implements GridRunnable {
        private static final long serialVersionUID = 5743899303046716550L;
        public void run() {}
    }

    @SuppressWarnings("rawtypes")
    static class NoOpCallable implements GridCallable {
        private static final long serialVersionUID = -3638841022019015734L;
        public Object call() { return Boolean.TRUE; }
    }

    static final String TEST_STRING = "a test string";

    static class StringTask implements GridCallable<String> {
        private static final long serialVersionUID = -515765932991023876L;
        public String call() { return TEST_STRING; }
    }

    static class NPETask implements GridCallable<String> {
        private static final long serialVersionUID = -1418942768756937437L;
        public String call() { throw new NullPointerException(); }
    }

    static class CallableOne implements GridCallable<Integer> {
        private static final long serialVersionUID = 1L;
        public Integer call() { return one; }
    }

    static class ShortRunnable implements GridRunnable {
        private static final long serialVersionUID = 7172506603673778067L;
        public void run() {
            try {
                Thread.sleep(SHORT_DELAY_MS);
            }
            catch(Exception e) {
                threadUnexpectedException();
            }
        }
    }

    static class ShortInterruptedRunnable implements GridRunnable {
        private static final long serialVersionUID = -2714773584275397220L;
        public void run() {
            try {
                Thread.sleep(SHORT_DELAY_MS);
                threadShouldThrow();
            }
            catch(InterruptedException success) {
            }
        }
    }

    static class SmallRunnable implements GridRunnable {
        private static final long serialVersionUID = -4639649733055566312L;
        public void run() {
            try {
                Thread.sleep(SMALL_DELAY_MS);
            }
            catch(Exception e) {
                threadUnexpectedException();
            }
        }
    }

    static class SmallPossiblyInterruptedRunnable implements GridRunnable {
        private static final long serialVersionUID = -7383112194894224586L;
        public void run() {
            try {
                Thread.sleep(SMALL_DELAY_MS);
            }
            catch(Exception e) {
            }
        }
    }

    @SuppressWarnings("rawtypes")
    static class SmallCallable implements GridCallable {
        private static final long serialVersionUID = 7253652535702399376L;
        public Object call() {
            try {
                Thread.sleep(SMALL_DELAY_MS);
            }
            catch(Exception e) {
                threadUnexpectedException();
            }
            return Boolean.TRUE;
        }
    }

    static class SmallInterruptedRunnable implements GridRunnable {
        private static final long serialVersionUID = 530009740419803354L;
        public void run() {
            try {
                Thread.sleep(SMALL_DELAY_MS);
                threadShouldThrow();
            }
            catch(InterruptedException success) {
            }
        }
    }


    static class MediumRunnable implements GridRunnable {
        private static final long serialVersionUID = 8139884053053785888L;
        public void run() {
            try {
                Thread.sleep(MEDIUM_DELAY_MS);
            }
            catch(Exception e) {
                threadUnexpectedException();
            }
        }
    }

    static class MediumInterruptedRunnable implements GridRunnable {
        private static final long serialVersionUID = 2099081509094165197L;
        public void run() {
            try {
                Thread.sleep(MEDIUM_DELAY_MS);
                threadShouldThrow();
            }
            catch(InterruptedException success) {
            }
        }
    }

    static class MediumPossiblyInterruptedRunnable implements GridRunnable {
        private static final long serialVersionUID = 7742072252958787969L;
        public void run() {
            try {
                Thread.sleep(MEDIUM_DELAY_MS);
            }
            catch(InterruptedException success) {
            }
        }
    }

    static class LongPossiblyInterruptedRunnable implements GridRunnable {
        private static final long serialVersionUID = 5077787455004433437L;
        public void run() {
            try {
                Thread.sleep(LONG_DELAY_MS);
            }
            catch(InterruptedException success) {
            }
        }
    }

    /**
     * For use as ThreadFactory in constructors
     */
    static class SimpleThreadFactory implements ThreadFactory{
        public Thread newThread(Runnable r){
            return new Thread(r);
        }   
    }

    static class TrackedShortRunnable implements GridRunnable {
        private static final long serialVersionUID = 1592879543894487091L;
        volatile boolean done = false;
        public void run() {
            try {
                Thread.sleep(SMALL_DELAY_MS);
                done = true;
            } catch(Exception e){
            }
        }
    }

    static class TrackedMediumRunnable implements GridRunnable {
        private static final long serialVersionUID = -4074775364242817525L;
        volatile boolean done = false;
        public void run() {
            try {
                Thread.sleep(MEDIUM_DELAY_MS);
                done = true;
            } catch(Exception e){
            }
        }
    }

    static class TrackedLongRunnable implements GridRunnable {
        private static final long serialVersionUID = -3154205545926262628L;
        volatile boolean done = false;
        public void run() {
            try {
                Thread.sleep(LONG_DELAY_MS);
                done = true;
            } catch(Exception e){
            }
        }
    }

    static class TrackedNoOpRunnable implements GridRunnable {
        private static final long serialVersionUID = 6075153451738084254L;
        volatile boolean done = false;
        public void run() {
            done = true;
        }
    }

    @SuppressWarnings("rawtypes")
    static class TrackedCallable implements GridCallable {
        private static final long serialVersionUID = -1321598403185843259L;
        volatile boolean done = false;
        public Object call() {
            try {
                Thread.sleep(SMALL_DELAY_MS);
                done = true;
            } catch(Exception e){
            }
            return Boolean.TRUE;
        }
    }

    /**
     * For use as RejectedExecutionHandler in constructors
     */
    static class NoOpREHandler implements RejectedExecutionHandler{
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor){} 
    }
}
