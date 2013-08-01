package org.opennms.core.grid.test.primitives;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.opennms.core.grid.GridRunnable;
import org.opennms.core.grid.concurrent.DistributedThreadPoolExecutor;

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
public class ThreadPoolExecutorTest extends JSR166TestCase {
    private static BlockingQueue<Runnable> getNewQueue() {
        return gridProvider.getQueue("queue" + ROLLING_ID++);
    }
    
    private static String getNewExecutorName() {
        return "executor" + ROLLING_ID++;
    }
    
    static class ExtendedTPE extends DistributedThreadPoolExecutor {
        volatile boolean beforeCalled = false;
        volatile boolean afterCalled = false;
        volatile boolean terminatedCalled = false;
        public ExtendedTPE() {
            super(1, 1, LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());;
        }
        protected void beforeExecute(Thread t, Runnable r) {
            beforeCalled = true;
        }
        protected void afterExecute(Runnable r, Throwable t) {
            afterCalled = true;
        }
        protected void terminated() {
            terminatedCalled = true;
        }
    }

    private static class MyGridRunnable implements GridRunnable {
        private static final long serialVersionUID = -94127140582583058L;
        public void run() {
            try {
                Thread.sleep(SHORT_DELAY_MS);
            } catch(InterruptedException e){
                threadUnexpectedException();
            }
        }
    }

    /**
     *  execute successfully executes a runnable
     */
    @Test
    public void testExecute() {
        DistributedThreadPoolExecutor p1 = new DistributedThreadPoolExecutor(1, 1, LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());;
        try {
            p1.execute(new MyGridRunnable());
            Thread.sleep(SMALL_DELAY_MS);
        } catch(InterruptedException e){
            unexpectedException();
        } 
        joinPool(p1);
    }

    /**
     *   getCompletedTaskCount increases, but doesn't overestimate,
     *   when tasks complete
     */
    @Test
    public void testGetCompletedTaskCount() {
        DistributedThreadPoolExecutor p2 = new DistributedThreadPoolExecutor(2, 2, LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());;
        assertEquals(0, p2.getCompletedTaskCount());
        p2.execute(new ShortRunnable());
        try {
            Thread.sleep(SMALL_DELAY_MS);
        } catch(Exception e){
            unexpectedException();
        }
        assertEquals(1, p2.getCompletedTaskCount());
        try { p2.shutdown(); } catch(SecurityException ok) { return; }
        joinPool(p2);
    }
    
    /**
     *   getCorePoolSize returns size given in constructor if not otherwise set
     */
    @Test
    public void testGetCorePoolSize() {
        DistributedThreadPoolExecutor p1 = new DistributedThreadPoolExecutor(1, 1, LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());;
        assertEquals(1, p1.getCorePoolSize());
        joinPool(p1);
    }

    /** 
     * setThreadFactory sets the thread factory returned by getThreadFactory
     */
    @Test
    public void testSetThreadFactory() {
        DistributedThreadPoolExecutor p = new DistributedThreadPoolExecutor(1,2,LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());;
        ThreadFactory tf = new SimpleThreadFactory();
        p.setThreadFactory(tf);
        assertSame(tf, p.getThreadFactory());
        joinPool(p);
    }

    /** 
     * setThreadFactory(null) throws NPE
     */
    @Test
    public void testSetThreadFactoryNull() {
        DistributedThreadPoolExecutor p = new DistributedThreadPoolExecutor(1,2,LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());
        try {
            p.setThreadFactory(null);
            shouldThrow();
        } catch (NullPointerException success) {
        } finally {
            joinPool(p);
        }
    }

    /** 
     * setRejectedExecutionHandler sets the handler returned by
     * getRejectedExecutionHandler
     */
    @Test
    public void testSetRejectedExecutionHandler() {
        DistributedThreadPoolExecutor p = new DistributedThreadPoolExecutor(1,2,LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());
        RejectedExecutionHandler h = new NoOpREHandler();
        p.setRejectedExecutionHandler(h);
        assertSame(h, p.getRejectedExecutionHandler());
        joinPool(p);
    }


    /** 
     * setRejectedExecutionHandler(null) throws NPE
     */
    @Test
    public void testSetRejectedExecutionHandlerNull() {
        DistributedThreadPoolExecutor p = new DistributedThreadPoolExecutor(1,2,LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());
        try {
            p.setRejectedExecutionHandler(null);
            shouldThrow();
        } catch (NullPointerException success) {
        } finally {
            joinPool(p);
        }
    }

    
    /**
     *   getMaximumPoolSize returns value given in constructor if not
     *   otherwise set
     */
    @Test
    public void testGetMaximumPoolSize() {
        DistributedThreadPoolExecutor p2 = new DistributedThreadPoolExecutor(2, 2, LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());
        assertEquals(2, p2.getMaximumPoolSize());
        joinPool(p2);
    }

    /**
     *   isShutDown is false before shutdown, true after
     */
    @Test
    public void testIsShutdown() {
        
        DistributedThreadPoolExecutor p1 = new DistributedThreadPoolExecutor(1, 1, LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());
        assertFalse(p1.isShutdown());
        try { p1.shutdown(); } catch(SecurityException ok) { return; }
        assertTrue(p1.isShutdown());
        joinPool(p1);
    }

        
    /**
     *  isTerminated is false before termination, true after
     */
    @Test
    public void testIsTerminated() {
        DistributedThreadPoolExecutor p1 = new DistributedThreadPoolExecutor(1, 1, LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());
        assertFalse(p1.isTerminated());
        try {
            p1.execute(new MediumRunnable());
        } finally {
            try { p1.shutdown(); } catch(SecurityException ok) { return; }
        }
        try {
            assertTrue(p1.awaitTermination(LONG_DELAY_MS, TimeUnit.MILLISECONDS));
            assertTrue(p1.isTerminated());
        } catch(Exception e){
            unexpectedException();
        }       
    }

    /**
     *  isTerminating is not true when running or when terminated
     */
    @Test
    public void testIsTerminating() {
        DistributedThreadPoolExecutor p1 = new DistributedThreadPoolExecutor(1, 1, LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());
        assertFalse(p1.isTerminating());
        try {
            p1.execute(new SmallRunnable());
            assertFalse(p1.isTerminating());
        } finally {
            try { p1.shutdown(); } catch(SecurityException ok) { return; }
        }
        try {
            assertTrue(p1.awaitTermination(LONG_DELAY_MS, TimeUnit.MILLISECONDS));
            assertTrue(p1.isTerminated());
            assertFalse(p1.isTerminating());
        } catch(Exception e){
            unexpectedException();
        }       
    }

    private static class GridFutureTask<T> extends FutureTask<T> implements Serializable {
        private static final long serialVersionUID = -4847144330561094332L;

        public GridFutureTask(Callable<T> callable) {
            super(callable);
        }

        public GridFutureTask(Runnable runnable, T result) {
            super(runnable, result);
        }
    }

    /**
     *   purge removes cancelled tasks from the queue
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testPurge() {
        DistributedThreadPoolExecutor p1 = new DistributedThreadPoolExecutor(1, 1, LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());
        FutureTask[] tasks = new FutureTask[5];
        for(int i = 0; i < 5; i++){
            tasks[i] = new GridFutureTask(new MediumPossiblyInterruptedRunnable(), Boolean.TRUE);
            p1.execute(tasks[i]);
        }
        tasks[4].cancel(true);
        tasks[3].cancel(true);
        p1.purge();
        long count = p1.getTaskCount();
        assertTrue(count >= 2 && count < 5);
        joinPool(p1);
    }

    /** 
     * Constructor throws if workQueue is set to null 
     */
    @Test
    public void testConstructorNullPointerException2() {
        try {
            new DistributedThreadPoolExecutor(1,2,LONG_DELAY_MS, TimeUnit.MILLISECONDS,null, gridProvider, getNewExecutorName());
            shouldThrow();
        }
        catch (NullPointerException success){}  
    }

    /**
     *  execute (null) throws NPE
     */
    @Test
    public void testExecuteNull() {
        DistributedThreadPoolExecutor tpe = null;
        try {
            tpe = new DistributedThreadPoolExecutor(1,2,LONG_DELAY_MS, TimeUnit.MILLISECONDS,getNewQueue(), gridProvider, getNewExecutorName());
            tpe.execute(null);
            shouldThrow();
        } catch(NullPointerException success){}
        
        joinPool(tpe);
    }
    
    /**
     *  setCorePoolSize of negative value throws IllegalArgumentException
     */
    @Test
    public void testCorePoolSizeIllegalArgumentException() {
        DistributedThreadPoolExecutor tpe = null;
        try {
            tpe = new DistributedThreadPoolExecutor(1,2,LONG_DELAY_MS, TimeUnit.MILLISECONDS,getNewQueue(), gridProvider, getNewExecutorName());
        } catch(Exception e){}
        try {
            tpe.setCorePoolSize(-1);
            shouldThrow();
        } catch(IllegalArgumentException success){
        } finally {
            try { tpe.shutdown(); } catch(SecurityException ok) { return; }
        }
        joinPool(tpe);
    }   

    /**
     *  setMaximumPoolSize(int) throws IllegalArgumentException if
     *  given a value less the core pool size
     */
    @Test
    public void testMaximumPoolSizeIllegalArgumentException() {
        DistributedThreadPoolExecutor tpe = null;
        try {
            tpe = new DistributedThreadPoolExecutor(2,3,LONG_DELAY_MS, TimeUnit.MILLISECONDS,getNewQueue(), gridProvider, getNewExecutorName());
        } catch(Exception e){}
        try {
            tpe.setMaximumPoolSize(1);
            shouldThrow();
        } catch(IllegalArgumentException success){
        } finally {
            try { tpe.shutdown(); } catch(SecurityException ok) { return; }
        }
        joinPool(tpe);
    }
    
    /**
     *  setMaximumPoolSize throws IllegalArgumentException
     *  if given a negative value
     */
    @Test
    public void testMaximumPoolSizeIllegalArgumentException2() {
        DistributedThreadPoolExecutor tpe = null;
        try {
            tpe = new DistributedThreadPoolExecutor(2,3,LONG_DELAY_MS, TimeUnit.MILLISECONDS,getNewQueue(), gridProvider, getNewExecutorName());
        } catch(Exception e){}
        try {
            tpe.setMaximumPoolSize(-1);
            shouldThrow();
        } catch(IllegalArgumentException success){
        } finally {
            try { tpe.shutdown(); } catch(SecurityException ok) { return; }
        }
        joinPool(tpe);
    }
    

    /**
     *  setKeepAliveTime  throws IllegalArgumentException
     *  when given a negative value
     */
    @Test
    public void testKeepAliveTimeIllegalArgumentException() {
        DistributedThreadPoolExecutor tpe = null;
        try {
            tpe = new DistributedThreadPoolExecutor(2,3,LONG_DELAY_MS, TimeUnit.MILLISECONDS,getNewQueue(), gridProvider, getNewExecutorName());
        } catch(Exception e){}
        
        try {
            tpe.setKeepAliveTime(-1,TimeUnit.MILLISECONDS);
            shouldThrow();
        } catch(IllegalArgumentException success){
        } finally {
            try { tpe.shutdown(); } catch(SecurityException ok) { return; }
        }
        joinPool(tpe);
    }

    /**
     * beforeExecute and afterExecute are called when executing task
     */
    @Test
    public void testBeforeAfter() {
        ExtendedTPE tpe = new ExtendedTPE();
        try {
            TrackedNoOpRunnable r = new TrackedNoOpRunnable();
            tpe.execute(r);
            Thread.sleep(SHORT_DELAY_MS);
            assertTrue(r.done);
            assertTrue(tpe.beforeCalled);
            assertTrue(tpe.afterCalled);
            try { tpe.shutdown(); } catch(SecurityException ok) { return; }
        }
        catch(Exception ex) {
            unexpectedException();
        } finally {
            joinPool(tpe);
        }
    }

    /**
     * completed submit of callable returns result
     */
    @Test
    public void testSubmitCallable() {
        ExecutorService e = new DistributedThreadPoolExecutor(2, 2, LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());
        try {
            Future<String> future = e.submit(new StringTask());
            String result = future.get();
            assertSame(TEST_STRING, result);
        }
        catch (ExecutionException ex) {
            unexpectedException();
        }
        catch (InterruptedException ex) {
            unexpectedException();
        } finally {
            joinPool(e);
        }
    }

    /**
     * completed submit of runnable returns successfully
     */
    @Test
    public void testSubmitRunnable() {
        ExecutorService e = new DistributedThreadPoolExecutor(2, 2, LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());
        try {
            Future<?> future = e.submit(new NoOpRunnable());
            future.get();
            assertTrue(future.isDone());
        }
        catch (ExecutionException ex) {
            unexpectedException();
        }
        catch (InterruptedException ex) {
            unexpectedException();
        } finally {
            joinPool(e);
        }
    }

    /**
     * completed submit of (runnable, result) returns result
     */
    @Test
    public void testSubmitRunnable2() {
        ExecutorService e = new DistributedThreadPoolExecutor(2, 2, LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());
        try {
            Future<String> future = e.submit(new NoOpRunnable(), TEST_STRING);
            String result = future.get();
            assertSame(TEST_STRING, result);
        }
        catch (ExecutionException ex) {
            unexpectedException();
        }
        catch (InterruptedException ex) {
            unexpectedException();
        } finally {
            joinPool(e);
        }
    }


    /**
     * invokeAny(null) throws NPE
     */
    @Test
    public void testInvokeAny1() {
        ExecutorService e = new DistributedThreadPoolExecutor(2, 2, LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());
        try {
            e.invokeAny(null);
        } catch (NullPointerException success) {
        } catch(Exception ex) {
            unexpectedException();
        } finally {
            joinPool(e);
        }
    }

    /**
     * invokeAny(empty collection) throws IAE
     */
    @Test
    public void testInvokeAny2() {
        ExecutorService e = new DistributedThreadPoolExecutor(2, 2, LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());
        try {
            e.invokeAny(new ArrayList<Callable<String>>());
        } catch (IllegalArgumentException success) {
        } catch(Exception ex) {
            unexpectedException();
        } finally {
            joinPool(e);
        }
    }

    /**
     * invokeAny(c) throws NPE if c has null elements
     */
    @Test
    public void testInvokeAny3() {
        ExecutorService e = new DistributedThreadPoolExecutor(2, 2, LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());
        try {
            ArrayList<Callable<String>> l = new ArrayList<Callable<String>>();
            l.add(new StringTask());
            l.add(null);
            e.invokeAny(l);
        } catch (NullPointerException success) {
        } catch(Exception ex) {
            unexpectedException();
        } finally {
            joinPool(e);
        }
    }

    /**
     * invokeAny(c) throws ExecutionException if no task completes
     */
    @Test
    public void testInvokeAny4() {
        ExecutorService e = new DistributedThreadPoolExecutor(2, 2, LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());
        try {
            ArrayList<Callable<String>> l = new ArrayList<Callable<String>>();
            l.add(new NPETask());
            e.invokeAny(l);
        } catch (ExecutionException success) {
        } catch(Exception ex) {
            unexpectedException();
        } finally {
            joinPool(e);
        }
    }

    /**
     * invokeAny(c) returns result of some task
     */
    @Test
    public void testInvokeAny5() {
        ExecutorService e = new DistributedThreadPoolExecutor(2, 2, LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());
        try {
            ArrayList<Callable<String>> l = new ArrayList<Callable<String>>();
            l.add(new StringTask());
            l.add(new StringTask());
            String result = e.invokeAny(l);
            assertSame(TEST_STRING, result);
        } catch (ExecutionException success) {
        } catch(Exception ex) {
            unexpectedException();
        } finally {
            joinPool(e);
        }
    }

    /**
     * invokeAll(null) throws NPE
     */
    @Test
    public void testInvokeAll1() {
        ExecutorService e = new DistributedThreadPoolExecutor(2, 2, LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());
        try {
            e.invokeAll(null);
        } catch (NullPointerException success) {
        } catch(Exception ex) {
            unexpectedException();
        } finally {
            joinPool(e);
        }
    }

    /**
     * invokeAll(empty collection) returns empty collection
     */
    @Test
    public void testInvokeAll2() {
        ExecutorService e = new DistributedThreadPoolExecutor(2, 2, LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());
        try {
            List<Future<String>> r = e.invokeAll(new ArrayList<Callable<String>>());
            assertTrue(r.isEmpty());
        } catch(Exception ex) {
            unexpectedException();
        } finally {
            joinPool(e);
        }
    }

    /**
     * invokeAll(c) throws NPE if c has null elements
     */
    @Test
    public void testInvokeAll3() {
        ExecutorService e = new DistributedThreadPoolExecutor(2, 2, LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());
        try {
            ArrayList<Callable<String>> l = new ArrayList<Callable<String>>();
            l.add(new StringTask());
            l.add(null);
            e.invokeAll(l);
        } catch (NullPointerException success) {
        } catch(Exception ex) {
            unexpectedException();
        } finally {
            joinPool(e);
        }
    }

    /**
     * get of element of invokeAll(c) throws exception on failed task
     */
    @Test
    public void testInvokeAll4() {
        ExecutorService e = new DistributedThreadPoolExecutor(2, 2, LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());
        try {
            ArrayList<Callable<String>> l = new ArrayList<Callable<String>>();
            l.add(new NPETask());
            List<Future<String>> result = e.invokeAll(l);
            assertEquals(1, result.size());
            for (Iterator<Future<String>> it = result.iterator(); it.hasNext();) 
                it.next().get();
        } catch(ExecutionException success) {
        } catch(Exception ex) {
            unexpectedException();
        } finally {
            joinPool(e);
        }
    }

    /**
     * invokeAll(c) returns results of all completed tasks
     */
    @Test
    public void testInvokeAll5() {
        ExecutorService e = new DistributedThreadPoolExecutor(2, 2, LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());
        try {
            ArrayList<Callable<String>> l = new ArrayList<Callable<String>>();
            l.add(new StringTask());
            l.add(new StringTask());
            List<Future<String>> result = e.invokeAll(l);
            assertEquals(2, result.size());
            for (Iterator<Future<String>> it = result.iterator(); it.hasNext();) 
                assertSame(TEST_STRING, it.next().get());
        } catch (ExecutionException success) {
        } catch(Exception ex) {
            unexpectedException();
        } finally {
            joinPool(e);
        }
    }

    /**
     * timed invokeAny(null) throws NPE
     */
    @Test
    public void testTimedInvokeAny1() {
        ExecutorService e = new DistributedThreadPoolExecutor(2, 2, LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());
        try {
            e.invokeAny(null, MEDIUM_DELAY_MS, TimeUnit.MILLISECONDS);
        } catch (NullPointerException success) {
        } catch(Exception ex) {
            unexpectedException();
        } finally {
            joinPool(e);
        }
    }

    /**
     * timed invokeAny(,,null) throws NPE
     */
    @Test
    public void testTimedInvokeAnyNullTimeUnit() {
        ExecutorService e = new DistributedThreadPoolExecutor(2, 2, LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());
        try {
            ArrayList<Callable<String>> l = new ArrayList<Callable<String>>();
            l.add(new StringTask());
            e.invokeAny(l, MEDIUM_DELAY_MS, null);
        } catch (NullPointerException success) {
        } catch(Exception ex) {
            unexpectedException();
        } finally {
            joinPool(e);
        }
    }

    /**
     * timed invokeAny(empty collection) throws IAE
     */
    @Test
    public void testTimedInvokeAny2() {
        ExecutorService e = new DistributedThreadPoolExecutor(2, 2, LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());
        try {
            e.invokeAny(new ArrayList<Callable<String>>(), MEDIUM_DELAY_MS, TimeUnit.MILLISECONDS);
        } catch (IllegalArgumentException success) {
        } catch(Exception ex) {
            unexpectedException();
        } finally {
            joinPool(e);
        }
    }

    /**
     * timed invokeAny(c) throws NPE if c has null elements
     */
    @Test
    public void testTimedInvokeAny3() {
        ExecutorService e = new DistributedThreadPoolExecutor(2, 2, LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());
        try {
            ArrayList<Callable<String>> l = new ArrayList<Callable<String>>();
            l.add(new StringTask());
            l.add(null);
            e.invokeAny(l, MEDIUM_DELAY_MS, TimeUnit.MILLISECONDS);
        } catch (NullPointerException success) {
        } catch(Exception ex) {
            ex.printStackTrace();
            unexpectedException();
        } finally {
            joinPool(e);
        }
    }

    /**
     * timed invokeAny(c) throws ExecutionException if no task completes
     */
    @Test
    public void testTimedInvokeAny4() {
        ExecutorService e = new DistributedThreadPoolExecutor(2, 2, LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());
        try {
            ArrayList<Callable<String>> l = new ArrayList<Callable<String>>();
            l.add(new NPETask());
            e.invokeAny(l, MEDIUM_DELAY_MS, TimeUnit.MILLISECONDS);
        } catch(ExecutionException success) {
        } catch(Exception ex) {
            unexpectedException();
        } finally {
            joinPool(e);
        }
    }

    /**
     * timed invokeAny(c) returns result of some task
     */
    @Test
    public void testTimedInvokeAny5() {
        ExecutorService e = new DistributedThreadPoolExecutor(2, 2, LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());
        try {
            ArrayList<Callable<String>> l = new ArrayList<Callable<String>>();
            l.add(new StringTask());
            l.add(new StringTask());
            String result = e.invokeAny(l, MEDIUM_DELAY_MS, TimeUnit.MILLISECONDS);
            assertSame(TEST_STRING, result);
        } catch (ExecutionException success) {
        } catch(Exception ex) {
            unexpectedException();
        } finally {
            joinPool(e);
        }
    }

    /**
     * timed invokeAll(null) throws NPE
     */
    @Test
    public void testTimedInvokeAll1() {
        ExecutorService e = new DistributedThreadPoolExecutor(2, 2, LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());
        try {
            e.invokeAll(null, MEDIUM_DELAY_MS, TimeUnit.MILLISECONDS);
        } catch (NullPointerException success) {
        } catch(Exception ex) {
            unexpectedException();
        } finally {
            joinPool(e);
        }
    }

    /**
     * timed invokeAll(,,null) throws NPE
     */
    @Test
    public void testTimedInvokeAllNullTimeUnit() {
        ExecutorService e = new DistributedThreadPoolExecutor(2, 2, LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());
        try {
            ArrayList<Callable<String>> l = new ArrayList<Callable<String>>();
            l.add(new StringTask());
            e.invokeAll(l, MEDIUM_DELAY_MS, null);
        } catch (NullPointerException success) {
        } catch(Exception ex) {
            unexpectedException();
        } finally {
            joinPool(e);
        }
    }

    /**
     * timed invokeAll(empty collection) returns empty collection
     */
    @Test
    public void testTimedInvokeAll2() {
        ExecutorService e = new DistributedThreadPoolExecutor(2, 2, LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());
        try {
            List<Future<String>> r = e.invokeAll(new ArrayList<Callable<String>>(), MEDIUM_DELAY_MS, TimeUnit.MILLISECONDS);
            assertTrue(r.isEmpty());
        } catch(Exception ex) {
            unexpectedException();
        } finally {
            joinPool(e);
        }
    }

    /**
     * timed invokeAll(c) throws NPE if c has null elements
     */
    @Test
    public void testTimedInvokeAll3() {
        ExecutorService e = new DistributedThreadPoolExecutor(2, 2, LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());
        try {
            ArrayList<Callable<String>> l = new ArrayList<Callable<String>>();
            l.add(new StringTask());
            l.add(null);
            e.invokeAll(l, MEDIUM_DELAY_MS, TimeUnit.MILLISECONDS);
        } catch (NullPointerException success) {
        } catch(Exception ex) {
            unexpectedException();
        } finally {
            joinPool(e);
        }
    }

    /**
     * get of element of invokeAll(c) throws exception on failed task
     */
    @Test
    public void testTimedInvokeAll4() {
        ExecutorService e = new DistributedThreadPoolExecutor(2, 2, LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());
        try {
            ArrayList<Callable<String>> l = new ArrayList<Callable<String>>();
            l.add(new NPETask());
            List<Future<String>> result = e.invokeAll(l, MEDIUM_DELAY_MS, TimeUnit.MILLISECONDS);
            assertEquals(1, result.size());
            for (Iterator<Future<String>> it = result.iterator(); it.hasNext();) 
                it.next().get();
        } catch(ExecutionException success) {
        } catch(Exception ex) {
            unexpectedException();
        } finally {
            joinPool(e);
        }
    }

    /**
     * timed invokeAll(c) returns results of all completed tasks
     */
    @Test
    public void testTimedInvokeAll5() {
        ExecutorService e = new DistributedThreadPoolExecutor(2, 2, LONG_DELAY_MS, TimeUnit.MILLISECONDS, getNewQueue(), gridProvider, getNewExecutorName());
        try {
            ArrayList<Callable<String>> l = new ArrayList<Callable<String>>();
            l.add(new StringTask());
            l.add(new StringTask());
            List<Future<String>> result = e.invokeAll(l, MEDIUM_DELAY_MS, TimeUnit.MILLISECONDS);
            assertEquals(2, result.size());
            for (Iterator<Future<String>> it = result.iterator(); it.hasNext();) 
                assertSame(TEST_STRING, it.next().get());
        } catch (ExecutionException success) {
        } catch(Exception ex) {
            unexpectedException();
        } finally {
            joinPool(e);
        }
    }
}
