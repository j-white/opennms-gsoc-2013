package org.opennms.core.grid.concurrent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.opennms.core.grid.DataGridProvider;
import org.opennms.core.grid.DataGridProviderAware;

/**
 * TODO: Prevent data from accumulating in the future map.
 *
 * @author jwhite
 */
public class DistributedThreadPoolExecutor extends ThreadPoolExecutor {

    private BlockingQueue<Runnable> m_workQueue;
    private DistributedExecutionVisitor m_visitor;
    private DataGridProvider m_dataGridProvider;
    private String m_name;
    private boolean fairPolicy = true;

    public DistributedThreadPoolExecutor(int nThreads,
            ThreadFactory threadFactory, DataGridProvider dataGridProvider,
            String name, BlockingQueue<Runnable> workQueue) {
        super(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, workQueue,
              threadFactory);
        init(dataGridProvider, name, workQueue, null);
    }

    public DistributedThreadPoolExecutor(int nThreads,
            ThreadFactory threadFactory, DataGridProvider dataGridProvider,
            String name, BlockingQueue<Runnable> workQueue,
            DistributedExecutionVisitor visitor) {
        super(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, workQueue,
              threadFactory);
        init(dataGridProvider, name, workQueue, visitor);
    }

    public static String getQueueName(String name) {
        return name + ".Queue";
    }

    private void init(DataGridProvider dataGridProvider, String name,
            BlockingQueue<Runnable> workQueue,
            DistributedExecutionVisitor visitor) {
        m_dataGridProvider = dataGridProvider;
        m_name = name;
        m_workQueue = workQueue;
        m_visitor = visitor;

        if (fairPolicy) {
            this.prestartAllCoreThreads();
        }
    }

    @Override
    public void execute(Runnable command) {
        if (command == null)
            throw new NullPointerException();

        if (fairPolicy) {
            m_workQueue.add(command);
        } else {
            // greedy policy
            super.execute(command);
        }
    }

    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
        return new DistributedFutureTask<T>(runnable, value, m_dataGridProvider, m_name + ".Futures");
    }

    protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
        return new DistributedFutureTask<T>(callable, m_dataGridProvider, m_name + ".Futures");
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        super.beforeExecute(t, r);

        // Set the data grid instance
        if (r instanceof DataGridProviderAware) {
            ((DataGridProviderAware) r).setDataGridProvider(m_dataGridProvider);
        }
        
        if (m_visitor != null) {
            m_visitor.beforeExecute(t, r);
        }
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        if (m_visitor != null) {
            m_visitor.afterExecute(r, t);
        }
    }
}
