package org.opennms.core.grid;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DistributedThreadPoolExecutor extends ThreadPoolExecutor {

    private BlockingQueue<Runnable> m_workQueue;
    private DistributedExecutionVisitor m_visitor;
    private boolean fairPolicy = true;

    public DistributedThreadPoolExecutor(int nThreads,
            ThreadFactory threadFactory, BlockingQueue<Runnable> workQueue) {
        super(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, workQueue,
              threadFactory);
        init(workQueue, null);
    }

    public DistributedThreadPoolExecutor(int nThreads,
            ThreadFactory threadFactory, BlockingQueue<Runnable> workQueue,
            DistributedExecutionVisitor visitor) {
        super(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, workQueue,
              threadFactory);
        init(workQueue, visitor);
    }

    private void init(BlockingQueue<Runnable> workQueue,
            DistributedExecutionVisitor visitor) {
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

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        super.beforeExecute(t, r);
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
