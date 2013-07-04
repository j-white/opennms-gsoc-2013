package org.opennms.core.grid;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;

public class DistributedExecutors {
    public static DistributedThreadPoolExecutor newDistributedExecutor(int nThreads, ThreadFactory threadFactory,
            DataGridProvider dataGridProvider, String queueName) {
        BlockingQueue<Runnable> workQueue = dataGridProvider.getQueue(queueName);
        return new DistributedThreadPoolExecutor(nThreads, threadFactory, workQueue);
    }
    
    public static DistributedThreadPoolExecutor newDistributedExecutor(int nThreads, ThreadFactory threadFactory,
            DataGridProvider dataGridProvider, String queueName, DistributedExecutionVisitor visitor) {
        BlockingQueue<Runnable> workQueue = dataGridProvider.getQueue(queueName);
        return new DistributedThreadPoolExecutor(nThreads, threadFactory, workQueue, visitor);
    }
}
