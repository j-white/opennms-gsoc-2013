package org.opennms.core.grid.concurrent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;

import org.opennms.core.grid.DataGridProvider;

public class DistributedExecutors {
    public static DistributedThreadPoolExecutor newDistributedExecutor(
            int nThreads, ThreadFactory threadFactory,
            DataGridProvider dataGridProvider, String name) {
        BlockingQueue<Runnable> workQueue = dataGridProvider.getQueue(DistributedThreadPoolExecutor.getQueueName(name));
        return new DistributedThreadPoolExecutor(nThreads, threadFactory,
                                                 dataGridProvider, name,
                                                 workQueue);
    }

    public static DistributedThreadPoolExecutor newDistributedExecutor(
            int nThreads, ThreadFactory threadFactory,
            DataGridProvider dataGridProvider, String name,
            DistributedExecutionVisitor visitor) {
        BlockingQueue<Runnable> workQueue = dataGridProvider.getQueue(DistributedThreadPoolExecutor.getQueueName(name));
        return new DistributedThreadPoolExecutor(nThreads, threadFactory,
                                                 dataGridProvider, name,
                                                 workQueue, visitor);
    }
}
