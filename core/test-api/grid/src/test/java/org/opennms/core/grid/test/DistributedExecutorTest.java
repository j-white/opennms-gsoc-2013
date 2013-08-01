package org.opennms.core.grid.test;

import static com.jayway.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opennms.core.concurrent.LogPreservingThreadFactory;
import org.opennms.core.grid.DataGridProvider;
import org.opennms.core.grid.DataGridProviderFactory;
import org.opennms.core.grid.concurrent.DistributedExecutionVisitor;
import org.opennms.core.grid.concurrent.DistributedExecutors;
import org.opennms.core.test.MockLogAppender;
import org.opennms.core.test.MockLogger;
import org.opennms.core.test.grid.GridTest;
import org.opennms.test.mock.MockUtil;

public class DistributedExecutorTest extends GridTest {

    private DistributedExecutor m_executors[];

    @Before
    public void setUp() {
        Properties loggingProperties = new Properties();
        loggingProperties.put(MockLogger.LOG_KEY_PREFIX + "com.hazelcast",
                              "ERROR");
        MockLogAppender.setupLogging(true, "DEBUG", loggingProperties);

        m_executors = new DistributedExecutor[N_MEMBERS];
        for (int i = 0; i < N_MEMBERS; i++) {
            m_executors[i] = new DistributedExecutor("test");
        }

        MockUtil.println("Waiting for cluster to reach " + N_MEMBERS
                + " members");
        await().until(getClusterSize(m_executors[N_MEMBERS - 1].getDataGridProvider()),
                      is(N_MEMBERS));
        MockUtil.println("Cluster has reached " + N_MEMBERS + " members");
    }

    @After
    public void tearDown() {
        for (DistributedExecutor executor : m_executors) {
            executor.shutdown();
        }
    }

    @Test(timeout = 30 * 1000)
    public void futuresTest() throws Exception {
        final int N_TASK_MULTIPLE = 10;
        int N_TASKS = N_MEMBERS * N_TASK_MULTIPLE;
        List<Callable<Integer>> tasks = new ArrayList<Callable<Integer>>();
        for (int i = 0; i < N_TASKS; i++) {
            tasks.add(new MyTask());
        }

        ExecutorService runner = m_executors[0].getRunner();
        List<Future<Integer>> futures = runner.invokeAll(tasks);
        assertEquals(N_TASKS, futures.size());
        for (Future<Integer> future : futures) {
            assertEquals(Integer.valueOf(7), future.get());
        }

        int totalNumTasksExecuted = 0;
        for (int i = 0; i < N_MEMBERS; i++) {
            int numTasksExecuted = m_executors[i].getNumTasksExecuted();
            MockUtil.println(String.format("Executor[%d]: %d", i,
                                           numTasksExecuted));
            totalNumTasksExecuted += numTasksExecuted;
        }
        assertEquals(N_TASKS, totalNumTasksExecuted);
    }

    private static class DistributedExecutor implements
            DistributedExecutionVisitor {
        private ThreadFactory m_threadFactory;
        private ExecutorService m_runner;
        private DataGridProvider m_dataGridProvider;
        private int m_threadPoolSize = 1;
        private final String m_queueName;
        private int m_tasksExecuted = 0;

        public DistributedExecutor(String queueName) {
            m_queueName = queueName;
            init();
        }

        private void init() {
            m_threadFactory = new LogPreservingThreadFactory(
                                                             getClass().getSimpleName(),
                                                             m_threadPoolSize,
                                                             true);

            m_dataGridProvider = DataGridProviderFactory.getNewInstance();

            m_runner = DistributedExecutors.newDistributedExecutor(m_threadPoolSize,
                                                                   m_threadFactory,
                                                                   m_dataGridProvider,
                                                                   m_queueName,
                                                                   this);
        }

        public DataGridProvider getDataGridProvider() {
            return m_dataGridProvider;
        }

        public ExecutorService getRunner() {
            return m_runner;
        }

        public int getNumTasksExecuted() {
            return m_tasksExecuted;
        }

        public void shutdown() {
            m_runner.shutdownNow();
        }

        @Override
        public void beforeExecute(Thread t, Runnable r) {
            // Do nothing
        }

        @Override
        public void afterExecute(Runnable r, Throwable t) {
            m_tasksExecuted++;
        }
    }

    public Callable<Integer> getClusterSize(
            final DataGridProvider dataGridProvider) {
        return new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return dataGridProvider.getGridMembers().size();
            }
        };
    }

    private static class MyTask implements Callable<Integer>, Serializable {
        private static final long serialVersionUID = 1891661983222998621L;

        @Override
        public Integer call() throws Exception {
            return Integer.valueOf(7);
        }
    }
}
