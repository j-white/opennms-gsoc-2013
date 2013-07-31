package org.opennms.netmgt.poller;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import org.opennms.core.concurrent.LogPreservingThreadFactory;
import org.opennms.core.grid.DataGridProvider;
import org.opennms.core.grid.DataGridProviderFactory;
import org.opennms.core.grid.LeaderSelector;
import org.opennms.core.grid.LeaderSelectorListener;
import org.opennms.core.grid.concurrent.DistributedExecutionVisitor;
import org.opennms.core.grid.concurrent.DistributedExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

/**
 * Allows several instances of the poller to run in parallel.
 * 
 * Every instance sets up a distributed executor that is used to perform
 * the polling, response time collection and thresholding.
 *
 * Amongst these, one is elected leader and is made in charge
 * of running the standard poller service. The standard poller service
 * handles events and maintains the state of the network/tree.
 *
 * <b>NOTE:</b> Ideally I would like to split all tasks evenly amongst the available
 * instances, but this would require a significant rewrite of the current
 * service.
 *
 * @author jwhite
 *
 */
public class DistributedPoller extends Poller implements LeaderSelectorListener, DistributedExecutionVisitor {

    /**
     * Data grid provider.
     */
    private DataGridProvider m_dataGridProvider;

    /**
     * Distributed executor.
     */
    private ExecutorService m_executor;

    /**
     * Used to perform leader election.
     */
    private LeaderSelector m_leaderSelector;

    /**
     * Set to to the thread handle on which the takeLeadership() function is
     * being run. Only set when we are the leader.
     */
    private volatile Thread m_leaderThread = null;

    /**
     * Used to notify the leader thread we are stopping.
     */
    private volatile boolean m_stopped = true;

    /**
     * Is the poller running on this instance?
     */
    private volatile boolean m_pollerRunning = false;

    /**
     * Keeps track of the number of polls that have originate from this instance.
     */
    private AtomicLong m_tasksExecuted = new AtomicLong();

    /**
     * Number of milliseconds used to sleep before checking the stopped flag
     * when leader.
     */
    public static final int LEADER_SLEEP_MS = 500;
    
    /**
     * Logger.
     */
    private final static Logger LOG = LoggerFactory.getLogger(DistributedPoller.class);

    @Override
    protected void onInit() {
        if (m_dataGridProvider == null) {
            m_dataGridProvider = DataGridProviderFactory.getInstance();
        }

        createExecutor();

        m_leaderSelector = new LeaderSelector("poller", this, m_dataGridProvider);
    }

    private void createExecutor() {
        int threadPoolSize = getPollerConfig().getThreads();
        String executorName = Poller.LOG4J_CATEGORY;

        // Initialize the distributed executor
        ThreadFactory threadFactory = new LogPreservingThreadFactory(
                                                                     getClass().getSimpleName(),
                                                                     threadPoolSize,
                                                                     true);

        LOG.debug("Creating distributed executor called {} with {} threads",
                  executorName, threadPoolSize);
        m_executor = DistributedExecutors.newDistributedExecutor(threadPoolSize,
                                                               threadFactory,
                                                               m_dataGridProvider,
                                                               executorName,
                                                               this);
    }

    @Override
    protected void onStart() {
        m_stopped = false;
        m_leaderSelector.start();
    }

    @Override
    protected void onStop() {
        LOG.debug("Stopping the leader selector.");
        m_leaderSelector.stop();
        if (m_leaderThread != null) {
            m_leaderThread.interrupt();
        }
        m_stopped = true;

        m_executor.shutdown();
    }

    @Override
    protected void onPause() {
        if (m_pollerRunning) {
            super.onPause();
        }
    }

    @Override
    protected void onResume() {
        if (m_pollerRunning) {
            super.onResume();
        }
    }

    @Override
    public void takeLeadership() {
        m_leaderThread = Thread.currentThread();
        LOG.info("Node was elected leader. Starting the poller.");

        super.onInit();
        super.onStart();

        LOG.debug("Done starting the poller.");
        m_pollerRunning = true;
        try {
            while (true) {
                Thread.sleep(LEADER_SLEEP_MS);
                if (m_stopped) {
                    break;
                }
            }
        } catch (InterruptedException abort) {
            LOG.info("We were interrupted!");
        } finally {
            LOG.info("Stopping the poller.");
            super.onStop();
            m_pollerRunning = false;

            LOG.info("Relinquishing leadership.");
            m_leaderThread = null;
        }
    }

    public boolean isLeaderPoller() {
        return (m_leaderThread != null && m_pollerRunning);
    }

    public LeaderSelector getLeaderSelector() {
        return m_leaderSelector;
    }

    @Required
    public void setLeaderSelector(LeaderSelector leaderSelector) {
        if (!m_stopped) {
            throw new RuntimeException("The leader selector can only be set "
                    + "when the service is stopped.");
        }
        m_leaderSelector = leaderSelector;
    }

    public DataGridProvider getDataGridProvider() {
        return m_dataGridProvider;
    }

    public void setDataGridProvider(DataGridProvider dataGridProvider) {
        m_dataGridProvider = dataGridProvider;
    }

    @Override
    public void beforeExecute(Thread t, Runnable r) {
        // this method is intentionally left blank
    }

    @Override
    public void afterExecute(Runnable r, Throwable t) {
        m_tasksExecuted.getAndIncrement();
    }

    @Override
    public long getNumPollsLocal() {
        return m_tasksExecuted.get();
    }

    public ExecutorService getExecutor() {
        return m_executor;
    }
}
