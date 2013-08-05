package org.opennms.core.grid.zookeeper;

import org.apache.curator.RetryLoop;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper class for org.apache.curator.RetryLoop that ignores
 * java.lang.InterruptedException exceptions.
 *
 * @author jwhite
 */
public class UninterruptibleRetryLoop {
    private static final Logger LOG = LoggerFactory.getLogger(UninterruptibleRetryLoop.class);
    private final RetryLoop m_retryLoop;

    public UninterruptibleRetryLoop(CuratorFramework client) {
        m_retryLoop = client.getZookeeperClient().newRetryLoop();
    }

    /**
     * If true is returned, make an attempt at the operation
     *
     * @return true/false
     */
    public boolean shouldContinue() {
        return m_retryLoop.shouldContinue();
    }

    /**
     * Call this when your operation has successfully completed
     */
    public void markComplete() {
        m_retryLoop.markComplete();
    }

    /**
     * Pass any caught exceptions here
     *
     * @param exception the exception
     * @throws Exception if not retry-able or the retry policy returned negative
     */
    public void takeException(Exception exception) throws Exception {
        if (exception instanceof InterruptedException) {
            LOG.warn("Ignoring interrupt.");

            // Reset the interrupted flag
            Thread.interrupted();

            // Ignore the exception
            return;
        }

        m_retryLoop.takeException(exception);
    }
}
