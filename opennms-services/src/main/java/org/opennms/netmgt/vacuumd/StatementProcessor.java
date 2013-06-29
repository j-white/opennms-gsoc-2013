package org.opennms.netmgt.vacuumd;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.opennms.core.db.DataSourceFactory;
import org.opennms.netmgt.config.vacuumd.Statement;
import org.opennms.netmgt.scheduler.ClusterRunnable;
import org.opennms.netmgt.scheduler.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatementProcessor implements ClusterRunnable {
    private static final Logger LOG = LoggerFactory.getLogger(StatementProcessor.class);
    private static final long serialVersionUID = 4019877945309435097L;
    private final Statement m_statement;
    private final int m_period;
    private transient Scheduler m_scheduler;

    public StatementProcessor(final Statement statement,final int period) {
        m_statement = statement;
        m_period = period;
    }

    private void runUpdate(String sql, boolean transactional) {
        LOG.info("Vacuumd executing statement: " + sql);
        // update the database
        Connection dbConn = null;

        // initially set doCommit to avoid doing a commit in the finally
        // if an exception is thrown.
        boolean commitRequired = false;
        boolean autoCommitFlag = !transactional;
        try {
            dbConn = getDataSourceFactory().getConnection();
            dbConn.setAutoCommit(autoCommitFlag);

            PreparedStatement stmt = dbConn.prepareStatement(sql);
            int count = stmt.executeUpdate();
            stmt.close();

            if (LOG.isDebugEnabled()) {
                LOG.debug("Vacuumd: Ran update " + sql + ": this affected "
                                    + count + " rows");
            }

            commitRequired = transactional;
        } catch (SQLException ex) {
            LOG.error("Vacuumd:  Database error execuating statement  "
                                + sql, ex);
        } finally {
            if (dbConn != null) {
                try {
                    if (commitRequired) {
                        dbConn.commit();
                    } else if (transactional) {
                        dbConn.rollback();
                    }
                } catch (SQLException ex) {
                } finally {
                    if (dbConn != null) {
                        try {
                            dbConn.close();
                        } catch (Throwable e) {
                        }
                    }
                }
            }
        }
    }

    @Override
    public void run() {
        runUpdate(m_statement.getContent(), m_statement.getTransactional());
        schedule();
    }

    @Override
    public boolean isReady() {
        return true;
    }

    private void schedule() {
        scheduleWith(m_scheduler);
    }

    public void scheduleWith(Scheduler scheduler) {
        scheduler.schedule(m_period, this);
    }

    public void setScheduler(Scheduler scheduler) {
        m_scheduler = scheduler;
    }

    private DataSource getDataSourceFactory() {
        return DataSourceFactory.getInstance();
    }

    public String toString() {
        return "SchedulerAware!";
    }
}
