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
        scheduleWith(m_scheduler, true);
    }

    @Override
    public boolean isReady() {
        return true;
    }

    public void scheduleWith(Scheduler scheduler, boolean isReschedule) {
        scheduler.schedule(m_period, this, isReschedule);
    }

    public void setScheduler(Scheduler scheduler) {
        m_scheduler = scheduler;
    }

    private DataSource getDataSourceFactory() {
        return DataSourceFactory.getInstance();
    }

    public String toString() {
        return "SchedulerAware";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + m_period;
        result = prime * result
                + ((m_statement == null) ? 0 : m_statement.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        StatementProcessor other = (StatementProcessor) obj;
        if (m_period != other.m_period)
            return false;
        if (m_statement == null) {
            if (other.m_statement != null)
                return false;
        } else if (!m_statement.equals(other.m_statement))
            return false;
        return true;
    }
}
