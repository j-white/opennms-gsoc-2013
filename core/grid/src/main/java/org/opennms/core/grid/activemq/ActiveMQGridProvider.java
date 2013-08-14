package org.opennms.core.grid.activemq;

import java.util.concurrent.BlockingQueue;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.opennms.core.grid.DataGridProvider;
import org.opennms.core.grid.GridConfigFactory;

public abstract class ActiveMQGridProvider implements DataGridProvider {
    //private static final Logger LOG = LoggerFactory.getLogger(ActiveMQGridProvider.class);
    private ActiveMQConnectionFactory m_connectionFactory;
    private AMQJMXConnector m_jmxConnector;

    public synchronized void init() {
        if (m_connectionFactory != null) {
            return;
        }

        // Connect to the ActiveMQ JMS broker
        m_connectionFactory = new ActiveMQConnectionFactory(GridConfigFactory.getInstance().getBrokerURL());

        // Setup the JMX connector
        m_jmxConnector = new AMQJMXConnector();
    }

    public synchronized void shutdown() {
    }

    public synchronized <T> BlockingQueue<T> getQueue(String name) {
        init();
        try {
            return new AMQBlockingQueue<T>(m_connectionFactory, m_jmxConnector, name);
        } catch (Exception e) {
            throw new AMQException(e);
        }
    }
}
