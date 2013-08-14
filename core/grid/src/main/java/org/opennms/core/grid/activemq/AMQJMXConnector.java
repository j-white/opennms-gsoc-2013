package org.opennms.core.grid.activemq;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;

public class AMQJMXConnector {
    //private static final Logger LOG = LoggerFactory.getLogger(AMQJMXConnector.class);
    private final String amqJmxUri = "service:jmx:rmi:///jndi/rmi://localhost:1099/jmxrmi";
    private final String amqObjectName = "org.apache.activemq:BrokerName=localhost,Type=Broker";
        
    private Map<String, QueueViewMBean> m_queueViewBeanCache = new ConcurrentHashMap<String, QueueViewMBean>();

    private static class NoSuchQueueException extends Exception {
        private static final long serialVersionUID = 988646385927165928L;
    }

    private QueueViewMBean getQueueViewMBean(String queueName) throws IOException, MalformedObjectNameException, InterruptedException, NoSuchQueueException {
        QueueViewMBean myBean = m_queueViewBeanCache.get(queueName);
        if (myBean != null) {
            return myBean;
        }

        JMXServiceURL url = new JMXServiceURL(amqJmxUri);
        JMXConnector jmxc = JMXConnectorFactory.connect(url);
        MBeanServerConnection conn = jmxc.getMBeanServerConnection();

        ObjectName activeMQ = new ObjectName(amqObjectName);
        BrokerViewMBean mbean = (BrokerViewMBean) MBeanServerInvocationHandler.newProxyInstance(conn, activeMQ,BrokerViewMBean.class, true);

        for (ObjectName name : mbean.getQueues()) {
            QueueViewMBean queueMbean = (QueueViewMBean)
                   MBeanServerInvocationHandler.newProxyInstance(conn, name, QueueViewMBean.class, true);

            if (queueMbean.getName().equals(queueName)) {
                m_queueViewBeanCache.put(queueName, queueMbean);
                myBean = queueMbean;
            }
        }

        if (myBean == null) {
            throw new NoSuchQueueException();
        }

        return myBean;
    }

    public long queueSize(String queueName) throws Exception {
        try {
            QueueViewMBean qvmb = getQueueViewMBean(queueName);
            return qvmb.getQueueSize();
        } catch (NoSuchQueueException e) {
            return 0;
        }
    }

    public void queueClear(String queueName) throws Exception {
        try {
            QueueViewMBean qvmb = getQueueViewMBean(queueName);
            qvmb.purge();
        } catch (NoSuchQueueException ignore) {
            // the queue does not exist
        }
    }

    public boolean queueRemove(String queueName, String messageId) throws Exception {
        try {
            QueueViewMBean qvmb = getQueueViewMBean(queueName);
            return qvmb.removeMessage(messageId);
        } catch (NoSuchQueueException e) {
            return false;
        }
    }
}
