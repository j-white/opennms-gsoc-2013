package org.opennms.core.grid.activemq;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * Experimental queue that stores uses ActiveMQ as a store.
 *
 * A JMX connection the ActiveMQ server is used for functions
 * such as size() and clear().
 *
 * @author jwhite
 */
public class AMQBlockingQueue<T> extends AbstractBlockingQueue<T> {
    private static final String QUEUE_PREFIX = "onms.queue.";

    private final ActiveMQConnectionFactory m_connectionFactory;
    private final AMQJMXConnector m_jmxConnector;
    private final String m_name;

    private Connection m_connection;
    private Session m_session;
    private Queue m_queue;
    private MessageProducer m_producer;
    private MessageConsumer m_consumer;

    public AMQBlockingQueue(ActiveMQConnectionFactory connectionFactory,
            AMQJMXConnector jmxConnector, String name) throws Exception {
        m_connectionFactory = connectionFactory;
        m_jmxConnector = jmxConnector;
        m_name = QUEUE_PREFIX + name;

        m_connection = m_connectionFactory.createConnection();
        m_connection.start();
        m_session = m_connection.createSession(false,
                                               Session.AUTO_ACKNOWLEDGE);
        m_queue = m_session.createQueue(m_name);
        
        m_producer = m_session.createProducer(m_queue);
        m_consumer = m_session.createConsumer(m_queue);
    }

    @Override
    public T peek() {
        try {
            Session session = m_connection.createSession(true,
                                                       Session.SESSION_TRANSACTED);
            Destination destination = session.createQueue(m_name);
            MessageConsumer consumer = session.createConsumer(destination);

            Message msg = consumer.receiveNoWait();
            if (msg != null) {
                return getObjectFromMessage(msg);
            }

            consumer.close();
            session.close();
        } catch (Exception e) {
            throw new AMQException(e);
        }

        return null;
    }

    @Override
    public int size() {
        try {
            return (int) m_jmxConnector.queueSize(m_name);
        } catch (Exception e) {
            throw new AMQException(e);
        }
    }

    @Override
    public void clear() {
        try {
            m_jmxConnector.queueClear(m_name);
        } catch (Exception e) {
            throw new AMQException(e);
        }
    }

    @Override
    public Collection<T> getElements() {
        List<T> elements = new ArrayList<T>();

        try {
            Session session = m_connection.createSession(true,
                                                       Session.SESSION_TRANSACTED);
            Destination destination = session.createQueue(m_name);
            MessageConsumer consumer = session.createConsumer(destination);

            Message msg = null;
            do {
                msg = consumer.receiveNoWait();
                if (msg != null) {
                    T el = getObjectFromMessage(msg);
                    elements.add(el);
                }
            } while(msg != null);

            consumer.close();
            session.close();
        } catch (Exception e) {
            throw new AMQException(e);
        }

        return elements;
    }

    @Override
    public boolean remove(Object o) {
        try {
            Session session = m_connection.createSession(true,
                                                       Session.SESSION_TRANSACTED);
            Destination destination = session.createQueue(m_name);
            MessageConsumer consumer = session.createConsumer(destination);

            Message msg = null;
            do {
                msg = consumer.receiveNoWait();
                if (msg != null) {
                    T el = getObjectFromMessage(msg);
                    if (o.equals(el)) {
                        return m_jmxConnector.queueRemove(m_name, msg.getJMSMessageID());
                    }
                }
            } while(msg != null);

            consumer.close();
            session.close();
        } catch (Exception e) {
            throw new AMQException(e);
        }

        return false;
    }

    @Override
    public boolean add(T e) {
        if (e == null) {
            throw new NullPointerException("Cannot enqueue a null object.");
        } else if (!(e instanceof Serializable)) {
            throw new RuntimeException("Type must be serializable.");
        }

        try {
            ObjectMessage objectMessage = m_session.createObjectMessage();
            objectMessage.setObject((Serializable)e);
            m_producer.send(objectMessage);
        } catch (JMSException ex) {
            throw new AMQException(ex);
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    private T getObjectFromMessage(Message m) throws JMSException {
        if (m == null) {
            return null;
        } else if (m instanceof ObjectMessage) {
            ObjectMessage objectMessage = (ObjectMessage)m;
            return (T) objectMessage.getObject();
        } else {
            throw new RuntimeException("Invalid message in queue.");
        }
    }

    @Override
    public T take() throws InterruptedException {
        try {
            System.out.println("before.");
            MessageConsumer consumer = m_session.createConsumer(m_queue);
            Message message = consumer.receive();
            consumer.close();
            System.out.println("after.");
            return getObjectFromMessage(message);
        } catch (JMSException ex) {
            throw new AMQException(ex);
        }
    }

    @Override
    public T poll() {
        try {
            Message message = m_consumer.receiveNoWait();
            return getObjectFromMessage(message);
        } catch (JMSException ex) {
            throw new AMQException(ex);
        }
    }

    @Override
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        try {
            long effectiveTimeout = TimeUnit.MILLISECONDS.convert(timeout,
                                                                  unit);
            if (effectiveTimeout <= 0) {
                effectiveTimeout = 1;
            }
            Message message = m_consumer.receive(effectiveTimeout);
            return getObjectFromMessage(message);
        } catch (JMSException ex) {
            throw new AMQException(ex);
        }
    }
}
