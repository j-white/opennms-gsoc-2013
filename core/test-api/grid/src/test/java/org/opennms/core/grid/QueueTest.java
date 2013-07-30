package org.opennms.core.grid;

import static com.jayway.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import org.junit.Test;
import org.opennms.test.mock.MockUtil;

public class QueueTest extends AbstractGridTest {

    /**
     * Ensure that the elements added to the distributed queue are
     * distributed amongst the available consumers.
     */
    @Test
    public void queueWithCompetingConsumers() {
        final int NUM_ELEMENTS_PER_CONSUMER = 5;
        final String queueName = "myTestQueue";

        // Initialize the grid providers and the queue consumers
        DataGridProvider dataGridProvider[] = new DataGridProvider[N_MEMBERS];
        MyConsumer consumer[] = new MyConsumer[N_MEMBERS];
        for (int i = 0; i < N_MEMBERS; i++) {
            dataGridProvider[i] = DataGridProviderFactory.getNewInstance();
            dataGridProvider[i].init();

            consumer[i] = new MyConsumer(dataGridProvider[i], queueName);
        }

        await().until(getNumClusterMembers(dataGridProvider[N_MEMBERS - 1]),
                      is(N_MEMBERS));

        // Initialize the queue
        Queue<Integer> queue = dataGridProvider[0].getQueue("myTestQueue");

        // Fire up the consumers
        for (int i = 0; i < N_MEMBERS; i++) {
            consumer[i].start();
        }

        // Add elements to the queue
        for (int i = 0; i < (N_MEMBERS * NUM_ELEMENTS_PER_CONSUMER); i++) {
            queue.add(i);
        }

        // Wait until the queue is empty
        await().until(getNumElements(queue), is(0));

        int totalNumElementsConsumed = 0;
        MockUtil.println("Queue distribution:");
        for (int i = 0; i < N_MEMBERS; i++) {
            int els = consumer[i].getNumElementsConsumed();
            totalNumElementsConsumed += els;
            MockUtil.println(String.format("\t Consumer[%d]: %d",
                                             i,
                                             els));
        }

        // Kill the consumers
        for (int i = 0; i < N_MEMBERS; i++) {
            consumer[i].stop();
        }

        // Verify the total
        assertEquals(N_MEMBERS*NUM_ELEMENTS_PER_CONSUMER, totalNumElementsConsumed);
    }

    private Callable<Integer> getNumElements(final Queue<Integer> queue) {
        return new Callable<Integer>() {
            public Integer call() throws Exception {
                return queue.size();
            }
        };
    }

    private class MyConsumer implements Runnable {
        private DataGridProvider m_dataGridProvider;
        private String m_queueName;
        private int m_numElementsConsumed = 0;
        private Thread m_thread = null;

        public MyConsumer(DataGridProvider dataGridProvider, String queueName) {
            m_dataGridProvider = dataGridProvider;
            m_queueName = queueName;
        }

        public void start() {
            m_thread = new Thread(this);
            m_thread.start();
        }

        public void stop() {
            if (m_thread != null) {
                m_thread.interrupt();
                m_thread = null;
            }
        }

        public int getNumElementsConsumed() {
            return m_numElementsConsumed;
        }

        @Override
        public void run() {
            BlockingQueue<Integer> queue = m_dataGridProvider.getQueue(m_queueName);
            try {
                while (true) {
                    queue.take();
                    m_numElementsConsumed++;
                }
            } catch (InterruptedException e) {
                return;
            }
        }
    }
}
