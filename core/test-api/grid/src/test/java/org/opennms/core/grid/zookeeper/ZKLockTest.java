package org.opennms.core.grid.zookeeper;

import java.util.concurrent.locks.Lock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opennms.test.mock.MockUtil;

public class ZKLockTest {

    private TestingServer m_server;

    @Before
    public void setUp() throws Exception {
        m_server = new TestingServer();
    }

    @After
    public void tearDown() throws Exception {
        if (m_server != null) {
            m_server.close();
        }
    }

    public void grabLock() throws InterruptedException {
        final String NAME = "example";
        CuratorFramework client = CuratorFrameworkFactory.newClient(m_server.getConnectString(),
                                                                    new ExponentialBackoffRetry(
                                                                                                1000,
                                                                                                3));
        client.start();
        Lock lock = new ZKLock(client, NAME);
        lock.lock();
        try {
            MockUtil.println("got lock");
            Thread.sleep(100);
        } finally {
            lock.unlock();
        }
    }

    @Test
    public void doTest() throws Exception {
        final int QTY = 5;
        for (int i = 0; i < QTY; i++) {
            grabLock();
        }
    }
}
