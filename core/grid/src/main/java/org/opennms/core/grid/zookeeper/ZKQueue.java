package org.opennms.core.grid.zookeeper;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryLoop;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.EnsurePath;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZKUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Based on org.apache.curator.framework.recipes.queue.SimpleDistributedQueue.
 * 
 * @author jwhite
 */
public class ZKQueue<T> implements BlockingQueue<T> {
    private static final Logger LOG = LoggerFactory.getLogger(ZKQueue.class);
    public static final String PATH_PREFIX = "/onms/queue/";
    private static final String NODE_PREFIX = "qn-";

    private final CuratorFramework m_client;
    private final String m_path;

    public ZKQueue(CuratorFramework client, String name) {
        m_client = client;
        m_path = ZKPaths.makePath(PATH_PREFIX, name);
    }

    @Override
    public T remove() {
        T node = null;

        try {
            UninterruptibleRetryLoop retryLoop = new UninterruptibleRetryLoop(m_client);
            while (retryLoop.shouldContinue()) {
                try {
                    node = internalElement(true, null);
                    retryLoop.markComplete();
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            ZKExceptionHandler.handle(e);
        }

        if (node == null) {
            throw new NoSuchElementException();
        }
        return node;
    }

    @Override
    public T poll() {
        try {
            return remove();
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    @Override
    public T element() {
        T node = null;

        try {
            UninterruptibleRetryLoop retryLoop = new UninterruptibleRetryLoop(m_client);
            while (retryLoop.shouldContinue()) {
                try {
                    node = internalElement(false, null);
                    retryLoop.markComplete();
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            ZKExceptionHandler.handle(e);
        }

        if (node == null) {
            throw new NoSuchElementException();
        }
        return node;
    }

    @Override
    public T peek() {
        try {
            return element();
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    @Override
    public int size() {
        int size = 0;
        try {
            UninterruptibleRetryLoop retryLoop = new UninterruptibleRetryLoop(m_client);
            while (retryLoop.shouldContinue()) {
                try {
                    List<String> nodes;
                    try {
                        nodes = m_client.getChildren().forPath(m_path);
                    } catch (KeeperException.NoNodeException ignore) {
                        // No queue, no nodes
                        return 0;
                    }

                    size = nodes.size();
                    retryLoop.markComplete();
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            ZKExceptionHandler.handle(e);
        }

        return size;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public Iterator<T> iterator() {
        try {
            UninterruptibleRetryLoop retryLoop = new UninterruptibleRetryLoop(m_client);
            while (retryLoop.shouldContinue()) {
                try {
                    List<T> els = internalAllElements(false);
                    return els.iterator();
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            ZKExceptionHandler.handle(e);
        }

        // Should never make it here
        return null;
    }

    @Override
    public Object[] toArray() {
        try {
            UninterruptibleRetryLoop retryLoop = new UninterruptibleRetryLoop(m_client);
            while (retryLoop.shouldContinue()) {
                try {
                    List<T> els = internalAllElements(false);
                    return els.toArray();
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            ZKExceptionHandler.handle(e);
        }

        // Should never make it here
        return null;
    }

    @Override
    public <W> W[] toArray(W[] a) {
        try {
            UninterruptibleRetryLoop retryLoop = new UninterruptibleRetryLoop(m_client);
            while (retryLoop.shouldContinue()) {
                try {
                    List<T> els = internalAllElements(false);
                    return els.toArray(a);
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            ZKExceptionHandler.handle(e);
        }

        // Should never make it here
        return null;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        try {
            UninterruptibleRetryLoop retryLoop = new UninterruptibleRetryLoop(m_client);
            while (retryLoop.shouldContinue()) {
                try {
                    List<T> els = internalAllElements(false);
                    return els.containsAll(c);
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            ZKExceptionHandler.handle(e);
        }
        return false;
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        for (T el : c) {
            add(el);
        }
        return c.size() > 0;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean removeAll(Collection<?> c) {
        boolean didRemove = false;
        try {
            UninterruptibleRetryLoop retryLoop = new UninterruptibleRetryLoop(m_client);
            while (retryLoop.shouldContinue()) {
                try {
                    List<String> nodePaths = getNodePaths();
                    for (String nodePath : nodePaths) {
                        try {
                            T el = (T) SerializationUtils.objFromBytes(m_client.getData().forPath(nodePath));
                            if (c.contains(el)) {
                                m_client.delete().forPath(nodePath);
                                didRemove = true;
                            }
                        } catch (KeeperException.NoNodeException ignore) {
                            // Another client removed the node first, try next
                        }
                    }
                    retryLoop.markComplete();
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            ZKExceptionHandler.handle(e);
        }

        return didRemove;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean retainAll(Collection<?> c) {
        boolean didRemove = false;
        try {
            UninterruptibleRetryLoop retryLoop = new UninterruptibleRetryLoop(m_client);
            while (retryLoop.shouldContinue()) {
                try {
                    List<String> nodePaths = getNodePaths();
                    for (String nodePath : nodePaths) {
                        try {
                            T el = (T) SerializationUtils.objFromBytes(m_client.getData().forPath(nodePath));
                            if (!c.contains(el)) {
                                m_client.delete().forPath(nodePath);
                                didRemove = true;
                            }
                        } catch (KeeperException.NoNodeException ignore) {
                            // Another client removed the node first, try next
                        }
                    }
                    retryLoop.markComplete();
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            ZKExceptionHandler.handle(e);
        }

        return didRemove;
    }

    @Override
    public void clear() {
        try {
            UninterruptibleRetryLoop retryLoop = new UninterruptibleRetryLoop(m_client);
            while (retryLoop.shouldContinue()) {
                try {
                    // Delete the queue recursively
                    ZKUtil.deleteRecursive(m_client.getZookeeperClient().getZooKeeper(),
                                           m_path);
                    retryLoop.markComplete();
                } catch (KeeperException.NoNodeException ignore) {
                    // Already cleared
                    retryLoop.markComplete();
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            ZKExceptionHandler.handle(e);
        }
    }

    @Override
    public boolean add(T el) {
        if (el == null) {
            throw new NullPointerException("The element cannot be null.");
        }

        try {
            UninterruptibleRetryLoop retryLoop = new UninterruptibleRetryLoop(m_client);
            while (retryLoop.shouldContinue()) {
                try {
                    EnsurePath ensurePath = m_client.newNamespaceAwareEnsurePath(m_path);
                    ensurePath.ensure(m_client.getZookeeperClient());

                    String nodePath = ZKPaths.makePath(m_path, NODE_PREFIX);
                    m_client.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(nodePath,
                                                                                         SerializationUtils.objToBytes(el));

                    retryLoop.markComplete();
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            ZKExceptionHandler.handle(e);
        }

        return true;
    }

    @Override
    public boolean offer(final T e) {
        return add(e);
    }

    @Override
    public void put(final T e) throws InterruptedException {
        add(e);
    }

    @Override
    public boolean offer(final T e, final long timeout, final TimeUnit unit) {
        return add(e);
    }

    @Override
    public T take() throws InterruptedException {
        try {
            RetryLoop retryLoop = m_client.getZookeeperClient().newRetryLoop();
            while (retryLoop.shouldContinue()) {
                try {
                    return internalPoll(0, null);
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            ZKExceptionHandler.handle(e);
        }

        // Should never make it here
        return null;
    }

    @Override
    public T poll(final long timeout, final TimeUnit unit)
            throws InterruptedException {
        try {
            RetryLoop retryLoop = m_client.getZookeeperClient().newRetryLoop();
            while (retryLoop.shouldContinue()) {
                try {
                    return internalPoll(timeout, unit);
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            ZKExceptionHandler.handle(e);
        }

        // Should never make it here
        return null;
    }

    @Override
    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean remove(Object o) {
        try {
            UninterruptibleRetryLoop retryLoop = new UninterruptibleRetryLoop(m_client);
            while (retryLoop.shouldContinue()) {
                try {
                    List<String> nodePaths = getNodePaths();
                    for (String nodePath : nodePaths) {
                        try {
                            T el = (T) SerializationUtils.objFromBytes(m_client.getData().forPath(nodePath));
                            if (o.equals(el)) {
                                m_client.delete().forPath(nodePath);
                                return true;
                            }
                        } catch (KeeperException.NoNodeException ignore) {
                            // Another client removed the node first, try next
                        }
                    }
                    retryLoop.markComplete();
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            ZKExceptionHandler.handle(e);
        }

        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean contains(Object o) {
        try {
            UninterruptibleRetryLoop retryLoop = new UninterruptibleRetryLoop(m_client);
            while (retryLoop.shouldContinue()) {
                try {
                    List<String> nodePaths = getNodePaths();
                    for (String nodePath : nodePaths) {
                        try {
                            T el = (T) SerializationUtils.objFromBytes(m_client.getData().forPath(nodePath));
                            if (o.equals(el)) {
                                return true;
                            }
                        } catch (KeeperException.NoNodeException ignore) {
                            // Another client removed the node first, try next
                        }
                    }
                    retryLoop.markComplete();
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            ZKExceptionHandler.handle(e);
        }

        return false;
    }

    @Override
    public int drainTo(Collection<? super T> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super T> c, int maxElements) {
        if (this == c) {
            throw new IllegalArgumentException("Cannot drain to self.");
        }

        int elementsAdded = 0;
        try {
            UninterruptibleRetryLoop retryLoop = new UninterruptibleRetryLoop(m_client);
            while (retryLoop.shouldContinue()) {
                try {
                    List<T> els = internalAllElements(true, maxElements);
                    c.addAll(els);
                    elementsAdded = els.size();
                    retryLoop.markComplete();
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            ZKExceptionHandler.handle(e);
        }

        return elementsAdded;
    }

    private T internalPoll(long timeout, TimeUnit unit) throws Exception {
        EnsurePath ensurePath = m_client.newNamespaceAwareEnsurePath(m_path);
        ensurePath.ensure(m_client.getZookeeperClient());

        long startMs = System.currentTimeMillis();
        boolean hasTimeout = (unit != null);
        long maxWaitMs = hasTimeout ? TimeUnit.MILLISECONDS.convert(timeout,
                                                                    unit)
                                   : Long.MAX_VALUE;
        for (;;) {
            final CountDownLatch latch = new CountDownLatch(1);
            Watcher watcher = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    latch.countDown();
                }
            };
            T el = internalElement(true, watcher);
            if (el != null) {
                return el;
            }

            if (hasTimeout) {
                long elapsedMs = System.currentTimeMillis() - startMs;
                long thisWaitMs = maxWaitMs - elapsedMs;
                if (thisWaitMs <= 0) {
                    return null;
                }
                latch.await(thisWaitMs, TimeUnit.MILLISECONDS);
            } else {
                latch.await();
            }
        }
    }

    private List<String> getNodePaths() throws Exception {
        return getNodePaths(null);
    }

    private List<String> getNodePaths(Watcher watcher) throws Exception {
        List<String> nodePaths = new LinkedList<String>();

        List<String> nodes;
        try {
            nodes = (watcher != null) ? m_client.getChildren().usingWatcher(watcher).forPath(m_path)
                                     : m_client.getChildren().forPath(m_path);
        } catch (KeeperException.NoNodeException dummy) {
            return nodePaths;
        }
        Collections.sort(nodes);

        for (String node : nodes) {
            if (!node.startsWith(NODE_PREFIX)) {
                LOG.warn("Foreign node in queue path: " + node);
                continue;
            }
            nodePaths.add(ZKPaths.makePath(m_path, node));
        }

        return nodePaths;
    }

    @SuppressWarnings("unchecked")
    private T internalElement(boolean removeIt, Watcher watcher)
            throws Exception {
        List<String> nodePaths = getNodePaths(watcher);
        for (String nodePath : nodePaths) {
            try {
                T el = (T) SerializationUtils.objFromBytes(m_client.getData().forPath(nodePath));
                if (removeIt) {
                    m_client.delete().forPath(nodePath);
                }
                return el;
            } catch (KeeperException.NoNodeException ignore) {
                // Another client removed the node first, try next
            }
        }

        return null;
    }

    private List<T> internalAllElements(boolean removeThem) throws Exception {
        return internalAllElements(removeThem, Integer.MAX_VALUE);
    }

    @SuppressWarnings("unchecked")
    private List<T> internalAllElements(boolean removeThem, int maxElements)
            throws Exception {
        List<T> els = new LinkedList<T>();
        List<String> nodePaths = getNodePaths();
        for (String nodePath : nodePaths) {
            try {
                T el = (T) SerializationUtils.objFromBytes(m_client.getData().forPath(nodePath));
                els.add(el);
                if (removeThem) {
                    m_client.delete().forPath(nodePath);
                }

                if (els.size() == maxElements) {
                    break;
                }
            } catch (KeeperException.NoNodeException ignore) {
                // Another client removed the node first, try next
            }
        }
        return els;
    }
}
