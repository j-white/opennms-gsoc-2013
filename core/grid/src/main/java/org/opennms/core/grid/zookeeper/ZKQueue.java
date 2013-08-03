package org.opennms.core.grid.zookeeper;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.queue.SimpleDistributedQueue;

public class ZKQueue<T> implements BlockingQueue<T> {

    public static final String PATH_PREFIX = "/onms/queue/";

    private final CuratorFramework m_client;
    private final SimpleDistributedQueue m_distributedQueue;

    public ZKQueue(CuratorFramework client, String name) {
        m_client = client;
        m_distributedQueue = new SimpleDistributedQueue(client, PATH_PREFIX + name);
    }

    private T objFromBytes(byte[] bytes) {
        return null;
    }

    private byte[] bytesFromObj(T obj) {
        return null;
    }

    private <U> U callWithRetry(Callable<U> proc) {
        return ZKUtils.callWithRetry(m_client.getZookeeperClient(), proc);
    }

    @Override
    public T remove() {
        return callWithRetry(new Callable<T>() {
              @Override
              public T call() throws Exception {
                  return objFromBytes(m_distributedQueue.remove());
              }
        });
    }

    @Override
    public T poll() {
        return callWithRetry(new Callable<T>() {
            @Override
            public T call() throws Exception {
                return objFromBytes(m_distributedQueue.poll());
            }
      });
    }

    @Override
    public T element() {
        return callWithRetry(new Callable<T>() {
            @Override
            public T call() throws Exception {
                return objFromBytes(m_distributedQueue.element());
            }
        });
    }

    @Override
    public T peek() {
        return callWithRetry(new Callable<T>() {
            @Override
            public T call() throws Exception {
                return objFromBytes(m_distributedQueue.peek());
            }
        });
    }

    @Override
    public int size() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public Iterator<T> iterator() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object[] toArray() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <W> W[] toArray(W[] a) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void clear() {
        // TODO Auto-generated method stub
        
    }

    @Override
    public boolean add(T e) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean offer(final T e) {
        return callWithRetry(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return m_distributedQueue.offer(bytesFromObj(e));
            }
        });
    }

    @Override
    public void put(final T e) throws InterruptedException {
        offer(e);
    }

    @Override
    public boolean offer(final T e, final long timeout, final TimeUnit unit) {
        return offer(e);
    }

    @Override
    public T take() throws InterruptedException {
        return callWithRetry(new Callable<T>() {
            @Override
            public T call() throws Exception {
                return objFromBytes(m_distributedQueue.take());
            }
        });
    }

    @Override
    public T poll(final long timeout, final TimeUnit unit) throws InterruptedException {
        return callWithRetry(new Callable<T>() {
            @Override
            public T call() throws Exception {
                return objFromBytes(m_distributedQueue.poll(timeout, unit));
            }
        });
    }

    @Override
    public int remainingCapacity() {
        return -1;
    }

    @Override
    public boolean remove(Object o) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean contains(Object o) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int drainTo(Collection<? super T> c) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int drainTo(Collection<? super T> c, int maxElements) {
        // TODO Auto-generated method stub
        return 0;
    }
}
