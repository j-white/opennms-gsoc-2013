package org.opennms.core.grid.hazelcast;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.hazelcast.core.IQueue;
import com.hazelcast.core.ItemListener;
import com.hazelcast.monitor.LocalQueueStats;

/**
 * Wrapper class for the IQueue that adds interrupt support
 * to the take() method.
 *
 * @author jwhite
 */
public class HazelcastQueue<T> implements IQueue<T> {
    private final IQueue<T> m_queue;
    private final BlockingQueue<T> m_localQueue = new LinkedBlockingQueue<T>();

    public HazelcastQueue(IQueue<T> queue) {
        m_queue = queue;
    }

    @Override
    public Object getId() {
        return m_queue.getId();
    }

    @Override
    public String getName() {
        return m_queue.getName();
    }

    @Override
    public void destroy() {
        m_queue.destroy();
    }

    @Override
    public String addItemListener(ItemListener<T> listener,
            boolean includeValue) {
        return m_queue.addItemListener(listener, includeValue);
    }

    @Override
    public boolean removeItemListener(String registrationId) {
        return m_queue.removeItemListener(registrationId);
    }

    @Override
    public boolean add(T e) {
        return m_queue.add(e);
    }

    @Override
    public boolean offer(T e) {
        return m_queue.offer(e);
    }

    @Override
    public void put(T e) throws InterruptedException {
        m_queue.put(e);
    }

    @Override
    public boolean offer(T e, long timeout, TimeUnit unit)
            throws InterruptedException {
        return m_queue.offer(e, timeout, unit);
    }

    @Override
    public T take() throws InterruptedException {
        /*
         * Terrible hack that allows this method to properly respond
         * to interrupt requests.
         */
        final AtomicBoolean done =  new AtomicBoolean();
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                for (;;) {
                    try {
                        if (done.get()) {
                            return;
                        }

                        T el = poll(100, TimeUnit.MILLISECONDS);
                        if (el != null) {
                            m_localQueue.add(el);
                            break;
                        }
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            }
        });

        t.start();
        try {
            return m_localQueue.take();
        } finally {
            done.set(true);
        }
    }

    @Override
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        return m_queue.poll(timeout, unit);
    }

    @Override
    public int remainingCapacity() {
        return m_queue.remainingCapacity();
    }

    @Override
    public boolean remove(Object o) {
        return m_queue.remove(o);
    }

    @Override
    public boolean contains(Object o) {
        return m_queue.contains(o);
    }

    @Override
    public int drainTo(Collection<? super T> c) {
        if (this == c) {
            throw new IllegalArgumentException("Cannot drain to self.");
        }

        return m_queue.drainTo(c);
    }

    @Override
    public int drainTo(Collection<? super T> c, int maxElements) {
        if (this == c) {
            throw new IllegalArgumentException("Cannot drain to self.");
        }

        return m_queue.drainTo(c, maxElements);
    }

    @Override
    public T remove() {
        return m_queue.remove();
    }

    @Override
    public T poll() {
        return m_queue.poll();
    }

    @Override
    public T element() {
        return m_queue.element();
    }

    @Override
    public T peek() {
        return m_queue.peek();
    }

    @Override
    public int size() {
        return m_queue.size();
    }

    @Override
    public boolean isEmpty() {
        return m_queue.isEmpty();
    }

    @Override
    public Iterator<T> iterator() {
        return m_queue.iterator();
    }

    @Override
    public Object[] toArray() {
        return m_queue.toArray();
    }

    @Override
    public <U> U[] toArray(U[] a) {
        return m_queue.toArray(a);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return m_queue.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        return m_queue.addAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return m_queue.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return m_queue.retainAll(c);
    }

    @Override
    public void clear() {
        m_queue.clear();
    }

    @Override
    public LocalQueueStats getLocalQueueStats() {
        return m_queue.getLocalQueueStats();
    }

}
