package org.opennms.core.grid.activemq;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Makes it easier to implement a blocking queue by leveraging
 * only a few core functions.
 *
 * @author jwhite
 */
public abstract class AbstractBlockingQueue<T> implements BlockingQueue<T> {
    public abstract Collection<T> getElements();

    @Override
    public T remove() {
        T el = poll();
        if (el != null) {
            return el;
        }

        throw new NoSuchElementException();
    }

    @Override
    public T element() {
        T el = peek();
        if (el != null) {
            return el;
        }

        throw new NoSuchElementException();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean offer(T e) {
        return add(e);
    }

    @Override
    public void put(T e) throws InterruptedException {
        add(e);
    }

    @Override
    public boolean offer(T e, long timeout, TimeUnit unit)
            throws InterruptedException {
        return add(e);
    }

    @Override
    public int remainingCapacity() {
        return -1;
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        for (T el : c) {
            add(el);
        }
        return true;
    }

    @Override
    public int drainTo(Collection<? super T> c) {
        return drainTo(c, 0);
    }

    @Override
    public int drainTo(Collection<? super T> c, int maxElements) {
        if (this == c) {
            throw new IllegalArgumentException("Cannot drain to self.");
        }

        int k = 0;
        T el;
        while ((el = poll()) != null) {
            c.add(el);
            k++;
            
            if (maxElements > 0 && k >= maxElements) {
                break;
            }
        }
        return k;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        boolean modified = false;
        for (Object o : c) {
            if (remove(o)) {
                modified = true;
            }
        }
        return modified;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        boolean modified = false;
        Collection<T> els = getElements();
        for (T el : els) {
            if (!c.contains(el)) {
                remove(el);
                modified = true;
            }
        }
        return modified;
    }

    @Override
    public Iterator<T> iterator() {
        return getElements().iterator();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return getElements().containsAll(c);
    }

    @Override
    public boolean contains(Object o) {
        return getElements().contains(o);
    }

    @Override
    public Object[] toArray() {
        return getElements().toArray();
    }

    @Override
    public <U> U[] toArray(U[] a) {
        return getElements().toArray(a);
    }
}
