package org.opennms.core.grid.activemq;

import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;

public class CamelBlockingQueue<T> extends LinkedBlockingQueue<T> {
    private static final long serialVersionUID = 3328779235302788507L;

    @Override
    public boolean add(T e) {
        return super.add(e);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        return super.addAll(c);
    }

    @Override
    public void put(T e) throws InterruptedException {
        super.put(e);
    }
}
