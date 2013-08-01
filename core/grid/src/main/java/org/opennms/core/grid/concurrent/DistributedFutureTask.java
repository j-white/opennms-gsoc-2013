package org.opennms.core.grid.concurrent;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.opennms.core.grid.DataGridProvider;
import org.opennms.core.grid.DataGridProviderAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedFutureTask<T> implements RunnableFuture<T>,
        DataGridProviderAware, Serializable {

    private static final long serialVersionUID = 7197998814461179867L;
    private static final Logger LOG = LoggerFactory.getLogger(DistributedFutureTask.class);
    private final Callable<T> m_callable;
    private final String m_uuid;

    private transient DataGridProvider m_dataGridProvider;
    private final String m_resultMapName;
    private transient Map<String, FutureTaskResult<T>> m_resultMap;

    public static class FutureTaskResult<T> implements Serializable {
        private static final long serialVersionUID = -5941393311128359245L;
        public T result;
        public Throwable ex;

        public String toString() {
            return "TaskResult[result='" + result + "', ex='" + ex + "']";
        }
    }

    public DistributedFutureTask(Callable<T> callable,
            DataGridProvider dataGridProvider, String resultMapName) {
        m_callable = callable;
        m_uuid = genUUID();

        m_dataGridProvider = dataGridProvider;
        m_resultMapName = resultMapName;
        m_resultMap = m_dataGridProvider.getMap(m_resultMapName);
    }

    public DistributedFutureTask(Runnable runnable, T result,
            DataGridProvider dataGridProvider, String resultMapName) {
        m_callable = Executors.callable(runnable, result);
        m_uuid = genUUID();

        m_dataGridProvider = dataGridProvider;
        m_resultMapName = resultMapName;
        m_resultMap = m_dataGridProvider.getMap(m_resultMapName);
    }

    private String genUUID() {
        return UUID.randomUUID().toString();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return m_resultMap.containsKey(m_uuid);
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        Map<String, FutureTaskResult<T>> resultMap = m_dataGridProvider.getMap(m_resultMapName);
        while (true) {
            if (resultMap.containsKey(m_uuid)) {
                FutureTaskResult<T> tr = resultMap.get(m_uuid);
                if (tr.ex != null) {
                    throw new ExecutionException(tr.ex);
                }
                return tr.result;
            }
            // TODO: better
            Thread.sleep(50);
        }
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException,
            ExecutionException, TimeoutException {
        return get();
    }

    @Override
    public void run() {
        FutureTaskResult<T> tr = new FutureTaskResult<T>();
        try {
            tr.result = m_callable.call();
        } catch (Throwable ex) {
            tr.ex = ex;
        }
        LOG.debug("{} returned {}", tr, m_uuid);
        m_dataGridProvider.getMap(m_resultMapName).put(m_uuid, tr);
    }

    @Override
    public void setDataGridProvider(DataGridProvider dataGridProvider) {
        m_dataGridProvider = dataGridProvider;
    }
}
