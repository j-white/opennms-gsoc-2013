package org.opennms.core.grid.zookeeper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import org.apache.curator.RetryLoop;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.EnsurePath;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;

import static org.opennms.core.grid.zookeeper.ZKUtils.objToBytes;
import static org.opennms.core.grid.zookeeper.ZKUtils.objFromBytes;

/**
 * Stores a map in a ZooKeeper tree: /onms/map/<name>/<hash>/key-<seq#>/value
 * 
 * The first level stores the hash-codes, the second level stores the keys and
 * the third level stores the values.
 * 
 * A distributed lock is used to synchronize operations that modify the maps
 * contents. No lock is used for read operations.
 * 
 * @author jwhite
 */
public class ZKMap<K, V> implements Map<K, V> {
    public static final String PATH_PREFIX = "/onms/map/";
    private static final String KEY_PREFIX = "key-";
    private static final String VALUE_SUFFIX = "val";

    private final CuratorFramework m_client;
    private final String m_path;
    private final Lock m_lock;

    public ZKMap(CuratorFramework client, String name) {
        m_client = client;
        m_path = ZKPaths.makePath(PATH_PREFIX, name);
        m_lock = new ZKLock(client, "internal.map." + name);
    }

    @Override
    public int size() {
        int numEls = 0;

        try {
            RetryLoop retryLoop = m_client.getZookeeperClient().newRetryLoop();
            while (retryLoop.shouldContinue()) {
                try {
                    numEls = getKeyPaths().size();
                    retryLoop.markComplete();
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            throw new ZKException(e);
        }

        return numEls;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        try {
            RetryLoop retryLoop = m_client.getZookeeperClient().newRetryLoop();
            while (retryLoop.shouldContinue()) {
                try {
                    List<String> keyPaths = getKeyPaths();
                    for (String keyPath : keyPaths) {
                        try {
                            if (key.equals(getKey(keyPath))) {
                                return true;
                            }
                        } catch (KeeperException.NoNodeException ignore) {
                            // Next
                        }
                    }
                    retryLoop.markComplete();
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            throw new ZKException(e);
        }

        return false;
    }

    @Override
    public boolean containsValue(Object value) {
        try {
            RetryLoop retryLoop = m_client.getZookeeperClient().newRetryLoop();
            while (retryLoop.shouldContinue()) {
                try {
                    List<String> keyPaths = getKeyPaths();
                    for (String keyPath : keyPaths) {
                        try {
                            if (value.equals(getValue(keyPath))) {
                                return true;
                            }
                        } catch (KeeperException.NoNodeException ignore) {
                            // Next
                        }
                    }
                    retryLoop.markComplete();
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            throw new ZKException(e);
        }

        return false;
    }

    @Override
    public V get(final Object key) {
        try {
            RetryLoop retryLoop = m_client.getZookeeperClient().newRetryLoop();
            while (retryLoop.shouldContinue()) {
                try {
                    List<String> keyPaths = getKeyPaths(key);
                    for (String keyPath : keyPaths) {
                        try {
                            if (key.equals(getKey(keyPath))) {
                                return getValue(keyPath);
                            }
                        } catch (KeeperException.NoNodeException ignore) {
                            // Next
                        }
                    }
                    retryLoop.markComplete();
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            throw new ZKException(e);
        }

        return null;
    }

    @Override
    public V put(final K key, final V value) {
        m_lock.lock();
        try {
            return putNoLock(key, value);
        } catch (Exception e) {
            throw new ZKException(e);
        } finally {
            m_lock.unlock();
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        m_lock.lock();
        try {
            for (Entry<? extends K, ? extends V> entry : m.entrySet()) {
                putNoLock(entry.getKey(), entry.getValue());
            }
        } catch (Exception e) {
            throw new ZKException(e);
        } finally {
            m_lock.unlock();
        }
    }

    private V putNoLock(final K key, final V value) throws Exception {
        RetryLoop retryLoop = m_client.getZookeeperClient().newRetryLoop();
        while (retryLoop.shouldContinue()) {
            try {
                // Search the existing keys
                List<String> keyPaths = getKeyPaths(key);
                for (String keyPath : keyPaths) {
                    K k = getKey(keyPath);
                    if (key.equals(k)) {
                        V last = getValue(keyPath);
                        setValue(keyPath, value);
                        return last;
                    }
                }

                // No existing keys found, add a new one
                addKeyValue(key, value);
                retryLoop.markComplete();
            } catch (Exception e) {
                retryLoop.takeException(e);
            }
        }

        return null;
    }

    @Override
    public V remove(final Object key) {
        m_lock.lock();
        try {
            RetryLoop retryLoop = m_client.getZookeeperClient().newRetryLoop();
            while (retryLoop.shouldContinue()) {
                try {
                    List<String> keyPaths = getKeyPaths(key);
                    for (String keyPath : keyPaths) {
                        K k = getKey(keyPath);
                        if (key.equals(k)) {
                            // Fetch the value
                            V val = getValue(keyPath);

                            // Delete the key recursively
                            ZKUtil.deleteRecursive(m_client.getZookeeperClient().getZooKeeper(),
                                                   keyPath);

                            return val;
                        }
                    }
                    retryLoop.markComplete();
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            throw new ZKException(e);
        } finally {
            m_lock.unlock();
        }
        return null;
    }

    @Override
    public void clear() {
        m_lock.lock();
        try {
            RetryLoop retryLoop = m_client.getZookeeperClient().newRetryLoop();
            while (retryLoop.shouldContinue()) {
                try {
                    // Delete the map recursively
                    ZKUtil.deleteRecursive(m_client.getZookeeperClient().getZooKeeper(),
                                           m_path);
                    retryLoop.markComplete();
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            throw new ZKException(e);
        } finally {
            m_lock.unlock();
        }
    }

    @Override
    public Set<K> keySet() {
        Set<K> keySet = new HashSet<K>();

        try {
            RetryLoop retryLoop = m_client.getZookeeperClient().newRetryLoop();
            while (retryLoop.shouldContinue()) {
                try {
                    List<String> keyPaths = getKeyPaths();
                    for (String keyPath : keyPaths) {
                        try {
                            keySet.add(getKey(keyPath));
                        } catch (KeeperException.NoNodeException ignore) {
                            // Next
                        }
                    }
                    retryLoop.markComplete();
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            throw new ZKException(e);
        }

        return keySet;
    }

    @Override
    public Collection<V> values() {
        List<V> values = new LinkedList<V>();

        try {
            RetryLoop retryLoop = m_client.getZookeeperClient().newRetryLoop();
            while (retryLoop.shouldContinue()) {
                try {
                    List<String> keyPaths = getKeyPaths();
                    for (String keyPath : keyPaths) {
                        try {
                            values.add(getValue(keyPath));
                        } catch (KeeperException.NoNodeException ignore) {
                            // Next
                        }
                    }
                    retryLoop.markComplete();
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            throw new ZKException(e);
        }

        return values;
    }

    @Override
    public Set<java.util.Map.Entry<K, V>> entrySet() {
        Set<java.util.Map.Entry<K, V>> entries = new HashSet<java.util.Map.Entry<K, V>>();

        try {
            RetryLoop retryLoop = m_client.getZookeeperClient().newRetryLoop();
            while (retryLoop.shouldContinue()) {
                try {
                    List<String> keyPaths = getKeyPaths();
                    for (String keyPath : keyPaths) {
                        try {
                            K key = getKey(keyPath);
                            V val = getValue(keyPath);
                            entries.add(new ZKMapEntry(key, val));
                        } catch (KeeperException.NoNodeException ignore) {
                            // Next
                        }
                    }
                    retryLoop.markComplete();
                } catch (Exception e) {
                    retryLoop.takeException(e);
                }
            }
        } catch (Exception e) {
            throw new ZKException(e);
        }

        return entries;
    }

    private String getHashPath(Object key) {
        return ZKPaths.makePath(m_path, "" + key.hashCode());
    }

    private K getKey(String keyPath) throws Exception {
        return objFromBytes(m_client.getData().forPath(keyPath));
    }

    private V getValue(String keyPath) throws Exception {
        return objFromBytes(m_client.getData().forPath((ZKPaths.makePath(keyPath,
                                                                         VALUE_SUFFIX))));
    }

    private void setValue(String keyPath, V value) throws Exception {
        m_client.setData().forPath(ZKPaths.makePath(keyPath, VALUE_SUFFIX),
                                   objToBytes(value));
    }

    private void addKeyValue(K key, V value) throws Exception {
        String hashPath = getHashPath(key);
        EnsurePath ensurePath = m_client.newNamespaceAwareEnsurePath(hashPath);
        ensurePath.ensure(m_client.getZookeeperClient());

        String keyPath = ZKPaths.makePath(hashPath, KEY_PREFIX);
        keyPath = m_client.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(keyPath,
                                                                                       objToBytes(key));

        String valuePath = ZKPaths.makePath(keyPath, VALUE_SUFFIX);
        m_client.create().withMode(CreateMode.PERSISTENT).forPath(valuePath,
                                                                  objToBytes(value));
    }

    private List<String> getHashPaths() throws Exception {
        List<String> hashPaths = new LinkedList<String>();
        try {
            List<String> hashNodes = m_client.getChildren().forPath(m_path);
            Collections.sort(hashNodes);
            for (String hashNode : hashNodes) {
                hashPaths.add(ZKPaths.makePath(m_path, hashNode));
            }
        } catch (KeeperException.NoNodeException ignore) {
            // No map, no entries
            return new ArrayList<String>(0);
        }
        return hashPaths;
    }

    private List<String> getKeyPaths() throws Exception {
        List<String> keyPaths = new LinkedList<String>();
        List<String> hashPaths = getHashPaths();
        for (String hashPath : hashPaths) {
            try {
                List<String> keyNodes = m_client.getChildren().forPath(hashPath);
                for (String keyNode : keyNodes) {
                    keyPaths.add(ZKPaths.makePath(hashPath, keyNode));
                }
            } catch (KeeperException.NoNodeException ignore) {
                // Another client removed the entry first, try next
            }
        }
        return keyPaths;
    }

    private List<String> getKeyPaths(Object key) throws Exception {
        List<String> keyPaths = new LinkedList<String>();
        List<String> keysForHash;
        String hashPath = getHashPath(key);
        try {
            keysForHash = m_client.getChildren().forPath(hashPath);
        } catch (KeeperException.NoNodeException ignore) {
            // No keys here
            return keyPaths;
        }

        for (String keyNode : keysForHash) {
            keyPaths.add(ZKPaths.makePath(hashPath, keyNode));
        }

        return keyPaths;
    }

    private class ZKMapEntry implements Entry<K, V> {
        private final K m_key;
        private final V m_val;

        public ZKMapEntry(K key, V val) {
            m_key = key;
            m_val = val;
        }

        @Override
        public K getKey() {
            return m_key;
        }

        @Override
        public V getValue() {
            return m_val;
        }

        @Override
        public V setValue(V value) {
            return ZKMap.this.put(m_key, value);
        }
    }
}
