package org.opennms.core.grid.zookeeper;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.EnsurePath;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;

import static org.opennms.core.grid.zookeeper.ZKUtils.objToBytes;
import static org.opennms.core.grid.zookeeper.ZKUtils.objFromBytes;

/**
 * /onms/map/<name>/<hashcode>/key-1/value
 * /onms/map/<name>/<hashcode>/key-2/value
 *
 * @author jwhite
 */
public class ZKMap<K, V> implements Map<K, V> {

    public static final String PATH_PREFIX = "/onms/map/";
    private static final String KEY_PREFIX = "key-";
    private static final String VALUE_SUFFIX = "val";

    private final CuratorFramework m_client;
    private final String m_path;
    private final EnsurePath m_ensurePath;

    public ZKMap(CuratorFramework client, String name) {
        m_client = client;
        m_path = ZKPaths.makePath(PATH_PREFIX, name);
        m_ensurePath = m_client.newNamespaceAwareEnsurePath(m_path);
    }

    private <U> U callWithRetry(Callable<U> proc) {
        return ZKUtils.callWithRetry(m_client.getZookeeperClient(), proc);
    }

    @Override
    public int size() {
        return callWithRetry(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                List<String> hashCodes;
                try {
                    hashCodes = m_client.getChildren().forPath(m_path);
                } catch (KeeperException.NoNodeException ignore) {
                    // No map - no elements
                    return 0;
                }

                int numEls = 0;
                for (String hashCode : hashCodes) {
                    String thisPath = ZKPaths.makePath(m_path, hashCode);
                    try {
                        List<String> els = m_client.getChildren().forPath(thisPath);
                        numEls += els.size();
                    } catch (KeeperException.NoNodeException ignore) {
                        // Another client removed the node first, try next
                    }
                }
                return numEls;
            }
        });
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        return keySet().contains(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return values().contains(value);
    }

    @Override
    public V get(final Object key) {
        return callWithRetry(new Callable<V>() {
            @Override
            public V call() throws Exception {
                String hashCodePath = ZKPaths.makePath(m_path, "" + key.hashCode());
                
                List<String> keyNodesForHash;
                try {
                    keyNodesForHash = m_client.getChildren().forPath(hashCodePath);
                } catch (KeeperException.NoNodeException ignore) {
                    // No keys here
                    return null;
                }

                for (String keyNode : keyNodesForHash) {
                    String keyNodePath = ZKPaths.makePath(hashCodePath, keyNode);
                    if (key.equals(objFromBytes(m_client.getData().forPath(keyNodePath)))) {
                        String valuePath = ZKPaths.makePath(keyNodePath, VALUE_SUFFIX);
                        return objFromBytes(m_client.getData().forPath(valuePath));
                    }
                }

                return null;
            }
        });
    }

    @Override
    public V put(final K key, final V value) {
        return callWithRetry(new Callable<V>() {
            @Override
            public V call() throws Exception {
                String hashCodePath = ZKPaths.makePath(m_path, "" + key.hashCode());
                EnsurePath ensurePath = m_client.newNamespaceAwareEnsurePath(hashCodePath);
                ensurePath.ensure(m_client.getZookeeperClient());

                String keyPath = ZKPaths.makePath(hashCodePath, KEY_PREFIX);
                keyPath = m_client.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(keyPath, objToBytes(key));

                String valuePath = ZKPaths.makePath(keyPath, VALUE_SUFFIX);
                m_client.create().withMode(CreateMode.PERSISTENT).forPath(valuePath, objToBytes(value));
                return value;
            }
        });
    }

    @Override
    public V remove(final Object key) {
        return callWithRetry(new Callable<V>() {
            @Override
            public V call() throws Exception {
                String hashCodePath = ZKPaths.makePath(m_path, "" + key.hashCode());

                List<String> keyNodesForHash;
                try {
                    keyNodesForHash = m_client.getChildren().forPath(hashCodePath);
                } catch (KeeperException.NoNodeException ignore) {
                    // No keys here
                    return null;
                }

                for (String keyNode : keyNodesForHash) {
                    String keyPath = ZKPaths.makePath(hashCodePath, keyNode);
                    if (key.equals(objFromBytes(m_client.getData().forPath(keyPath)))) {
                        // Fetch the value
                        String valuePath = ZKPaths.makePath(keyPath, VALUE_SUFFIX);
                        V val = objFromBytes(m_client.getData().forPath(valuePath));

                        // Delete the key recursively
                        ZKUtil.deleteRecursive(m_client.getZookeeperClient().getZooKeeper(), keyPath);
                        return val;
                    }
                }

                return null;
            }
        });
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        for (Entry<? extends K, ? extends V> entry : m.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void clear() {
        callWithRetry(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                try {
                    // Delete the key recursively
                    ZKUtil.deleteRecursive(m_client.getZookeeperClient().getZooKeeper(), m_path);
                } catch (KeeperException.NoNodeException ignore) {
                    // It's already gone
                }
                return null;
            }
        });
    }

    @Override
    public Set<K> keySet() {
        return callWithRetry(new Callable<Set<K>>() {
            @Override
            public Set<K> call() throws Exception {
                Set<K> keySet = new HashSet<K>();
                List<String> hashCodes = m_client.getChildren().forPath(m_path);
                for (String hashCode : hashCodes) {
                    String hashCodePath = ZKPaths.makePath(m_path, hashCode);
                    
                    
                    List<String> keyNodesForHash = m_client.getChildren().forPath(hashCodePath);
                    for (String keyNode : keyNodesForHash) {
                        String keyNodePath = ZKPaths.makePath(hashCodePath, keyNode);
                        K key = objFromBytes(m_client.getData().forPath(keyNodePath));
                        keySet.add(key);
                    }
                }
                return keySet;
            }
        });
    }

    @Override
    public Collection<V> values() {
        return callWithRetry(new Callable<Collection<V>>() {
            @Override
            public Collection<V> call() throws Exception {
                List<V> values = new LinkedList<V>();
                List<String> hashCodes = m_client.getChildren().forPath(m_path);
                for (String hashCode : hashCodes) {
                    String hashCodePath = ZKPaths.makePath(m_path, hashCode);
                    List<String> keyNodesForHash = m_client.getChildren().forPath(hashCodePath);
                    for (String keyNode : keyNodesForHash) {
                        // Fetch the value
                        String keyNodePath = ZKPaths.makePath(hashCodePath, keyNode);
                        String valuePath = ZKPaths.makePath(keyNodePath, VALUE_SUFFIX);
                        V val = objFromBytes(m_client.getData().forPath(valuePath));
                        values.add(val);
                    }
                }
                return values;
            }
        });
    }

    @Override
    public Set<java.util.Map.Entry<K, V>> entrySet() {
        return callWithRetry(new Callable<Set<java.util.Map.Entry<K, V>>>() {
            @Override
            public Set<java.util.Map.Entry<K, V>> call() throws Exception {
                Set<java.util.Map.Entry<K, V>> entries = new HashSet<java.util.Map.Entry<K, V>>();

                List<String> hashCodes = m_client.getChildren().forPath(m_path);
                for (String hashCode : hashCodes) {
                    String hashCodePath = ZKPaths.makePath(m_path, hashCode);
                    List<String> keyNodesForHash = m_client.getChildren().forPath(hashCodePath);
                    for (String keyNode : keyNodesForHash) {
                        // Fetch the key
                        String keyNodePath = ZKPaths.makePath(hashCodePath, keyNode);
                        K key = objFromBytes(m_client.getData().forPath(keyNodePath));

                        // Fetch the value
                        String valuePath = ZKPaths.makePath(keyNodePath, VALUE_SUFFIX);
                        V val = objFromBytes(m_client.getData().forPath(valuePath));

                        entries.add(new ZKMapEntry(key, val));
                    }
                }
                return entries;
            }
        });
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
