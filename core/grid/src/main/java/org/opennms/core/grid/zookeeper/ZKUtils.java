package org.opennms.core.grid.zookeeper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.concurrent.Callable;

import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.RetryLoop;

public class ZKUtils {
    public static <U> U callWithRetry(CuratorZookeeperClient client,
            Callable<U> proc) {
        try {
            return RetryLoop.callWithRetry(client, proc);
        } catch (Exception e) {
            throw new ZKException(e);
        }
    }

    public static <U> byte[] objToBytes(U o) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        try {
            try {
                out = new ObjectOutputStream(bos);
                out.writeObject(o);
                return bos.toByteArray();
            } catch (IOException e) {
                throw new ZKSerializationException(e);
            } finally {
                out.close();
                bos.close();
            }
        } catch (IOException e) {
            throw new ZKSerializationException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <U> U objFromBytes(byte[] b) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(b);
            ObjectInput in = null;
            try {
                in = new ObjectInputStream(bis);
                return (U) in.readObject();
            } catch (IOException e) {
                throw new ZKSerializationException(e);
            } catch (ClassNotFoundException e) {
                throw new ZKSerializationException(e);
            } finally {
                bis.close();
                in.close();
            }
        } catch (IOException e) {
            throw new ZKSerializationException(e);
        }
    }
}
