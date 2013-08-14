package org.opennms.core.grid.zookeeper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

/**
 * Convenience methods for encoding/decoding an object to a byte array.
 *
 * @author jwhite
 */
public class SerializationUtils {

    /**
     * Serializes on object into a byte array.
     *
     * @param o
     *          the object to serialize
     * @return
     *          a byte array
     * @throws
     *          a ZKSerializationException if anything goes wrong
     */
    public static byte[] objToBytes(Object o) {
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

    /**
     * Deserializes a byte array into an object.
     *
     * @param b
     *          a byte array
     * @return
     *          the object stored in the byte array
     * @throws
     *          a ZKSerializationException if anything goes wrong
     */
    public static Object objFromBytes(byte[] b) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(b);
            ObjectInput in = null;
            try {
                in = new ObjectInputStream(bis);
                return in.readObject();
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
