package org.opennms.netmgt.config;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.security.MessageDigest;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HashUtils {

    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(HashUtils.class);

    public static String getId(Serializable object) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;

        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(object);

            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(bos.toByteArray());

            byte[] mdbytes = md.digest();

            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < mdbytes.length; i++) {
                sb.append(Integer.toString((mdbytes[i] & 0xff) + 0x100, 16).substring(1));
            }

            return sb.toString();
        } catch (Throwable e) {
            LOG.error("Failed to calculate the md5 sum.", e);
        } finally {
            IOUtils.closeQuietly(bos);
            bos = null;

            if (out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                    // be quiet
                } finally {
                    out = null;
                }
            }
        }

        return "" + object.hashCode();
    }
}
