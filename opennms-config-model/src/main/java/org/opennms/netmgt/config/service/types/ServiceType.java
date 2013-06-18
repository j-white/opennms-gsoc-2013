package org.opennms.netmgt.config.service.types;

import java.io.Serializable;
import java.util.Enumeration;
import java.util.Hashtable;

public class ServiceType implements Serializable {
    private static final long serialVersionUID = -2213062924726257869L;

    /**
     * The vanilla type
     */
    public static final int VANILLA_TYPE = 0;

    /**
     * The bootstrap type
     */
    public static final int BOOTSTRAP_TYPE = 0;

    /**
     * The distributed type
     */
    public static final int DISTRIBUTED_TYPE = 0;

    /**
     * The instance of the vanilla type
     */
    public static final ServiceType VANILLA = new ServiceType(VANILLA_TYPE,
                                                              "vanilla");

    /**
     * The instance of the bootstrap type
     */
    public static final ServiceType BOOTSTRAP = new ServiceType(
                                                                BOOTSTRAP_TYPE,
                                                                "bootstrap");

    /**
     * The instance of the distributed type
     */
    public static final ServiceType DISTRIBUTED = new ServiceType(
                                                                  DISTRIBUTED_TYPE,
                                                                  "distributed");

    /**
     * Field _memberTable.
     */
    private static Hashtable<Object, Object> _memberTable = init();

    /**
     * Field type.
     */
    private final int type;

    /**
     * Field stringValue.
     */
    private String stringValue = null;

    private ServiceType(final int type, final String value) {
        super();
        this.type = type;
        this.stringValue = value;
    }

    /**
     * Method enumerate.Returns an enumeration of all possible instances of
     * ServiceType
     * 
     * @return an Enumeration over all possible instances of ServiceType
     */
    public static Enumeration<Object> enumerate() {
        return _memberTable.elements();
    }

    /**
     * Method getType.Returns the type of this ServiceType
     * 
     * @return the type of this ServiceType
     */
    public int getType() {
        return this.type;
    }

    /**
     * Method init.
     * 
     * @return the initialized Hashtable for the member table
     */
    private static Hashtable<Object, Object> init() {
        Hashtable<Object, Object> members = new Hashtable<Object, Object>();
        members.put(VANILLA.stringValue, VANILLA);
        members.put(BOOTSTRAP.stringValue, BOOTSTRAP);
        members.put(DISTRIBUTED.stringValue, DISTRIBUTED);
        return members;
    }

    /**
     * Method readResolve. will be called during deserialization to replace
     * the deserialized object with the correct constant instance.
     * 
     * @return this deserialized object
     */
    private Object readResolve() {
        return valueOf(this.stringValue);
    }

    /**
     * Method toString.Returns the String representation of this ServiceType
     * 
     * @return the String representation of this ServiceType
     */
    public String toString() {
        return this.stringValue;
    }

    /**
     * Method valueOf.Returns a new ServiceType based on the given String
     * value.
     * 
     * @param string
     * @return the ServiceType value of parameter 'string'
     */
    public static ServiceType valueOf(final String string) {
        Object obj = null;
        if (string != null) {
            obj = _memberTable.get(string);
        }
        if (obj == null) {
            String err = "" + string + " is not a valid ServiceType";
            throw new IllegalArgumentException(err);
        }
        return (ServiceType) obj;
    }
}
