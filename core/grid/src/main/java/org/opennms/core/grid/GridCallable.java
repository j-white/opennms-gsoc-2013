package org.opennms.core.grid;

import java.io.Serializable;
import java.util.concurrent.Callable;

public interface GridCallable<T> extends Callable<T>, Serializable {

}
