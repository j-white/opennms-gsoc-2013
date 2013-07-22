package org.opennms.netmgt.scheduler;

import java.io.Serializable;

import org.opennms.netmgt.scheduler.ReadyRunnable;

public interface ClusterRunnable extends ReadyRunnable, Serializable, SchedulerAware {
}
