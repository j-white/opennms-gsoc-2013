/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2012 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2012 The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is a registered trademark of The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * OpenNMS(R) is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with OpenNMS(R).  If not, see:
 *      http://www.gnu.org/licenses/
 *
 * For more information contact:
 *     OpenNMS(R) Licensing <license@opennms.org>
 *     http://www.opennms.org/
 *     http://www.opennms.com/
 *******************************************************************************/

package org.opennms.netmgt.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;

import org.apache.commons.io.IOUtils;
import org.opennms.core.utils.ConfigFileConstants;
import org.opennms.core.xml.JaxbUtils;
import org.opennms.netmgt.config.vacuumd.Action;
import org.opennms.netmgt.config.vacuumd.ActionEvent;
import org.opennms.netmgt.config.vacuumd.Automation;
import org.opennms.netmgt.config.vacuumd.Trigger;
import org.opennms.netmgt.config.vacuumd.VacuumdConfiguration;
import org.springframework.util.Assert;

/**
 * This is the singleton class used to load the configuration for the OpenNMS
 * Vacuumd process from the vacuumd-configuration xml file.
 * 
 * <strong>Note: </strong>Users of this class should make sure the
 * <em>setReader()</em> method is called before calling any other method to
 * ensure the config is loaded before accessing other convenience methods.
 * 
 * @author <a href="mailto:david@opennms.com">David Hustace </a>
 * @author <a href="mailto:brozow@opennms.com">Mathew Brozowski </a>
 * @author <a href="http://www.opennms.org/">OpenNMS </a>
 * @author <a href="mailto:david@opennms.com">David Hustace </a>
 * @author <a href="mailto:brozow@opennms.com">Mathew Brozowski </a>
 * @author <a href="http://www.opennms.org/">OpenNMS </a>
 * @author <a href="mailto:david@opennms.com">David Hustace </a>
 * @author <a href="mailto:brozow@opennms.com">Mathew Brozowski </a>
 * @author <a href="http://www.opennms.org/">OpenNMS </a>
 * @version $Id: $
 */
public final class VacuumdConfigFactory implements VacuumdConfigDao {

    /**
     * The singleton instance of this factory
     */
    private static VacuumdConfigFactory m_singleton = null;

    private static boolean m_loadedFromFile = false;

    private static final long serialVersionUID = 5695526138379612483L;

    /**
     * The config class loaded from the config file
     */
    private VacuumdConfiguration m_config;

    /**
     * <p>
     * Constructor for VacuumdConfigFactory.
     * </p>
     * 
     * @param stream
     *            a {@link java.io.InputStream} object.
     */
    public VacuumdConfigFactory(InputStream stream) {
        m_config = JaxbUtils.unmarshal(VacuumdConfiguration.class,
                                       new InputStreamReader(stream));
    }

    /**
     * <p>
     * Constructor for VacuumdConfigFactory.
     * </p>
     * 
     * Calling reload() on a instance created with method will have no effect.
     * 
     * @param config
     *            The configuration the use.
     */
    public VacuumdConfigFactory(VacuumdConfiguration config) {
        m_config = config;
        m_loadedFromFile = false;
    }

    /**
     * Load the config from the default config file and create the singleton
     * instance of this factory.
     * 
     * @exception java.io.IOException
     *                Thrown if the specified config file cannot be read
     * @throws java.io.IOException
     *             if any.
     */
    public static synchronized void init() throws IOException {
        if (m_singleton != null) {
            /*
             * The init method has already called, so return. To reload,
             * reload() will need to be called.
             */
            return;
        }

        InputStream is = null;

        try {
            is = new FileInputStream(
                                     ConfigFileConstants.getFile(ConfigFileConstants.VACUUMD_CONFIG_FILE_NAME));
            setInstance(new VacuumdConfigFactory(is));
        } finally {
            if (is != null) {
                IOUtils.closeQuietly(is);
            }
        }

        m_loadedFromFile = true;
    }

    /**
     * Reload the config from the default config file
     * 
     * @exception java.io.IOException
     *                Thrown if the specified config file cannot be
     *                read/loaded
     * @throws java.io.IOException
     *             if any.
     */
    public static synchronized void reload() throws IOException {
        if (m_loadedFromFile) {
            setInstance(null);

            init();
        }
    }

    /**
     * Return the singleton instance of this factory.
     * 
     * @return The current factory instance.
     * @throws java.lang.IllegalStateException
     *             Thrown if the factory has not yet been initialized.
     */
    public static synchronized VacuumdConfigDao getInstance() {
        Assert.state(m_singleton != null,
                     "The factory has not been initialized");

        return m_singleton;
    }

    /**
     * Set the singleton instance of this factory.
     * 
     * @param instance
     *            The factory instance to set.
     */
    public static synchronized void setInstance(VacuumdConfigFactory instance) {
        m_singleton = instance;
    }

    /** {@inheritDoc} */
    @Override
    public synchronized Collection<Automation> getAutomations() {
        return m_config.getAutomations().getAutomationCollection();
    }

    /** {@inheritDoc} */
    @Override
    public synchronized Collection<Trigger> getTriggers() {
        return m_config.getTriggers().getTriggerCollection();
    }

    /** {@inheritDoc} */
    @Override
    public synchronized Collection<Action> getActions() {
        return m_config.getActions().getActionCollection();
    }

    /** {@inheritDoc} */
    @Override
    public synchronized Collection<ActionEvent> getActionEvents() {
        return m_config.getActionEvents().getActionEventCollection();
    }

    /** {@inheritDoc} */
    @Override
    public synchronized Automation getAutomation(String autoName) {
        for (Automation auto : getAutomations()) {
            if (auto.getName().equals(autoName)) {
                return auto;
            }
        }
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public synchronized Trigger getTrigger(String triggerName) {
        for (Trigger trig : getTriggers()) {
            if (trig.getName().equals(triggerName)) {
                return trig;
            }
        }
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public synchronized Action getAction(String actionName) {
        for (Action act : getActions()) {
            if (act.getName().equals(actionName)) {
                return act;
            }
        }
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public ActionEvent getActionEvent(String name) {
        for (ActionEvent actionEvent : getActionEvents()) {
            if (actionEvent.getName().equals(name)) {
                return actionEvent;
            }
        }
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public String getUniqueId() {
        return HashUtils.getId(m_config);
    }
}
