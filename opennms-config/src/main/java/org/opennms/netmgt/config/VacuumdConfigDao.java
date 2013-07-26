package org.opennms.netmgt.config;

import java.util.Collection;

import org.opennms.netmgt.config.vacuumd.Action;
import org.opennms.netmgt.config.vacuumd.ActionEvent;
import org.opennms.netmgt.config.vacuumd.Automation;
import org.opennms.netmgt.config.vacuumd.Trigger;

public interface VacuumdConfigDao extends Gridable {

    /**
     * Returns a Collection of automations defined in the config
     * 
     * @return a {@link java.util.Collection} object.
     */
    public Collection<Automation> getAutomations();

    /**
     * Returns a Collection of triggers defined in the config
     * 
     * @return a {@link java.util.Collection} object.
     */
    public Collection<Trigger> getTriggers();

    /**
     * Returns a Collection of actions defined in the config
     * 
     * @return a {@link java.util.Collection} object.
     */
    public Collection<Action> getActions();

    /**
     * Returns a Collection of named events to that may have been configured
     * to be sent after an automation has run.
     * 
     * @return a {@link java.util.Collection} object.
     */
    public Collection<ActionEvent> getActionEvents();

    /**
     * Returns an Automation with a name matching the string parameter
     * 
     * @param autoName
     *            a {@link java.lang.String} object.
     * @return a {@link org.opennms.netmgt.config.vacuumd.Automation} object.
     */
    public Automation getAutomation(String autoName);

    /**
     * Returns a Trigger with a name matching the string parameter
     * 
     * @param triggerName
     *            a {@link java.lang.String} object.
     * @return a {@link org.opennms.netmgt.config.vacuumd.Trigger} object.
     */
    public Trigger getTrigger(String triggerName);

    /**
     * Returns an Action with a name matching the string parameter
     * 
     * @param actionName
     *            a {@link java.lang.String} object.
     * @return a {@link org.opennms.netmgt.config.vacuumd.Action} object.
     */
    public Action getAction(String actionName);

    /**
     * <p>
     * getActionEvent
     * </p>
     * 
     * @param name
     *            a {@link java.lang.String} object.
     * @return a {@link org.opennms.netmgt.config.vacuumd.ActionEvent} object.
     */
    public ActionEvent getActionEvent(String name);
}
