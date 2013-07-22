package org.opennms.netmgt.config.vacuumd;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.opennms.core.xml.JaxbUtils;

public class VacuumdLegacyConversions {

    @Test
    public void statementToAutomation() throws Exception {
        String statementSql = "SELECT * FROM events;";
        String vacuumdConfigXml = "<VacuumdConfiguration period=\"1\">"
                + "<statement>" + statementSql + "</statement>"
                + "<automations/>" + "<triggers/>" + "<actions/>"
                + "<auto-events/>" + "<action-events/>"
                + "</VacuumdConfiguration>";

        VacuumdConfiguration vacuumdConfig = JaxbUtils.unmarshal(VacuumdConfiguration.class,
                                                                 vacuumdConfigXml);

        assertEquals(0, vacuumdConfig.getStatementCount());
        assertEquals(1, vacuumdConfig.getAutomations().getAutomationCount());
        assertEquals(1, vacuumdConfig.getActions().getActionCount());
        Action action = vacuumdConfig.getActions().getAction(0);
        assertEquals(statementSql, action.getStatement().getContent());
    }

    @Test
    public void autoEventToActionEvent() throws Exception {
        String eventUei = "uei.opennms.org/vacuumd/alarmEscalated";
        String vacuumdConfigXml = "<VacuumdConfiguration period=\"1\">"
                + "<automations>"
                + "  <automation name=\"escalate\" interval=\"60000\" active=\"true\""
                + "     auto-event-name=\"escalationEvent\" />"
                + "</automations>" + "<triggers/>" + "<actions/>"
                + "<auto-events>"
                + "  <auto-event name=\"escalationEvent\" >" + "     <uei>"
                + eventUei + "</uei>" + "  </auto-event>" + "</auto-events>"
                + "<action-events/>" + "</VacuumdConfiguration>";

        VacuumdConfiguration vacuumdConfig = JaxbUtils.unmarshal(VacuumdConfiguration.class,
                                                                 vacuumdConfigXml);

        assertEquals(0, vacuumdConfig.getAutoEvents().getAutoEventCount());
        assertEquals(1, vacuumdConfig.getActionEvents().getActionEventCount());
        ActionEvent actionEvent = vacuumdConfig.getActionEvents().getActionEvent(0);
        assertEquals(eventUei, actionEvent.getAssignment(0).getValue());
    }
}
