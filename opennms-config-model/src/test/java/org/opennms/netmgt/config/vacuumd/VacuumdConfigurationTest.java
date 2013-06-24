package org.opennms.netmgt.config.vacuumd;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Collection;

import org.junit.runners.Parameterized.Parameters;
import org.opennms.core.test.xml.XmlTestNoCastor;

public class VacuumdConfigurationTest extends
        XmlTestNoCastor<VacuumdConfiguration> {

    public VacuumdConfigurationTest(final VacuumdConfiguration sampleObject,
            final String sampleXml, final String schemaFile) {
        super(sampleObject, sampleXml, schemaFile);
    }

    @Parameters
    public static Collection<Object[]> data() throws ParseException {
        return Arrays.asList(new Object[][] { {
                // Only set the required fields
                getMinimalistVacuumdConfig(),
                "<VacuumdConfiguration period=\"1\">"
                        + "<automations>"
                        + "<automation name=\"myautomation\" interval=\"100\""
                        + " action-name=\"myaction\"/>"
                        + "</automations>" + "</VacuumdConfiguration>",
                "target/classes/xsds/vacuumd-configuration.xsd", }, 
                // Set all fields
                });
    }

    private static VacuumdConfiguration getMinimalistVacuumdConfig() {
        Automations automations = new Automations();
        Automation automation = new Automation("myautomation", 100,
                                               "myaction");
        automations.addAutomation(automation);

        VacuumdConfiguration minimalistVacuumdConfig = new VacuumdConfiguration(
                                                                                1,
                                                                                automations);
        return minimalistVacuumdConfig;
    }
}
