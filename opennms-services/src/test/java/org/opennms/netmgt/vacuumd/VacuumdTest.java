package org.opennms.netmgt.vacuumd;

import org.junit.Before;
import org.junit.Test;
import org.opennms.netmgt.config.VacuumdConfigFactory;
import org.opennms.netmgt.config.vacuumd.VacuumdConfiguration;
import org.opennms.netmgt.eventd.mock.MockEventIpcManager;

public class VacuumdTest {
    MockEventIpcManager m_mockEventIpcManager;

    @Before
    public void setUp() {
        m_mockEventIpcManager = new MockEventIpcManager();
    }
    
    @Test
    public void trivialStartStop() {
        VacuumdConfiguration vacuumdConfig = new VacuumdConfiguration();
        useVacuumdConfig(vacuumdConfig);

        Vacuumd vacuumd = new Vacuumd();
        vacuumd.setEventManager(m_mockEventIpcManager);
        vacuumd.init();
        vacuumd.start();
        vacuumd.stop();
    }

    private static void useVacuumdConfig(VacuumdConfiguration vacuumdConfig) {
        VacuumdConfigFactory vacuumdConfigFactory = new VacuumdConfigFactory(vacuumdConfig);
        VacuumdConfigFactory.setInstance(vacuumdConfigFactory);
    }
}
