package org.opennms.core.grid;

import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.opennms.core.test.MockLogAppender;
import org.opennms.core.test.OpenNMSJUnit4ClassRunner;
import org.opennms.core.test.grid.annotations.JUnitGrid;
import org.opennms.core.utils.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;

@RunWith(OpenNMSJUnit4ClassRunner.class)
@ContextConfiguration(locations = {
        "classpath:/META-INF/opennms/applicationContext-soa.xml",
        "classpath:/META-INF/opennms/component-grid-test.xml" })
//@DirtiesContext
@JUnitGrid(reuseGrid = false)
public abstract class AbstractGridTest {
    public final int N_MEMBERS;
    public static final int N_MEMBERS_DEFAULT = 3;

    @Autowired
    protected DataGridProvider m_dataGridProvider = null;

    public AbstractGridTest() {
        N_MEMBERS = N_MEMBERS_DEFAULT;
    }

    public AbstractGridTest(final int numMembers) {
        N_MEMBERS = numMembers;
    }

    @BeforeClass
    public static void onlyOnce() {
        org.junit.Assume.assumeNotNull(System.getProperty("gridClazz"));
    }

    @Before
    public void setUp() {
        MockLogAppender.setupLogging(true, "DEBUG");
        BeanUtils.assertAutowiring(this);
    }

    @After
    public void tearDown() throws Exception {
        MockLogAppender.assertNoErrorOrGreater();
    }

    @SuppressWarnings("unchecked")
    public Class<? extends DataGridProvider> getGridClazz() {
        try {
            return (Class<? extends DataGridProvider>) Class.forName(System.getProperty("gridClazz"));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public DataGridProvider getGridProvider() {
        try {
            return getGridClazz().newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public Callable<Integer> getNumClusterMembers(
            final DataGridProvider dataGridProvider) {
        return new Callable<Integer>() {
            public Integer call() throws Exception {
                return dataGridProvider.getGridMembers().size();
            }
        };
    }
}
