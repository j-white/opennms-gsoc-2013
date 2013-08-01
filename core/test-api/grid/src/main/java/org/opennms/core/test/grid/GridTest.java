package org.opennms.core.test.grid;

import static com.jayway.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;

import java.util.concurrent.Callable;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.opennms.core.grid.DataGridProvider;
import org.opennms.core.test.MockLogAppender;
import org.opennms.core.test.OpenNMSJUnit4ClassRunner;
import org.opennms.core.test.grid.annotations.JUnitGrid;
import org.springframework.test.context.ContextConfiguration;

@RunWith(OpenNMSJUnit4ClassRunner.class)
@ContextConfiguration(locations = {
        "classpath:/META-INF/opennms/applicationContext-soa.xml",
        "classpath:/META-INF/opennms/component-grid-test.xml" })
@JUnitGrid()
public class GridTest extends TestCase {
    public final int N_MEMBERS;
    public static final int N_MEMBERS_DEFAULT = 3;
    public static DataGridProvider gridProvider;

    public GridTest() {
        N_MEMBERS = N_MEMBERS_DEFAULT;
    }

    public GridTest(final int numMembers) {
        N_MEMBERS = numMembers;
    }

    @BeforeClass
    public static void onlyOnce() {
        org.junit.Assume.assumeNotNull(System.getProperty("gridClazz"));
        gridProvider = getGridProvider();
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockLogAppender.setupLogging(true, "DEBUG");
        if (!gridProvider.isRunning()) {
            gridProvider = getGridProvider();
        }
        await().until(getNumMembers(gridProvider), is(1));
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        MockLogAppender.assertNoErrorOrGreater();
    }

    @SuppressWarnings("unchecked")
    protected static Class<? extends DataGridProvider> getGridClazz() {
        try {
            return (Class<? extends DataGridProvider>) Class.forName(System.getProperty("gridClazz"));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static DataGridProvider getGridProvider() {
        try {
            return getGridClazz().newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public Callable<Integer> getNumMembers(
            final DataGridProvider gridProvider) {
        return new Callable<Integer>() {
            public Integer call() throws Exception {
                return gridProvider.getGridMembers().size();
            }
        };
    }
}
