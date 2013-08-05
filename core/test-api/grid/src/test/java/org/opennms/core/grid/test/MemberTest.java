package org.opennms.core.grid.test;

import static com.jayway.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;

import java.util.concurrent.Callable;

import org.junit.Before;
import org.junit.Test;
import org.opennms.core.grid.DataGridProvider;
import org.opennms.core.grid.DataGridProviderFactory;
import org.opennms.core.grid.MembershipEvent;
import org.opennms.core.grid.MembershipListener;
import org.opennms.core.test.grid.GridTest;

public class MemberTest extends GridTest implements MembershipListener {
    /**
     * Number of members added since the last test.
     */
    private int m_membersAdded;

    /**
     * Number of members removed since the last test.
     */
    private int m_membersRemoved;

    public MemberTest()  {
        super(false);
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        m_membersAdded = 0;
        m_membersRemoved = 0;
    }

    @Test
    public void membershipListener() {
        gridProvider.addMembershipListener(this);
        await().until(getNumMembers(gridProvider), is(1));

        DataGridProvider dataGridProvider[] = new DataGridProvider[N_MEMBERS];
        for (int i = 0; i < N_MEMBERS; i++) {
            // Get a new instance as opposed to re-using the same one so that
            // multiple members can be on the cluster
            dataGridProvider[i] = DataGridProviderFactory.getNewInstance();
            dataGridProvider[i].init();
        }

        await().until(getNumMembersAdded(), is(N_MEMBERS));

        for (int i = 0; i < N_MEMBERS; i++) {
            dataGridProvider[i].shutdown();
        }

        await().until(getNumMembersRemoved(), is(N_MEMBERS));
    }

    private Callable<Integer> getNumMembersAdded() {
        return new Callable<Integer>() {
            public Integer call() throws Exception {
                return m_membersAdded;
            }
        };
    }

    private Callable<Integer> getNumMembersRemoved() {
        return new Callable<Integer>() {
            public Integer call() throws Exception {
                return m_membersRemoved;
            }
        };
    }

    @Override
    public void memberAdded(MembershipEvent membershipEvent) {
        m_membersAdded++;
    }

    @Override
    public void memberRemoved(MembershipEvent membershipEvent) {
        m_membersRemoved++;
    }
}
