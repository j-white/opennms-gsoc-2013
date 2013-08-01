package org.opennms.core.grid.test;

import org.junit.Before;
import org.junit.Test;
import org.opennms.core.grid.DataGridProvider;
import org.opennms.core.test.grid.GridTest;
import org.opennms.core.utils.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;

public class AutowireTest extends GridTest {
    @Autowired
    protected DataGridProvider m_gridProvider = null;

    @Before
    public void setUp() {
        BeanUtils.assertAutowiring(this);
    }

    @Test
    public void checkAutowire() {
    }
}
