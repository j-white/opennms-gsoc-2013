/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2013 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2013 The OpenNMS Group, Inc.
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

package org.opennms.core.test.grid;

import java.lang.reflect.Method;

import org.opennms.core.grid.DataGridProviderFactory;
import org.opennms.core.test.grid.annotations.JUnitGrid;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.support.AbstractTestExecutionListener;

import com.hazelcast.core.Hazelcast;

public class JUnitGridExecutionListener extends AbstractTestExecutionListener {
    public void beforeTestClass(TestContext testContext) throws Exception {
        final JUnitGrid jug = findAnnotation(testContext);

        if (jug == null) {
            return;
        }

        System.setProperty("hazelcast.logging.type", "slf4j");
        System.setProperty("java.net.preferIPv4Stack", "true");
    }

    public void beforeTestMethod(TestContext testContext) throws Exception {
        final JUnitGrid jug = findAnnotation(testContext);

        if (jug == null) {
            return;
        }

        shutdownAndResetProvider();
    }

    public void afterTestMethod(TestContext testContext) throws Exception {
        final JUnitGrid jug = findAnnotation(testContext);

        if (jug == null) {
            return;
        }

        shutdownAndResetProvider();
    }

    private void shutdownAndResetProvider() {
        // Shutdown all of the instances
        Hazelcast.shutdownAll();

        // Reset the default instance - it will no longer work after being shutdown
        DataGridProviderFactory.setInstance(null);
    }

    private static JUnitGrid findAnnotation(final TestContext testContext) {
        JUnitGrid jug = null;
        final Method testMethod = testContext.getTestMethod();
        if (testMethod != null) {
            jug = testMethod.getAnnotation(JUnitGrid.class);
        }
        if (jug == null) {
            final Class<?> testClass = testContext.getTestClass();
            jug = testClass.getAnnotation(JUnitGrid.class);
        }
        return jug;
    }
}
