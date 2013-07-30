package org.opennms.core.test.grid;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Modifier;
import java.util.List;

import org.junit.runners.Suite;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.RunnerBuilder;
import org.junit.runners.model.TestClass;
import org.opennms.core.grid.DataGridProvider;

public class SuiteWithType extends Suite {

    public SuiteWithType(Class<?> klass, RunnerBuilder builder)
            throws Throwable {
        super(klass, builder);
        Class<? extends DataGridProvider> gridClazz = getGridType(getTestClass());
        System.setProperty("gridClazz", gridClazz.getName());
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public static @interface GridType {
    }

    @SuppressWarnings("unchecked")
    private Class<? extends DataGridProvider> getGridType(TestClass klass)
                    throws Throwable {
            return (Class<? extends DataGridProvider>)getGridTypeMethod(klass).invokeExplosively(
                            null);
    }

    private FrameworkMethod getGridTypeMethod(TestClass testClass)
                    throws Exception {
            List<FrameworkMethod> methods= testClass
                            .getAnnotatedMethods(GridType.class);
            for (FrameworkMethod each : methods) {
                    int modifiers= each.getMethod().getModifiers();
                    if (Modifier.isStatic(modifiers) && Modifier.isPublic(modifiers))
                            return each;
            }

            throw new Exception("No public static parameters method on class "
                            + testClass.getName());
    }
}
