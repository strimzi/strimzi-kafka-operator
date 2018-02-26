/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test;

import io.strimzi.test.k8s.KubeClient;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.k8s.OpenShift;
import org.junit.ClassRule;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.Annotatable;
import org.junit.runners.model.FrameworkField;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;
import org.junit.runners.model.TestClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.annotation.Repeatable;
import java.lang.reflect.Method;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class StrimziRunner extends BlockJUnit4ClassRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(StrimziRunner.class);

    /**
     * If env var NOTEARDOWN is set to any value then teardown for resources supported by annotations
     * won't happen. This can be useful in debugging a single test, because it leaves the cluster
     * in the state it was in when the test failed.
     */
    public static final String NOTEARDOWN = "NOTEARDOWN";

    private KubeClusterResource clusterResource;


    public StrimziRunner(Class<?> klass) throws InitializationError {
        super(klass);
    }

    @Override
    protected boolean isIgnored(FrameworkMethod child) {
        if (super.isIgnored(child)) {
            return true;
        } else {
            return isWrongClusterType(getTestClass()) || isWrongClusterType(child);
        }
    }

    private boolean isWrongClusterType(Annotatable child) {
        boolean result = child.getAnnotation(OpenShiftOnly.class) != null
                && !(clusterResource().cluster() instanceof OpenShift);
        if (result) {
            LOGGER.info("{} is @OpenShiftOnly, but the running cluster is not OpenShift: Ignoring", name(child));
        }
        return result;
    }

    @Override
    protected Statement methodBlock(FrameworkMethod method) {
        Statement statement = super.methodBlock(method);
        statement = withResources(method, statement);
        statement = withNamespaces(method, statement);
        return statement;
    }

    /**
     * Get the (possibly @Repeatable) annotations on the given element.
     * @param element
     * @param annotationType
     * @param <A>
     * @return
     */
    <A extends Annotation> List<A> annotations(Annotatable element, Class<A> annotationType) {
        final List<A> list;
        A c = element.getAnnotation(annotationType);
        if (c != null) {
            list = singletonList(c);
        } else {
            Repeatable r = annotationType.getAnnotation(Repeatable.class);
            if (r != null) {
                Class<? extends Annotation> ra = r.value();
                Annotation container = element.getAnnotation(ra);
                if (container != null) {
                    try {
                        Method value = ra.getDeclaredMethod("value");
                        list = asList((A[]) value.invoke(container));
                    } catch (ReflectiveOperationException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    list = emptyList();
                }
            } else {
                list = emptyList();
            }
        }

        return list;
    }

    static abstract class Bracket extends Statement implements Runnable {
        private final Statement statement;
        private final Thread hook = new Thread(this);
        public Bracket(Statement statement) {
            this.statement = statement;
        }
        @Override
        public void evaluate() throws Throwable {
            try {
                Runtime.getRuntime().addShutdownHook(hook);
                before();
                statement.evaluate();
            } finally {
                Runtime.getRuntime().removeShutdownHook(hook);
                runAfter();
            }

        }
        protected abstract void before();
        protected abstract void after();
        @Override
        public void run() {
            runAfter();
        }
        public void runAfter() {
            if (System.getenv(NOTEARDOWN) == null) {
                after();
            }
        }
    }

    String name(Annotatable a) {
        if (a instanceof TestClass) {
            return "class " + ((TestClass) a).getJavaClass().getSimpleName();
        } else if (a instanceof FrameworkMethod) {
            return "method " + ((FrameworkMethod) a).getName();
        } else if (a instanceof FrameworkField) {
            return "field " + ((FrameworkField) a).getName();
        } else {
            return a.toString();
        }
    }

    private Statement withResources(Annotatable element,
                                    Statement statement) {
        Statement last = statement;
        for (Resources resources : annotations(element, Resources.class)) {
            last = new Bracket(last) {
                @Override
                protected void before() {
                    // Here we record the state of the cluster
                    LOGGER.info("Creating {} resources {}", name(element), resources.value());

                    kubeClient().create(resources.value());
                }

                private KubeClient kubeClient() {
                    KubeClient client = clusterResource().client();
                    if (resources.asAdmin()) {
                        client = client.clientWithAdmin();
                    }
                    return client;
                }

                @Override
                protected void after() {
                    LOGGER.info("Deleting {} resources {}", name(element), resources.value());
                    // Here we verify the cluster is in the same state
                    kubeClient().delete(resources.value());
                }
            };
        }
        return last;
    }

    private Statement withNamespaces(Annotatable element,
                                     Statement statement) {
        Statement last = statement;
        for (Namespace namespace : annotations(element, Namespace.class)) {
            last = new Bracket(last) {
                @Override
                protected void before() {
                    LOGGER.info("Creating {} namespace {}", name(element), namespace.value());
                    clusterResource().client().createNamespace(namespace.value());
                }

                @Override
                protected void after() {
                    LOGGER.info("Deleting {} namespace {}", name(element), namespace.value());
                    clusterResource().client().deleteNamespace(namespace.value());
                }
            };
        }
        return last;
    }

    private boolean areAllChildrenIgnored() {
        for (FrameworkMethod child : getChildren()) {
            if (!isIgnored(child)) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected Statement classBlock(final RunNotifier notifier) {
        Statement statement = super.classBlock(notifier);
        TestClass testClass = getTestClass();
        if (!areAllChildrenIgnored()) {
            statement = withResources(testClass, statement);
            statement = withNamespaces(testClass, statement);
        }
        return statement;
    }

    /** Get the value of the @ClassRule-annotated KubeClusterResource field*/
    private KubeClusterResource clusterResource() {
        if (clusterResource == null) {
            List<KubeClusterResource> fieldValues = getTestClass().getAnnotatedFieldValues(null, ClassRule.class, KubeClusterResource.class);
            if (fieldValues == null || fieldValues.isEmpty()) {
                fieldValues = getTestClass().getAnnotatedMethodValues(null, ClassRule.class, KubeClusterResource.class);
            }
            if (fieldValues == null || fieldValues.isEmpty()) {
                clusterResource = new KubeClusterResource();
            } else {
                clusterResource = fieldValues.get(0);
            }
        }
        return clusterResource;
    }


}
