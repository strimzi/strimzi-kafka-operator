/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test;

import io.strimzi.test.Annotations.Namespace;
import io.strimzi.test.Annotations.Resources;
import io.strimzi.test.Extensions.StrimziExtension;
import io.strimzi.test.k8s.KubeClient;
import io.strimzi.test.k8s.KubeClusterResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.Assumptions;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;

import static java.util.Arrays.asList;

class StrimziExtensionTest {

    private static final KubeClient MOCK_KUBE_CLIENT = Mockito.mock(KubeClient.class);

    StrimziExtensionTest() throws InvocationTargetException {
    }

    public static class MockKubeClusterResource extends KubeClusterResource implements AfterEachCallback, BeforeEachCallback {
        MockKubeClusterResource() {
            super(null, null);
        }

        @Override
        public KubeClient<?> client() {
            return MOCK_KUBE_CLIENT;
        }

        @Override
        public void afterEach(ExtensionContext context) throws Exception {

        }

        @Override
        public void beforeEach(ExtensionContext context) throws Exception {

        }
    }

    @Namespace("test")
    @Resources("foo")
    @ExtendWith(StrimziExtension.class)
    static class ClsWithClusterResource {

        public static KubeClusterResource cluster = new MockKubeClusterResource() {
        };

        @Test
        void test0() {
            System.out.println("Hello");
        }

        @Namespace("different")
        @Resources("moreResources")
        @Test
        void test1() {
            System.out.println("Hello");
        }

        @Namespace("different")
        @Resources("moreResources")
        @Test
        void test2() {
            Assertions.fail("This test fails");
        }

        @BeforeEach
        void invokeBefore() {
            cluster.before();
        }
    }

    @BeforeEach
    void resetMock() {
        Mockito.reset(MOCK_KUBE_CLIENT);
    }

    @Test
    void test0() {
        Assumptions.assumeTrue(System.getenv(StrimziExtension.NOTEARDOWN) == null);

        SummaryGeneratingListener listener = new SummaryGeneratingListener();
        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                .selectors(
                        DiscoverySelectors.selectMethod(ClsWithClusterResource.class, "test0")
                )
                .build();
        Launcher launcher = LauncherFactory.create();
        launcher.registerTestExecutionListeners(listener);
        launcher.execute(request);

        if (!listener.getSummary().getFailures().isEmpty()) {
            listener.getSummary().getFailures().get(0).getException().printStackTrace();
        }
        Assertions.assertTrue(listener.getSummary().getFailures().isEmpty());

        Mockito.verify(MOCK_KUBE_CLIENT).createNamespace(ArgumentMatchers.eq("test"));
        Mockito.verify(MOCK_KUBE_CLIENT).create(ArgumentMatchers.eq("foo"));
        Mockito.verify(MOCK_KUBE_CLIENT).deleteNamespace(ArgumentMatchers.eq("test"));
        Mockito.verify(MOCK_KUBE_CLIENT).delete(ArgumentMatchers.eq("foo"));
    }

    @Test
    void test1() {
        Assumptions.assumeTrue(System.getenv(StrimziExtension.NOTEARDOWN) == null);
        SummaryGeneratingListener listener = new SummaryGeneratingListener();
        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                .selectors(
                        DiscoverySelectors.selectMethod(ClsWithClusterResource.class, "test1")
                )
                .build();

        Launcher launcher = LauncherFactory.create();
        launcher.registerTestExecutionListeners(listener);
        launcher.execute(request);

        if (!listener.getSummary().getFailures().isEmpty()) {
            listener.getSummary().getFailures().get(0).getException().printStackTrace();
        }
        Assertions.assertTrue(listener.getSummary().getFailures().isEmpty());

        Mockito.verify(MOCK_KUBE_CLIENT).createNamespace(ArgumentMatchers.eq("test"));
        Mockito.verify(MOCK_KUBE_CLIENT).create(ArgumentMatchers.eq("foo"));
        Mockito.verify(MOCK_KUBE_CLIENT).deleteNamespace(ArgumentMatchers.eq("test"));
        Mockito.verify(MOCK_KUBE_CLIENT).delete(ArgumentMatchers.eq("foo"));


        Mockito.verify(MOCK_KUBE_CLIENT, Mockito.times(1)).createNamespace(ArgumentMatchers.eq("different"));
        Mockito.verify(MOCK_KUBE_CLIENT, Mockito.times(1)).create(ArgumentMatchers.eq("moreResources"));
        Mockito.verify(MOCK_KUBE_CLIENT, Mockito.times(1)).deleteNamespace(ArgumentMatchers.eq("different"));
        Mockito.verify(MOCK_KUBE_CLIENT, Mockito.times(1)).delete(ArgumentMatchers.eq("moreResources"));
    }

    @Test
    void test2() {
        Assumptions.assumeTrue(System.getenv(StrimziExtension.NOTEARDOWN) == null);
        for (String resourceType : asList("pod", "deployment", "statefulset", "kafka")) {
            Mockito.when(MOCK_KUBE_CLIENT.list(resourceType)).thenReturn(asList(resourceType + "1", resourceType + "2"));
            Mockito.when(MOCK_KUBE_CLIENT.getResourceAsJson(resourceType, resourceType + "1")).thenReturn("Blah\nblah,\n" + resourceType + "1");
            Mockito.when(MOCK_KUBE_CLIENT.getResourceAsJson(resourceType, resourceType + "2")).thenReturn("Blah\nblah,\n" + resourceType + "2");
        }

        SummaryGeneratingListener listener = new SummaryGeneratingListener();
        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                .selectors(
                        DiscoverySelectors.selectMethod(ClsWithClusterResource.class, "test2")
                )
                .build();

        Launcher launcher = LauncherFactory.create();
        launcher.registerTestExecutionListeners(listener);
        launcher.execute(request);

        Mockito.when(MOCK_KUBE_CLIENT.logs("pod1")).thenReturn("these\nare\nthe\nlogs\nfrom\npod\n1");
        Mockito.when(MOCK_KUBE_CLIENT.logs("pod2")).thenReturn("these\nare\nthe\nlogs\nfrom\npod\n2");

        if (!listener.getSummary().getFailures().isEmpty()) {
            listener.getSummary().getFailures().get(0).getException().printStackTrace();
        }
        Assertions.assertEquals(1, listener.getSummary().getFailures().size());

        Mockito.verify(MOCK_KUBE_CLIENT, Mockito.times(1)).createNamespace(ArgumentMatchers.eq("test"));
        Mockito.verify(MOCK_KUBE_CLIENT, Mockito.times(1)).create(ArgumentMatchers.eq("foo"));
        Mockito.verify(MOCK_KUBE_CLIENT, Mockito.times(1)).deleteNamespace(ArgumentMatchers.eq("test"));
        Mockito.verify(MOCK_KUBE_CLIENT, Mockito.times(1)).delete(ArgumentMatchers.eq("foo"));
    }
}

