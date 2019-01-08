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
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;

import java.lang.reflect.InvocationTargetException;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectMethod;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

class StrimziExtensionTest {

    private static final KubeClient MOCK_KUBE_CLIENT = mock(KubeClient.class);

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
    public static class ClsWithClusterResource {

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
    }

    @BeforeEach
    void resetMock() {
        reset(MOCK_KUBE_CLIENT);
    }

    @Test
    void test0() {
        Assumptions.assumeTrue(System.getenv(StrimziExtension.NOTEARDOWN) == null);

        SummaryGeneratingListener listener = new SummaryGeneratingListener();
        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                .selectors(
                        selectMethod(ClsWithClusterResource.class, "test0")
                )
                .build();
        Launcher launcher = LauncherFactory.create();
        launcher.registerTestExecutionListeners(listener);
        launcher.execute(request);

        if (!listener.getSummary().getFailures().isEmpty()) {
            listener.getSummary().getFailures().get(0).getException().printStackTrace();
        }
        assertTrue(listener.getSummary().getFailures().isEmpty());

        verify(MOCK_KUBE_CLIENT).createNamespace(eq("test"));
        verify(MOCK_KUBE_CLIENT).create(eq("foo"));
        verify(MOCK_KUBE_CLIENT).deleteNamespace(eq("test"));
        verify(MOCK_KUBE_CLIENT).delete(eq("foo"));
    }

    @Test
    void test1() {
        Assumptions.assumeTrue(System.getenv(StrimziExtension.NOTEARDOWN) == null);
        SummaryGeneratingListener listener = new SummaryGeneratingListener();
        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                .selectors(
                        selectMethod(ClsWithClusterResource.class, "test1")
                )
                .build();

        Launcher launcher = LauncherFactory.create();
        launcher.registerTestExecutionListeners(listener);
        launcher.execute(request);

        if (!listener.getSummary().getFailures().isEmpty()) {
            listener.getSummary().getFailures().get(0).getException().printStackTrace();
        }
        assertTrue(listener.getSummary().getFailures().isEmpty());

        verify(MOCK_KUBE_CLIENT).createNamespace(eq("test"));
        verify(MOCK_KUBE_CLIENT).create(eq("foo"));
        verify(MOCK_KUBE_CLIENT).deleteNamespace(eq("test"));
        verify(MOCK_KUBE_CLIENT).delete(eq("foo"));


        verify(MOCK_KUBE_CLIENT, times(1)).createNamespace(eq("different"));
        verify(MOCK_KUBE_CLIENT, times(1)).create(eq("moreResources"));
        verify(MOCK_KUBE_CLIENT, times(1)).deleteNamespace(eq("different"));
        verify(MOCK_KUBE_CLIENT, times(1)).delete(eq("moreResources"));
    }

    @Test
    void test2() {
        Assumptions.assumeTrue(System.getenv(StrimziExtension.NOTEARDOWN) == null);
        for (String resourceType : asList("pod", "deployment", "statefulset", "kafka")) {
            when(MOCK_KUBE_CLIENT.list(resourceType)).thenReturn(asList(resourceType + "1", resourceType + "2"));
            when(MOCK_KUBE_CLIENT.getResourceAsJson(resourceType, resourceType + "1")).thenReturn("Blah\nblah,\n" + resourceType + "1");
            when(MOCK_KUBE_CLIENT.getResourceAsJson(resourceType, resourceType + "2")).thenReturn("Blah\nblah,\n" + resourceType + "2");
        }

        SummaryGeneratingListener listener = new SummaryGeneratingListener();
        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                .selectors(
                        selectMethod(ClsWithClusterResource.class, "test2")
                )
                .build();

        Launcher launcher = LauncherFactory.create();
        launcher.registerTestExecutionListeners(listener);
        launcher.execute(request);

        when(MOCK_KUBE_CLIENT.logs("pod1")).thenReturn("these\nare\nthe\nlogs\nfrom\npod\n1");
        when(MOCK_KUBE_CLIENT.logs("pod2")).thenReturn("these\nare\nthe\nlogs\nfrom\npod\n2");

        if (!listener.getSummary().getFailures().isEmpty()) {
            listener.getSummary().getFailures().get(0).getException().printStackTrace();
        }
        assertEquals(1, listener.getSummary().getFailures().size());

        verify(MOCK_KUBE_CLIENT, times(1)).createNamespace(eq("test"));
        verify(MOCK_KUBE_CLIENT, times(1)).create(eq("foo"));
        verify(MOCK_KUBE_CLIENT, times(1)).deleteNamespace(eq("test"));
        verify(MOCK_KUBE_CLIENT, times(1)).delete(eq("foo"));
    }
}

