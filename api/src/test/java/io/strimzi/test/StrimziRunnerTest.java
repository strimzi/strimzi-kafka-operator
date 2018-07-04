/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test;

import io.strimzi.test.k8s.KubeClient;
import io.strimzi.test.k8s.KubeClusterResource;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Request;
import org.junit.runner.Result;
import org.junit.runner.RunWith;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StrimziRunnerTest {

    private static final KubeClient MOCK_KUBE_CLIENT = mock(KubeClient.class);

    static class MockKubeClusterResource extends KubeClusterResource {
        public MockKubeClusterResource() {
            super(null, null);
        }

        @Override
        public void before() {
        }

        @Override
        public void after() {
        }

        @Override
        public KubeClient<?> client() {
            return MOCK_KUBE_CLIENT;
        }
    }

    @Namespace("test")
    @Resources("foo")
    @RunWith(StrimziRunner.class)
    public static class ClsWithClusterResource {

        @ClassRule
        public static KubeClusterResource mockCluster = new MockKubeClusterResource() {

        };

        @Test
        public void test0() {
            System.out.println("Hello");
        }

        @Namespace("different")
        @Resources("moreResources")
        @Test
        public void test1() {
            System.out.println("Hello");
        }

        @Namespace("different")
        @Resources("moreResources")
        @Test
        public void test2() {
            Assert.fail("This test fails");
        }
    }

    JUnitCore jUnitCore = new JUnitCore();

    @Before
    public void resetMock() {
        reset(MOCK_KUBE_CLIENT);
    }

    @Test
    public void test0() {
        Assume.assumeTrue(System.getenv(StrimziRunner.NOTEARDOWN) == null);
        Result r =  jUnitCore.run(Request.method(ClsWithClusterResource.class, "test0"));
        if (!r.wasSuccessful()) {
            r.getFailures().get(0).getException().printStackTrace();
        }
        assertTrue(r.wasSuccessful());
        verify(MOCK_KUBE_CLIENT).createNamespace(eq("test"));
        verify(MOCK_KUBE_CLIENT).create(eq("foo"));
        verify(MOCK_KUBE_CLIENT).deleteNamespace(eq("test"));
        verify(MOCK_KUBE_CLIENT).delete(eq("foo"));
    }

    @Test
    public void test1() {
        Assume.assumeTrue(System.getenv(StrimziRunner.NOTEARDOWN) == null);
        Result r =  jUnitCore.run(Request.method(ClsWithClusterResource.class, "test1"));
        if (!r.wasSuccessful()) {
            r.getFailures().get(0).getException().printStackTrace();
        }
        assertTrue(r.wasSuccessful());
        verify(MOCK_KUBE_CLIENT, times(1)).createNamespace(eq("test"));
        verify(MOCK_KUBE_CLIENT, times(1)).create(eq("foo"));
        verify(MOCK_KUBE_CLIENT, times(1)).deleteNamespace(eq("test"));
        verify(MOCK_KUBE_CLIENT, times(1)).delete(eq("foo"));
    }

    @Test
    public void test2() {
        Assume.assumeTrue(System.getenv(StrimziRunner.NOTEARDOWN) == null);
        for (String resourceType : asList("pod", "deployment", "statefulset", "kafka")) {
            when(MOCK_KUBE_CLIENT.list(resourceType)).thenReturn(asList(resourceType + "1", resourceType + "2"));
            when(MOCK_KUBE_CLIENT.getResourceAsJson(resourceType, resourceType + "1")).thenReturn("Blah\nblah,\n" + resourceType + "1");
            when(MOCK_KUBE_CLIENT.getResourceAsJson(resourceType, resourceType + "2")).thenReturn("Blah\nblah,\n" + resourceType + "2");
        }
        when(MOCK_KUBE_CLIENT.logs("pod1")).thenReturn("these\nare\nthe\nlogs\nfrom\npod\n1");
        when(MOCK_KUBE_CLIENT.logs("pod2")).thenReturn("these\nare\nthe\nlogs\nfrom\npod\n2");

        Result r =  jUnitCore.run(Request.method(ClsWithClusterResource.class, "test2"));
        if (!r.wasSuccessful()) {
            r.getFailures().get(0).getException().printStackTrace();
        }
        assertFalse(r.wasSuccessful());
        assertEquals(1, r.getFailures().size());
        verify(MOCK_KUBE_CLIENT, times(1)).createNamespace(eq("test"));
        verify(MOCK_KUBE_CLIENT, times(1)).create(eq("foo"));
        verify(MOCK_KUBE_CLIENT, times(1)).deleteNamespace(eq("test"));
        verify(MOCK_KUBE_CLIENT, times(1)).delete(eq("foo"));
    }
}
