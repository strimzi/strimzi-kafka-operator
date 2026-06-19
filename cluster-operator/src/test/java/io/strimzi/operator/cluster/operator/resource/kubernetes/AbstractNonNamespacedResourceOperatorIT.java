/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.concurrent.AbstractNonNamespacedResourceOperator;
import io.strimzi.operator.common.operator.resource.concurrent.ResourceSupport;
import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The main purpose of the Integration Tests for the operators is to test them against a real Kubernetes cluster.
 * Real Kubernetes cluster has often some quirks such as some fields being immutable, some fields in the spec section
 * being created by the Kubernetes API etc. These things are hard to test with mocks. These IT tests make it easy to
 * test them against real clusters.
 */
public abstract class AbstractNonNamespacedResourceOperatorIT<C extends KubernetesClient,
        T extends HasMetadata,
        L extends KubernetesResourceList<T>,
        R extends Resource<T>> {
    public static final String RESOURCE_NAME = "my-resource";
    protected String resourceName;
    protected static KubernetesClient client;

    protected static Executor asyncExecutor;
    protected static ResourceSupport resourceSupport;

    @BeforeAll
    public static void before() {
        asyncExecutor = ForkJoinPool.commonPool();
        resourceSupport = new ResourceSupport(asyncExecutor);
        client = new KubernetesClientBuilder().build();
    }

    @BeforeEach
    public void renameResource() {
        resourceName = getResourceName(RESOURCE_NAME);
    }

    @AfterAll
    public static void after() {
    }

    abstract AbstractNonNamespacedResourceOperator<C, T, L, R> operator();
    abstract T getOriginal();
    abstract T getModified();
    abstract void assertResources(T expected, T actual);

    @Test
    public void testCreateModifyDelete() {
        AbstractNonNamespacedResourceOperator<C, T, L, R> op = operator();

        T newResource = getOriginal();
        T modResource = getModified();

        TestUtils.await(op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resourceName, newResource)
            .whenComplete((rrCreate, error) -> {
                assertNull(error);
                T created = op.get(resourceName);

                assertThat("Failed to get created Resource", created, is(notNullValue()));
                assertResources(newResource, created);
            })
            .thenCompose(rr -> op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resourceName, modResource))
            .whenComplete((rrModified, error) -> {
                assertNull(error);
                T modified = op.get(resourceName);

                assertThat("Failed to get modified Resource", modified, is(notNullValue()));
                assertResources(modResource, modified);
            })
            .thenCompose(rr -> op.reconcile(Reconciliation.DUMMY_RECONCILIATION, resourceName, null))
            .whenComplete(TestUtils::assertSuccessful)
            .thenCompose(rr -> resourceSupport.waitFor(
                    Reconciliation.DUMMY_RECONCILIATION,
                    "resource deletion " + resourceName,
                    "deleted",
                    1000,
                    30_000,
                    () -> op.get(resourceName) == null))
            .thenRun(() -> assertThat(op.get(resourceName), is(nullValue()))));
    }

    protected String getResourceName(String name) {
        return name + "-" + new Random().nextInt(Integer.MAX_VALUE);
    }
}
