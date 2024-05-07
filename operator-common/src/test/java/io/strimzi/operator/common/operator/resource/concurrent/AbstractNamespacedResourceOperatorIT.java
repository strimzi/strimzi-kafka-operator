/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource.concurrent;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.cluster.KubeCluster;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Objects;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The main purpose of the Integration Tests for the operators is to test them against a real Kubernetes cluster.
 * Real Kubernetes cluster has often some quirks such as some fields being immutable, some fields in the spec section
 * being created by the Kubernetes API etc. These things are hard to test with mocks. These IT tests make it easy to
 * test them against real clusters.
 */
public abstract class AbstractNamespacedResourceOperatorIT<
        C extends KubernetesClient,
        T extends HasMetadata,
        L extends KubernetesResourceList<T>,
        R extends Resource<T>> {

    protected static final Logger LOGGER = LogManager.getLogger(AbstractNamespacedResourceOperatorIT.class);
    public static final String RESOURCE_NAME = "my-test-resource";

    protected String resourceName;
    protected static KubernetesClient client;
    protected static String namespace = "resource-operator-it-namespace";

    protected static Executor asyncExecutor;
    protected static ResourceSupport resourceSupport;

    @BeforeEach
    public void renameResource() {
        this.resourceName = getResourceName(RESOURCE_NAME);
    }

    @BeforeAll
    public static void before() {
        asyncExecutor = ForkJoinPool.commonPool();
        resourceSupport = new ResourceSupport(asyncExecutor);

        assertDoesNotThrow(() -> KubeCluster.bootstrap(), "Could not bootstrap server");
        client = new KubernetesClientBuilder().build();

        if (client.namespaces().withName(namespace).get() != null && System.getenv("SKIP_TEARDOWN") == null) {
            LOGGER.warn("Namespace {} is already created, going to delete it", namespace);
            client.namespaces().withName(namespace).withPropagationPolicy(DeletionPropagation.BACKGROUND).delete();
            client.namespaces().withName(namespace).waitUntilCondition(Objects::isNull, 30_000, TimeUnit.MILLISECONDS);
        }

        LOGGER.info("Creating namespace: {}", namespace);
        client.namespaces().resource(new NamespaceBuilder().withNewMetadata().withName(namespace).endMetadata().build()).create();
        client.namespaces().withName(namespace).waitUntilCondition(ns -> ns.getStatus() != null && "Active".equals(ns.getStatus().getPhase()), 30_000, TimeUnit.MILLISECONDS);
    }

    @AfterAll
    public static void after() {
        if (client.namespaces().withName(namespace).get() != null && System.getenv("SKIP_TEARDOWN") == null) {
            LOGGER.warn("Deleting namespace {} after tests run", namespace);
            client.namespaces().withName(namespace).withPropagationPolicy(DeletionPropagation.BACKGROUND).delete();
            client.namespaces().withName(namespace).waitUntilCondition(Objects::isNull, 30_000, TimeUnit.MILLISECONDS);
        }
    }

    abstract AbstractNamespacedResourceOperator<C, T, L, R> operator();
    abstract T getOriginal();
    abstract T getModified();
    abstract void assertResources(T expected, T actual);

    @Test
    public void testCreateModifyDelete() {
        AbstractNamespacedResourceOperator<C, T, L, R> op = operator();
        T newResource = getOriginal();
        T modResource = getModified();

        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, newResource)
            .whenComplete((rrCreated, error) -> {
                assertNull(error);
                T created = op.get(namespace, resourceName);
                assertThat(created, is(notNullValue()));
                assertResources(newResource, created);
            })
            .thenCompose(rr -> op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, modResource))
            .whenComplete((rrModified, error) -> {
                assertNull(error);
                T modified = op.get(namespace, resourceName);
                assertThat(modified, is(notNullValue()));
                assertResources(modResource, modified);
            })
            .thenCompose(rr -> op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, null))
            .whenComplete(TestUtils::assertSuccessful)
            .thenCompose(rr -> resourceSupport.waitFor(
                    Reconciliation.DUMMY_RECONCILIATION,
                    "resource deletion " + resourceName,
                    "deleted",
                    1000,
                    60_000,
                    () -> op.get(namespace, resourceName) == null))
            .thenRun(() -> {
                assertThat(op.get(namespace, resourceName), is(nullValue()));
            });
    }

    protected String getResourceName(String name) {
        return name + "-" + new Random().nextInt(Integer.MAX_VALUE);
    }
}
