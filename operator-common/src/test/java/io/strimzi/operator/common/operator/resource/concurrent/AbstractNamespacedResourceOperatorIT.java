/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource.concurrent;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.k8s.cluster.KubeCluster;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
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

    private static KubeClusterResource cluster;
    protected static Executor asyncExecutor;
    protected static ResourceSupport resourceSupport;

    @BeforeEach
    public void renameResource() {
        this.resourceName = getResourceName(RESOURCE_NAME);
    }

    @BeforeAll
    public static void before() {
        cluster = KubeClusterResource.getInstance();
        cluster.setNamespace(namespace);
        asyncExecutor = ForkJoinPool.commonPool();
        resourceSupport = new ResourceSupport(asyncExecutor);

        assertDoesNotThrow(() -> KubeCluster.bootstrap(), "Could not bootstrap server");
        client = new KubernetesClientBuilder().build();

        if (cluster.getNamespace() != null && System.getenv("SKIP_TEARDOWN") == null) {
            LOGGER.warn("Namespace {} is already created, going to delete it", namespace);
            kubeClient().deleteNamespace(namespace);
            cmdKubeClient().waitForResourceDeletion("Namespace", namespace);
        }

        LOGGER.info("Creating namespace: {}", namespace);
        kubeClient().createNamespace(namespace);
        cmdKubeClient().waitForResourceCreation("Namespace", namespace);
    }

    @AfterAll
    public static void after() {
        if (kubeClient().getNamespace(namespace) != null && System.getenv("SKIP_TEARDOWN") == null) {
            LOGGER.warn("Deleting namespace {} after tests run", namespace);
            kubeClient().deleteNamespace(namespace);
            cmdKubeClient().waitForResourceDeletion("Namespace", namespace);
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
