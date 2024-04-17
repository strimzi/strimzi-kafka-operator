/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource.concurrent;

import io.fabric8.kubernetes.api.model.DefaultKubernetesResourceList;
import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.common.ConditionBuilder;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.k8s.cluster.KubeCluster;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * The main purpose of the Integration Tests for the operators is to test them against a real Kubernetes cluster.
 * Real Kubernetes cluster has often some quirks such as some fields being immutable, some fields in the spec section
 * being created by the Kubernetes API etc. These things are hard to test with mocks. These IT tests make it easy to
 * test them against real clusters.
 */
// TestInstance lifecycle set to per class so that @BeforeAll and @AfterAll methods are non-static.
// Methods must be non-static as they make a non-static call to getCrd()
// to correctly set up the test environment before the tests.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractCustomResourceOperatorIT<
            C extends KubernetesClient,
            T extends CustomResource<?, ?>,
            L extends DefaultKubernetesResourceList<T>> {

    protected static final Logger LOGGER = LogManager.getLogger(AbstractCustomResourceOperatorIT.class);
    protected static final String RESOURCE_NAME = "my-test-resource";
    protected static final Condition READY_CONDITION = new ConditionBuilder()
            .withType("Ready")
            .withStatus("True")
            .build();

    protected static KubernetesClient client;

    protected abstract CrdOperator<C, T, L> operator();
    protected abstract String getCrd();
    protected abstract String getCrdName();
    protected abstract String getNamespace();
    protected abstract T getResource(String name);
    protected abstract T getResourceWithModifications(T resourceInCluster);
    protected abstract T getResourceWithNewReadyStatus(T resourceInCluster);
    protected abstract void assertReady(T modifiedCustomResource);

    @BeforeAll
    public void before() {
        String namespace = getNamespace();
        KubeClusterResource cluster = KubeClusterResource.getInstance();
        cluster.setNamespace(namespace);

        assertDoesNotThrow(KubeCluster::bootstrap, "Could not bootstrap server");
        client = new KubernetesClientBuilder().build();

        if (client.namespaces().withName(namespace).get() != null && System.getenv("SKIP_TEARDOWN") == null) {
            LOGGER.warn("Namespace {} is already created, going to delete it", namespace);
            client.namespaces().withName(namespace).withPropagationPolicy(DeletionPropagation.BACKGROUND).delete();
            client.namespaces().withName(namespace).waitUntilCondition(Objects::isNull, 30_000, TimeUnit.MILLISECONDS);
        }

        LOGGER.info("Creating namespace: {}", namespace);
        client.namespaces().resource(new NamespaceBuilder().withNewMetadata().withName(namespace).endMetadata().build()).create();
        client.namespaces().withName(namespace).waitUntilCondition(ns -> ns.getStatus() != null && "Active".equals(ns.getStatus().getPhase()), 30_000, TimeUnit.MILLISECONDS);

        LOGGER.info("Creating CRD");
        cluster.createCustomResources(getCrd());
        cluster.waitForCustomResourceDefinition(getCrdName());
        LOGGER.info("Created CRD");
    }

    @AfterAll
    public void after() {
        String namespace = getNamespace();
        if (client.namespaces().withName(namespace).get() != null && System.getenv("SKIP_TEARDOWN") == null) {
            LOGGER.warn("Deleting namespace {} after tests run", namespace);
            client.namespaces().withName(namespace).withPropagationPolicy(DeletionPropagation.BACKGROUND).delete();
            client.namespaces().withName(namespace).waitUntilCondition(Objects::isNull, 30_000, TimeUnit.MILLISECONDS);
        }
    }

    @Test
    public void testUpdateStatus() {
        String resourceName = getResourceName(RESOURCE_NAME);
        String namespace = getNamespace();
        CrdOperator<C, T, L> op = operator();
        LOGGER.info("Creating resource");

        TestUtils.await(op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, getResource(resourceName))
            .whenComplete(TestUtils::assertSuccessful)
            .thenCompose(rrCreated -> {
                T newStatus = getResourceWithNewReadyStatus(rrCreated.resource());

                LOGGER.info("Updating resource status");
                return op.updateStatusAsync(Reconciliation.DUMMY_RECONCILIATION, newStatus);
            })
            .thenCompose(rrModified -> op.getAsync(namespace, resourceName))
            .whenComplete((modifiedCustomResource, error) -> assertReady(modifiedCustomResource))
            .thenCompose(rrModified -> {
                LOGGER.info("Deleting resource");
                return op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, null);
            }));
    }

    /**
     * Tests what happens when the resource is deleted while updating the status.
     *
     * The CR removal does not consistently complete within the default timeout.
     * This requires increasing the timeout for completion to 1 minute.
     */
    @Test
    public void testUpdateStatusAfterResourceDeletedThrowsKubernetesClientException() {
        String resourceName = getResourceName(RESOURCE_NAME);
        String namespace = getNamespace();
        CrdOperator<C, T, L> op = operator();
        AtomicReference<T> newStatus = new AtomicReference<>();

        LOGGER.info("Creating resource");
        TestUtils.await(op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, getResource(resourceName))
            .whenComplete(TestUtils::assertSuccessful)
            .thenCompose(rr -> {
                LOGGER.info("Saving resource with status change prior to deletion");
                newStatus.set(getResourceWithNewReadyStatus(op.get(namespace, resourceName)));
                LOGGER.info("Deleting resource");
                return op.deleteAsync(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, false);
            })
            .thenCompose(i -> {
                LOGGER.info("Updating resource with new status - should fail");
                return op.updateStatusAsync(Reconciliation.DUMMY_RECONCILIATION, newStatus.get());
            })
            .<Void>handle((i, error) -> {
                assertThat(Util.unwrap(error), instanceOf(KubernetesClientException.class));
                return null;
            }),
            1,
            TimeUnit.MINUTES);
    }

    /**
     * Tests what happens when the resource is modified while updating the status
     */
    @Test
    public void testUpdateStatusAfterResourceUpdated() {
        String resourceName = getResourceName(RESOURCE_NAME);
        String namespace = getNamespace();
        CrdOperator<C, T, L> op = operator();

        LOGGER.info("Creating resource");
        TestUtils.await(op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, getResource(resourceName))
            .whenComplete(TestUtils::assertSuccessful)
            .thenCompose(rrCreated -> {
                T updated = getResourceWithModifications(rrCreated.resource());
                T newStatus = getResourceWithNewReadyStatus(rrCreated.resource());

                LOGGER.info("Updating resource (mocking an update due to some other reason)");
                op.operation().inNamespace(namespace).withName(resourceName).patch(updated);

                LOGGER.info("Updating resource status after underlying resource has changed");
                return op.updateStatusAsync(Reconciliation.DUMMY_RECONCILIATION, newStatus);
            })
            .<Void>handle((unused, error) -> {
                assertNotNull(error);
                LOGGER.info("Failed as expected");
                Throwable cause = Util.unwrap(error);
                assertThat(cause, instanceOf(KubernetesClientException.class));
                assertThat(((KubernetesClientException) cause).getCode(), is(409));
                return null;
            })
            .thenCompose(v -> {
                LOGGER.info("Deleting resource");
                return op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, null);
            }));
    }

    protected String getResourceName(String name) {
        return name + "-" + new Random().nextInt(Integer.MAX_VALUE);
    }
}

