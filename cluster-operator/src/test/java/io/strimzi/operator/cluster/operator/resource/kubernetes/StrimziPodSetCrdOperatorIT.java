/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.api.kafka.model.podset.StrimziPodSetBuilder;
import io.strimzi.api.kafka.model.podset.StrimziPodSetList;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.kubernetes.AbstractCustomResourceOperatorIT;
import io.strimzi.test.CrdUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * The main purpose of the Integration Tests for the operators is to test them against a real Kubernetes cluster.
 * Real Kubernetes cluster has often some quirks such as some fields being immutable, some fields in the spec section
 * being created by the Kubernetes API etc. These things are hard to test with mocks. These IT tests make it easy to
 * test them against real clusters.
 */
public class StrimziPodSetCrdOperatorIT extends AbstractCustomResourceOperatorIT<KubernetesClient, StrimziPodSet, StrimziPodSetList> {
    protected static final Logger LOGGER = LogManager.getLogger(StrimziPodSetCrdOperatorIT.class);

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    protected StrimziPodSetOperator operator() {
        return new StrimziPodSetOperator(asyncExecutor, client);
    }

    @Override
    protected String getCrd() {
        return CrdUtils.CRD_STRIMZI_POD_SET;
    }

    @Override
    protected String getCrdName() {
        return CrdUtils.CRD_STRIMZI_POD_SET_NAME;
    }

    @Override
    protected String getNamespace() {
        return "strimzipodset-crd-it-namespace";
    }

    @SuppressWarnings("unchecked")
    @Override
    protected StrimziPodSet getResource(String resourceName) {
        Pod pod = new PodBuilder()
                .withNewMetadata()
                    .withName("broker")
                    .withLabels(Map.of("role", "broker"))
                .endMetadata()
                .withNewSpec()
                    .withContainers(new ContainerBuilder().withName("broker").withImage("kafka:latest").build())
                .endSpec()
                .build();

        return new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName(resourceName)
                    .withNamespace(getNamespace())
                .endMetadata()
                .withNewSpec()
                    .withSelector(new LabelSelectorBuilder().withMatchLabels(Map.of("role", "broker")).build())
                    .withPods(mapper.convertValue(pod, new TypeReference<Map<String, Object>>() { }))
                .endSpec()
                .withNewStatus()
                    .withPods(1)
                    .withCurrentPods(1)
                    .withReadyPods(1)
                .endStatus()
                .build();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected StrimziPodSet getResourceWithModifications(StrimziPodSet resourceInCluster) {
        Pod pod = new PodBuilder()
                .withNewMetadata()
                    .withName("broker2")
                    .withLabels(Map.of("role", "broker"))
                .endMetadata()
                .withNewSpec()
                    .withContainers(new ContainerBuilder().withName("broker").withImage("kafka:latest").build())
                .endSpec()
                .build();

        return new StrimziPodSetBuilder(resourceInCluster)
                .editSpec()
                    .addToPods(mapper.convertValue(pod, new TypeReference<Map<String, Object>>() { }))
                .endSpec()
                .build();

    }

    @Override
    protected StrimziPodSet getResourceWithNewReadyStatus(StrimziPodSet resourceInCluster) {
        return new StrimziPodSetBuilder(resourceInCluster)
                .withNewStatus()
                    .withPods(1)
                    .withCurrentPods(1)
                    .withReadyPods(1)
                .endStatus()
                .build();
    }

    @Override
    protected void assertReady(StrimziPodSet resource) {
        int replicas = resource.getSpec().getPods().size();

        assertThat(resource.getStatus() != null
                && replicas == resource.getStatus().getPods()
                && replicas == resource.getStatus().getCurrentPods()
                && replicas == resource.getStatus().getReadyPods(), is(true));
    }

    @Test
    public void testCreateSucceeds() {
        String resourceName = getResourceName(RESOURCE_NAME);
        String namespace = getNamespace();

        StrimziPodSetOperator op = operator();

        LOGGER.info("Creating resource");
        TestUtils.await(op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, getResource(resourceName))
                .whenComplete(TestUtils::assertSuccessful)
                .thenCompose(rrModified -> {
                    LOGGER.info("Deleting resource");
                    return op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, null);
                }));
    }

    @Test
    public void testUpdateStatus() {
        String resourceName = getResourceName(RESOURCE_NAME);
        String namespace = getNamespace();

        StrimziPodSetOperator op = operator();

        LOGGER.info("Creating resource");
        TestUtils.await(op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, getResource(resourceName))
                .whenComplete(TestUtils::assertSuccessful)
                .thenCompose(i -> op.getAsync(namespace, resourceName)) // We need to get it again because of the faked readiness which would cause 409 error
                .thenCompose(resource -> {
                    StrimziPodSet newStatus = getResourceWithNewReadyStatus(resource);

                    LOGGER.info("Updating resource status");
                    return op.updateStatusAsync(Reconciliation.DUMMY_RECONCILIATION, newStatus);
                })
                .whenComplete(TestUtils::assertSuccessful)
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

        StrimziPodSetOperator op = operator();

        AtomicReference<StrimziPodSet> newStatus = new AtomicReference<>();

        LOGGER.info("Creating resource");
        TestUtils.await(op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, getResource(resourceName))
                        .whenComplete(TestUtils::assertSuccessful)
                        .thenCompose(rr -> {
                            LOGGER.info("Saving resource with status change prior to deletion");
                            newStatus.set(getResourceWithNewReadyStatus(op.get(namespace, resourceName)));
                            LOGGER.info("Deleting resource");
                            return op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, null);
                        })
                        .whenComplete(TestUtils::assertSuccessful)
                        .thenCompose(i -> {
                            LOGGER.info("Updating resource with new status - should fail");
                            return op.updateStatusAsync(Reconciliation.DUMMY_RECONCILIATION, newStatus.get());
                        })
                        .whenComplete(TestUtils::assertSuccessful)
                        .<Void>handle((i, error) -> {
                            assertThat(error, is(notNullValue()));
                            assertThat(error, is(instanceOf(CompletionException.class)));
                            assertThat(error.getCause(), is(instanceOf(KubernetesClientException.class)));
                            return null;
                        }),
                        1, TimeUnit.MINUTES);
    }

    /**
     * Tests what happens when the resource is modified while updating the status
     */
    @Test
    public void testUpdateStatusAfterResourceUpdated() {
        String resourceName = getResourceName(RESOURCE_NAME);
        String namespace = getNamespace();

        StrimziPodSetOperator op = operator();

        LOGGER.info("Creating resource");
        TestUtils.await(op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, getResource(resourceName))
                .whenComplete(TestUtils::assertSuccessful)
                .thenCompose(rrCreated -> {
                    StrimziPodSet updated = getResourceWithModifications(rrCreated.resource());
                    StrimziPodSet newStatus = getResourceWithNewReadyStatus(rrCreated.resource());

                    LOGGER.info("Updating resource (mocking an update due to some other reason)");
                    op.operation().inNamespace(namespace).withName(resourceName).patch(updated);

                    LOGGER.info("Updating resource status after underlying resource has changed");
                    return op.updateStatusAsync(Reconciliation.DUMMY_RECONCILIATION, newStatus);
                })
                .<Void>handle((unused, error) -> {
                    assertNotNull(error);
                    LOGGER.info("Failed as expected");
                    assertThat(error, is(notNullValue()));
                    assertThat(error, is(instanceOf(CompletionException.class)));
                    assertThat(error.getCause(), is(instanceOf(KubernetesClientException.class)));
                    assertThat(((KubernetesClientException) error.getCause()).getCode(), is(409));
                    return null;
                })
                .thenCompose(v -> {
                    LOGGER.info("Deleting resource");
                    return op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, null);
                }));
    }
}
