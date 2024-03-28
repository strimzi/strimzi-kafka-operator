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
import io.strimzi.test.TestUtils;
import io.vertx.core.Promise;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * The main purpose of the Integration Tests for the operators is to test them against a real Kubernetes cluster.
 * Real Kubernetes cluster has often some quirks such as some fields being immutable, some fields in the spec section
 * being created by the Kubernetes API etc. These things are hard to test with mocks. These IT tests make it easy to
 * test them against real clusters.
 */
@ExtendWith(VertxExtension.class)
public class StrimziPodSetCrdOperatorIT extends AbstractCustomResourceOperatorIT<KubernetesClient, StrimziPodSet, StrimziPodSetList> {
    protected static final Logger LOGGER = LogManager.getLogger(StrimziPodSetCrdOperatorIT.class);

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    protected StrimziPodSetOperator operator() {
        return new StrimziPodSetOperator(vertx, client);
    }

    @Override
    protected String getCrd() {
        return TestUtils.CRD_STRIMZI_POD_SET;
    }

    @Override
    protected String getCrdName() {
        return StrimziPodSet.CRD_NAME;
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
    protected void assertReady(VertxTestContext context, StrimziPodSet resource) {
        context.verify(() -> {
            int replicas = resource.getSpec().getPods().size();

            assertThat(resource.getStatus() != null
                    && replicas == resource.getStatus().getPods()
                    && replicas == resource.getStatus().getCurrentPods()
                    && replicas == resource.getStatus().getReadyPods(), is(true));
        });
    }

    @Test
    public void testCreateSucceeds(VertxTestContext context) {
        String resourceName = getResourceName(RESOURCE_NAME);
        Checkpoint async = context.checkpoint();
        String namespace = getNamespace();

        StrimziPodSetOperator op = operator();

        LOGGER.info("Creating resource");
        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, getResource(resourceName))
                .onComplete(context.succeeding(i -> { }))
                .compose(rrModified -> {
                    LOGGER.info("Deleting resource");
                    return op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, null);
                })
                .onComplete(context.succeeding(rrDeleted ->  async.flag()));
    }

    @Test
    public void testUpdateStatus(VertxTestContext context) {
        String resourceName = getResourceName(RESOURCE_NAME);
        Checkpoint async = context.checkpoint();
        String namespace = getNamespace();

        StrimziPodSetOperator op = operator();

        LOGGER.info("Creating resource");
        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, getResource(resourceName))
                .onComplete(context.succeeding(i -> { }))
                .compose(i -> op.getAsync(namespace, resourceName)) // We need to get it again because of the faked readiness which would cause 409 error
                .onComplete(context.succeeding(i -> { }))
                .compose(resource -> {
                    StrimziPodSet newStatus = getResourceWithNewReadyStatus(resource);

                    LOGGER.info("Updating resource status");
                    return op.updateStatusAsync(Reconciliation.DUMMY_RECONCILIATION, newStatus);
                })
                .onComplete(context.succeeding(i -> { }))

                .compose(rrModified -> op.getAsync(namespace, resourceName))
                .onComplete(context.succeeding(modifiedCustomResource -> context.verify(() -> assertReady(context, modifiedCustomResource))))

                .compose(rrModified -> {
                    LOGGER.info("Deleting resource");
                    return op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, null);
                })
                .onComplete(context.succeeding(rrDeleted ->  async.flag()));
    }

    /**
     * Tests what happens when the resource is deleted while updating the status.
     *
     * The CR removal does not consistently complete within the default timeout.
     * This requires increasing the timeout for completion to 1 minute.
     *
     * @param context Test context
     */
    @Test
    @Timeout(value = 1, timeUnit = TimeUnit.MINUTES)
    public void testUpdateStatusAfterResourceDeletedThrowsKubernetesClientException(VertxTestContext context) {
        String resourceName = getResourceName(RESOURCE_NAME);
        Checkpoint async = context.checkpoint();
        String namespace = getNamespace();

        StrimziPodSetOperator op = operator();

        AtomicReference<StrimziPodSet> newStatus = new AtomicReference<>();

        LOGGER.info("Creating resource");
        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, getResource(resourceName))
                .onComplete(context.succeeding(i -> { }))

                .compose(rr -> {
                    LOGGER.info("Saving resource with status change prior to deletion");
                    newStatus.set(getResourceWithNewReadyStatus(op.get(namespace, resourceName)));
                    LOGGER.info("Deleting resource");
                    return op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, null);
                })
                .onComplete(context.succeeding(i -> { }))
                .compose(i -> {
                    LOGGER.info("Updating resource with new status - should fail");
                    return op.updateStatusAsync(Reconciliation.DUMMY_RECONCILIATION, newStatus.get());
                })
                .onComplete(context.failing(e -> context.verify(() -> {
                    assertThat(e, instanceOf(KubernetesClientException.class));
                    async.flag();
                })));
    }

    /**
     * Tests what happens when the resource is modified while updating the status
     *
     * @param context   Test context
     */
    @Test
    public void testUpdateStatusAfterResourceUpdated(VertxTestContext context) {
        String resourceName = getResourceName(RESOURCE_NAME);
        Checkpoint async = context.checkpoint();
        String namespace = getNamespace();

        StrimziPodSetOperator op = operator();

        Promise<Void> updateStatus = Promise.promise();
        //readinessHelper(op, namespace, resourceName); // Required to be able to create the resource

        LOGGER.info("Creating resource");
        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, getResource(resourceName))
                .onComplete(context.succeeding(i -> { }))
                .compose(rrCreated -> {
                    StrimziPodSet updated = getResourceWithModifications(rrCreated.resource());
                    StrimziPodSet newStatus = getResourceWithNewReadyStatus(rrCreated.resource());

                    LOGGER.info("Updating resource (mocking an update due to some other reason)");
                    op.operation().inNamespace(namespace).withName(resourceName).patch(updated);

                    LOGGER.info("Updating resource status after underlying resource has changed");
                    return op.updateStatusAsync(Reconciliation.DUMMY_RECONCILIATION, newStatus);
                })
                .onComplete(context.failing(e -> context.verify(() -> {
                    LOGGER.info("Failed as expected");
                    assertThat(e, instanceOf(KubernetesClientException.class));
                    assertThat(((KubernetesClientException) e).getCode(), Matchers.is(409));
                    updateStatus.complete();
                })));

        updateStatus.future()
                .compose(v -> {
                    LOGGER.info("Deleting resource");
                    return op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, null);
                })
                .onComplete(context.succeeding(v -> async.flag()));
    }
}