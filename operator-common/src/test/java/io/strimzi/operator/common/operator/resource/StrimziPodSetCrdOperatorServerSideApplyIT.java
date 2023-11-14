/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.ManagedFieldsEntry;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.api.kafka.model.StrimziPodSetBuilder;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Promise;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * The main purpose of the Integration Tests for the operators is to test them against a real Kubernetes cluster.
 * Real Kubernetes cluster has often some quirks such as some fields being immutable, some fields in the spec section
 * being created by the Kubernetes API etc. These things are hard to test with mocks. These IT tests make it easy to
 * test them against real clusters.
 */
@ExtendWith(VertxExtension.class)
public class StrimziPodSetCrdOperatorServerSideApplyIT extends StrimziPodSetCrdOperatorIT {
    protected static final Logger LOGGER = LogManager.getLogger(StrimziPodSetCrdOperatorServerSideApplyIT.class);

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    protected StrimziPodSetOperator operator() {
        return new StrimziPodSetOperator(vertx, client, true);
    }

    @Test
    public void testStrimziPodSetAnnotationsAreUpdated(VertxTestContext context) {
        String resourceName = getResourceName(RESOURCE_NAME);
        Checkpoint async = context.checkpoint();
        String namespace = getNamespace();

        StrimziPodSetOperator op = operator();

        Promise<Void> updateAnnotations = Promise.promise();

        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, getResource(resourceName))
                .onComplete(context.succeeding(i -> {
                }))
                .compose(rrCreated -> {
                    //we don't use the cluster resource as it already contains managedfields property and we cant send that in a patch
                    StrimziPodSet updated = getResourceWithModifiedAnnotations(getResource(resourceName));
                    return op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, updated);
                })
                .compose(i -> op.getAsync(namespace, resourceName)) // We need to get it again
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    updateAnnotations.complete();
                    assertThat(result.getMetadata().getAnnotations().containsKey("new-test-annotation"), is(true));
                })));

        updateAnnotations.future()
                .compose(v -> {
                    return op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, null);
                })
                .onComplete(context.succeeding(v -> async.flag()));
    }

    @Test
    public void testStrimziPodSetManagedFieldsAreOwnedByStrimziCorrectly(VertxTestContext context) {
        String resourceName = getResourceName(RESOURCE_NAME);
        Checkpoint async = context.checkpoint();
        String namespace = getNamespace();

        StrimziPodSetOperator op = operator();

        Promise<Void> updateAnnotations = Promise.promise();

        client.resource(getResourceWithStartingAnnotations(getResource(resourceName))).create();

        op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, getResourceWithModifiedAnnotations(getResource(resourceName)))
                .onComplete(context.succeeding(result -> context.verify(() -> {
                    updateAnnotations.complete();
                    assertThat(result.resource().getMetadata().getAnnotations().containsKey("test-annotation"), is(true));
                    assertThat(result.resource().getMetadata().getAnnotations().containsKey("new-test-annotation"), is(true));
                    assertThat(annotationManagedBy("test-annotation", "strimzi-cluster-operator", result.resource().getMetadata().getManagedFields()), is(false));
                    assertThat(annotationManagedBy("new-test-annotation", "strimzi-cluster-operator", result.resource().getMetadata().getManagedFields()), is(true));
                })));

        updateAnnotations.future()
                .compose(v -> {
                    return op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, null);
                })
                .onComplete(context.succeeding(v -> async.flag()));
    }

    private StrimziPodSet getResourceWithStartingAnnotations(StrimziPodSet previousResource) {
        return new StrimziPodSetBuilder(previousResource)
                .withNewMetadata()
                .withName(previousResource.getMetadata().getName())
                .withNamespace(previousResource.getMetadata().getNamespace())
                .withAnnotations(Map.of("test-annotation", "test-value"))
                .endMetadata()
                .build();
    }

    private StrimziPodSet getResourceWithModifiedAnnotations(StrimziPodSet previousResource) {
        return new StrimziPodSetBuilder(previousResource)
                .withNewMetadata()
                .withName(previousResource.getMetadata().getName())
                .withNamespace(previousResource.getMetadata().getNamespace())
                .withAnnotations(Map.of("new-test-annotation", "new-test-value"))
                .endMetadata()
                .build();
    }

    private boolean annotationManagedBy(String annotationName, String owner, List<ManagedFieldsEntry> managedFieldsEntries) {
        return managedFieldsEntries.stream()
                .filter(managedFieldsEntry -> managedFieldsEntry.getManager().equals(owner))
                .anyMatch(managedFieldsEntry -> {
                    var properties = managedFieldsEntry.getFieldsV1().getAdditionalProperties();
                    Map<String, Object> metadata = (Map<String, Object>) properties.get("f:metadata");
                    if (metadata != null) {
                        Map<String, Object> annotations = (Map<String, Object>) metadata.get("f:annotations");
                        return annotations.containsKey("f:" + annotationName);
                    } else {
                        return false;
                    }
                });
    }
}