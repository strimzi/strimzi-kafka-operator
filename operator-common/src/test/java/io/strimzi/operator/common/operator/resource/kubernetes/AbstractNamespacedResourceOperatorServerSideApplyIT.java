/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.PatchContext;
import io.fabric8.kubernetes.client.dsl.base.PatchType;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The primary purpose of the integration tests for the operators is to validate their behavior on a live Kubernetes cluster.
 * Live clusters often exhibit specific behaviors, such as immutable fields or API-generated spec fields, that are difficult to simulate with mocks.
 * These integration tests enable reliable validation of operator functionality in a production-like environment.
 */
public abstract class AbstractNamespacedResourceOperatorServerSideApplyIT<C extends KubernetesClient,
        T extends HasMetadata,
        L extends KubernetesResourceList<T>,
        R extends Resource<T>> extends AbstractNamespacedResourceOperatorIT<C, T, L, R> {
    protected static final Logger LOGGER = LogManager.getLogger(AbstractNamespacedResourceOperatorServerSideApplyIT.class);

    abstract public T getNonConflicting();
    abstract public T getConflicting();

    @Test
    public void testCreateModifyDelete() {
        AbstractNamespacedResourceOperator<C, T, L, R> op = operator();

        T newResource = getOriginal();
        T nonConfResource = getNonConflicting();
        T modResource = getModified();

        TestUtils.await(op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, newResource)
            .whenComplete((rrCreated, error) -> {
                assertNull(error);
                T created = op.get(namespace, resourceName);

                assertThat(created, is(notNullValue()));
                assertResources(newResource, created);
            })
            .thenRun(() -> {
                op.client().inNamespace(namespace).resource(nonConfResource).update();

                T currentResource = op.get(namespace, resourceName);

                assertThat(currentResource.getMetadata().getAnnotations(), is(nonConfResource.getMetadata().getAnnotations()));
            })
            .thenCompose(rr -> op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, modResource))
            .whenComplete((rrModified, error) -> {
                assertNull(error);
                T modified = op.get(namespace, resourceName);

                assertThat(rrModified.getType(), is(ReconcileResult.Type.PATCHED_WITH_SERVER_SIDE_APPLY));
                assertThat(modified, is(notNullValue()));
                assertThat(modified.getMetadata().getAnnotations(), is(nonConfResource.getMetadata().getAnnotations()));
                assertResources(modResource, modified);
            })
            .thenCompose(rr -> op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, null))
            .whenComplete(TestUtils::assertSuccessful)
            .thenCompose(rr -> resourceSupport.waitFor(
                    Reconciliation.DUMMY_RECONCILIATION,
                    "resource deletion " + resourceName,
                    "deleted",
                    1000,
                    30_000,
                    () -> op.get(namespace, resourceName) == null))
            .thenRun(() -> assertThat(op.get(namespace, resourceName), is(nullValue()))));
    }

    @Test
    void testCreateModifyWithConflictAndDelete() {
        AbstractNamespacedResourceOperator<C, T, L, R> op = operator();

        T newResource = getOriginal();
        T modResource = getModified();
        T confResource = getConflicting();

        TestUtils.await(op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, newResource)
            .whenComplete((rrCreated, error) -> {
                assertNull(error);
                T created = op.get(namespace, resourceName);

                assertThat(created, is(notNullValue()));
                assertResources(newResource, created);
            })
            .thenRun(() -> {
                PatchContext patchContext = new PatchContext.Builder()
                    .withFieldManager("another-operator")
                    .withForce(false)
                    .withPatchType(PatchType.SERVER_SIDE_APPLY)
                    .build();

                op.client().inNamespace(namespace).withName(resourceName).patch(patchContext, confResource);

                T modified = op.get(namespace, resourceName);

                assertResources(confResource, modified);
            })
            .thenCompose(rr -> op.reconcile(Reconciliation.DUMMY_RECONCILIATION, namespace, resourceName, modResource))
            .whenComplete((rrModified, error) -> {
                assertNull(error);
                T modified = op.get(namespace, resourceName);

                assertThat(rrModified.getType(), is(ReconcileResult.Type.PATCHED_WITH_SERVER_SIDE_APPLY));
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
                    30_000,
                    () -> op.get(namespace, resourceName) == null))
            .thenRun(() -> assertThat(op.get(namespace, resourceName), is(nullValue()))));
    }
}
