/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimList;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.PatchContext;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.kubernetes.AbstractNamespacedResourceOperator;
import io.strimzi.operator.common.operator.resource.kubernetes.AbstractNamespacedResourceOperatorTest;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PvcOperatorTest extends AbstractNamespacedResourceOperatorTest<KubernetesClient, PersistentVolumeClaim, PersistentVolumeClaimList, Resource<PersistentVolumeClaim>> {

    @Override
    protected boolean supportsServerSideApply() {
        return true;
    }

    @Override
    protected Class<KubernetesClient> clientType() {
        return KubernetesClient.class;
    }

    @Override
    protected Class<Resource> resourceType() {
        return Resource.class;
    }

    @Override
    protected PersistentVolumeClaim resource(String name) {
        return new PersistentVolumeClaimBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(name)
                .endMetadata()
                .build();
    }

    @Override
    protected PersistentVolumeClaim modifiedResource(String name) {
        return new PersistentVolumeClaimBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(name)
                    .addToLabels("foo", "bar")
                .endMetadata()
                .build();
    }

    @Override
    protected void mocker(KubernetesClient mockClient, MixedOperation<PersistentVolumeClaim, PersistentVolumeClaimList, Resource<PersistentVolumeClaim>> op) {
        when(mockClient.persistentVolumeClaims()).thenReturn(op);
    }

    @Override
    protected AbstractNamespacedResourceOperator<KubernetesClient, PersistentVolumeClaim, PersistentVolumeClaimList, Resource<PersistentVolumeClaim>> createResourceOperations(KubernetesClient mockClient) {
        return new PvcOperator(asyncExecutor, mockClient, false);
    }

    @Override
    protected PvcOperator createResourceOperations(KubernetesClient mockClient, boolean useServerSideApply) {
        return new PvcOperator(asyncExecutor, mockClient, useServerSideApply);
    }

    @Test
    public void testRevertingImmutableFields() {
        PersistentVolumeClaim desired = new PersistentVolumeClaimBuilder()
                .withNewMetadata()
                    .withName("my-pvc")
                    .withNamespace("my-namespace")
                .endMetadata()
                .withNewSpec()
                    .withNewResources()
                        .withRequests(Collections.singletonMap("storage", new Quantity("100", null)))
                    .endResources()
                .endSpec()
                .build();

        PersistentVolumeClaim current = new PersistentVolumeClaimBuilder()
                .withNewMetadata()
                    .withName("my-pvc")
                    .withNamespace("my-namespace")
                .endMetadata()
                .withNewSpec()
                    .withAccessModes("ReadWriteOnce")
                    .withNewResources()
                        .withRequests(Collections.singletonMap("storage", new Quantity("10", null)))
                    .endResources()
                    .withStorageClassName("my-storage-class")
                    .withSelector(new LabelSelector(null, Collections.singletonMap("key", "label")))
                    .withVolumeName("pvc-ce9ebf52-435a-11e9-8fbc-06b5ff7c7748")
                .endSpec()
                .build();

        PvcOperator op = createResourceOperations(mock(KubernetesClient.class), false);
        op.revertImmutableChanges(current, desired);

        assertThat(current.getSpec().getStorageClassName(), is(desired.getSpec().getStorageClassName()));
        assertThat(current.getSpec().getAccessModes(), is(desired.getSpec().getAccessModes()));
        assertThat(current.getSpec().getSelector(), is(desired.getSpec().getSelector()));
        assertThat(current.getSpec().getVolumeName(), is(desired.getSpec().getVolumeName()));
    }

    @Test
    public void testIgnoredAnnotationsInDiff()   {
        PersistentVolumeClaim pvcWithDefaultAnnos = new PersistentVolumeClaimBuilder(resource("my-pvc"))
                .editMetadata()
                    .withAnnotations(Map.of("strimzi.io/delete-claim", "false"))
                .endMetadata()
                .build();
        PersistentVolumeClaim pvcWithOtherAnnos = new PersistentVolumeClaimBuilder(pvcWithDefaultAnnos)
                .editMetadata()
                    .withAnnotations(Map.of("strimzi.io/delete-claim", "false",
                            "pv.kubernetes.io/bound-by-controller", "my-controller",
                            "some.annotation.io/key", "value"))
                .endMetadata()
                .build();
        PersistentVolumeClaim pvcWithIgnoredAnnos = new PersistentVolumeClaimBuilder(pvcWithDefaultAnnos)
                .editMetadata()
                    .withAnnotations(Map.of("strimzi.io/delete-claim", "false",
                            "pv.kubernetes.io/bound-by-controller", "my-controller",
                            "volume.beta.kubernetes.io/storage-provisioner", "my-provisioner",
                            "volume.kubernetes.io/storage-resizer", "my-resizer"))
                .endMetadata()
                .build();

        KubernetesClient mockClient = mock(KubernetesClient.class);
        PvcOperator op = createResourceOperations(mockClient, false);

        // This tests check that the AbstractResourceoperator.diff(...) method to verify that IGNORABLE_PATHS is respected.
        // But because it is not accessible directly, it checks it through the internalUpdate method that is accessible.
        @SuppressWarnings("unchecked")
        MixedOperation<PersistentVolumeClaim, PersistentVolumeClaimList, Resource<PersistentVolumeClaim>> pvcOp = mock(MixedOperation.class);
        @SuppressWarnings("unchecked")
        NonNamespaceOperation<PersistentVolumeClaim, PersistentVolumeClaimList, Resource<PersistentVolumeClaim>> nsOp = mock(NonNamespaceOperation.class);
        @SuppressWarnings("unchecked")
        Resource<PersistentVolumeClaim> resourceOp = mock(Resource.class);
        when(mockClient.persistentVolumeClaims()).thenReturn(pvcOp);
        when(nsOp.withName(anyString())).thenReturn(resourceOp);
        when(pvcOp.inNamespace(anyString())).thenReturn(nsOp);
        when(resourceOp.patch(any(PatchContext.class), any(PersistentVolumeClaim.class))).thenAnswer(i -> new PersistentVolumeClaimBuilder((PersistentVolumeClaim) i.getArgument(1))
                .editMetadata()
                    .withResourceVersion("new-version")
                .endMetadata()
                .build());

        assertThat(op.internalUpdate(Reconciliation.DUMMY_RECONCILIATION, "my-namespace", "my-pvc", pvcWithDefaultAnnos, pvcWithDefaultAnnos).toCompletableFuture().join(), is(ReconcileResult.noop(pvcWithDefaultAnnos)));
        assertThat(op.internalUpdate(Reconciliation.DUMMY_RECONCILIATION, "my-namespace", "my-pvc", pvcWithDefaultAnnos, pvcWithIgnoredAnnos).toCompletableFuture().join(), is(ReconcileResult.noop(pvcWithDefaultAnnos)));
        assertThat(op.internalUpdate(Reconciliation.DUMMY_RECONCILIATION, "my-namespace", "my-pvc", pvcWithDefaultAnnos, pvcWithOtherAnnos).toCompletableFuture().join(), is(instanceOf(ReconcileResult.Patched.class)));
        assertThat(op.internalUpdate(Reconciliation.DUMMY_RECONCILIATION, "my-namespace", "my-pvc", pvcWithDefaultAnnos, pvcWithOtherAnnos).toCompletableFuture().join().resource().getMetadata().getAnnotations().get("some.annotation.io/key"), is("value"));
    }

    @Test
    public void testNormalizedVolumeSize() {
        PersistentVolumeClaim pvcWithDefaultStorageSize = new PersistentVolumeClaimBuilder(resource("my-pvc"))
            .withNewSpec()
                .withNewResources()
                    .withRequests(Collections.singletonMap("storage", new Quantity("1Gi", null)))
                .endResources()
            .endSpec()
            .build();

        PersistentVolumeClaim pvcWithSameSizeInMi = new PersistentVolumeClaimBuilder(pvcWithDefaultStorageSize)
            .editSpec()
                .editResources()
                    .withRequests(Collections.singletonMap("storage", new Quantity("1024Mi", null)))
                .endResources()
            .endSpec()
            .build();

        PersistentVolumeClaim pvcWithHigherSizeInMi = new PersistentVolumeClaimBuilder(pvcWithDefaultStorageSize)
            .editSpec()
                .editResources()
                    .withRequests(Collections.singletonMap("storage", new Quantity("1048Mi", null)))
                .endResources()
            .endSpec()
            .build();

        PvcOperator op = createResourceOperations(mock(KubernetesClient.class), false);

        op.configureNormalizedStorageSizeIfEqual(pvcWithDefaultStorageSize, pvcWithSameSizeInMi);
        assertThat(pvcWithSameSizeInMi.getSpec().getResources().getRequests().get("storage").toString().equals("1Gi"), is(true));

        op.configureNormalizedStorageSizeIfEqual(pvcWithDefaultStorageSize, pvcWithHigherSizeInMi);
        assertThat(pvcWithHigherSizeInMi.getSpec().getResources().getRequests().get("storage").toString().equals("1048Mi"), is(true));
    }
}
