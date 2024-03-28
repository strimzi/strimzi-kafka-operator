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
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PvcOperatorTest extends AbstractNamespacedResourceOperatorTest<KubernetesClient, PersistentVolumeClaim, PersistentVolumeClaimList, Resource<PersistentVolumeClaim>> {

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
    protected void mocker(KubernetesClient mockClient, MixedOperation op) {
        when(mockClient.persistentVolumeClaims()).thenReturn(op);
    }

    @Override
    protected PvcOperator createResourceOperations(Vertx vertx, KubernetesClient mockClient) {
        return new PvcOperator(vertx, mockClient);
    }

    @Test
    public void testRevertingImmutableFields()   {
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

        PvcOperator op = createResourceOperations(vertx, mock(KubernetesClient.class));
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

        PvcOperator op = createResourceOperations(vertx, mock(KubernetesClient.class));

        assertThat(op.diff(Reconciliation.DUMMY_RECONCILIATION, "my-pvc", pvcWithDefaultAnnos, pvcWithDefaultAnnos).isEmpty(), is(true));
        assertThat(op.diff(Reconciliation.DUMMY_RECONCILIATION, "my-pvc", pvcWithDefaultAnnos, pvcWithIgnoredAnnos).isEmpty(), is(true));
        assertThat(op.diff(Reconciliation.DUMMY_RECONCILIATION, "my-pvc", pvcWithDefaultAnnos, pvcWithOtherAnnos).isEmpty(), is(false));
    }
}
