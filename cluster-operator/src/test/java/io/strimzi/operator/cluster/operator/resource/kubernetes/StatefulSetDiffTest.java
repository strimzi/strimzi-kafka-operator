/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.strimzi.operator.common.Reconciliation;
import org.junit.jupiter.api.Test;

import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class StatefulSetDiffTest {
    @Test
    public void testSpecVolumesIgnored() {
        StatefulSet ss1 = new StatefulSetBuilder()
            .withNewMetadata()
                .withNamespace("test")
                .withName("foo")
            .endMetadata()
            .withNewSpec().
                withNewTemplate()
                    .withNewSpec()
                        .addToVolumes(0, new VolumeBuilder()
                                    .withConfigMap(new ConfigMapVolumeSourceBuilder().withDefaultMode(1).build())
                                .build())
                    .endSpec()
                .endTemplate()
            .endSpec()
            .build();
        StatefulSet ss2 = new StatefulSetBuilder()
            .withNewMetadata()
                .withNamespace("test")
                .withName("foo")
            .endMetadata()
            .withNewSpec()
                .withNewTemplate()
                    .withNewSpec()
                        .addToVolumes(0, new VolumeBuilder()
                                .withConfigMap(new ConfigMapVolumeSourceBuilder().withDefaultMode(2).build())
                                .build())
                    .endSpec()
                .endTemplate()
            .endSpec()
            .build();
        assertThat(new StatefulSetDiff(Reconciliation.DUMMY_RECONCILIATION, ss1, ss2).changesSpecTemplate(), is(false));
    }

    public StatefulSetDiff testCpuResources(ResourceRequirements requirements1, ResourceRequirements requirements2) {
        StatefulSet ss1 = new StatefulSetBuilder()
                .withNewMetadata()
                    .withNamespace("test")
                    .withName("foo")
                .endMetadata()
                .withNewSpec().
                    withNewTemplate()
                        .withNewSpec()
                            .addNewContainer()
                                .withResources(requirements1)
                            .endContainer()
                        .endSpec()
                    .endTemplate()
                .endSpec()
            .build();
        StatefulSet ss2 = new StatefulSetBuilder()
                .withNewMetadata()
                .withNamespace("test")
                .withName("foo")
                .endMetadata()
                .withNewSpec()
                    .withNewTemplate()
                        .withNewSpec()
                            .addNewContainer()
                                .withResources(requirements2)
                            .endContainer()
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();
        return new StatefulSetDiff(Reconciliation.DUMMY_RECONCILIATION, ss1, ss2);
    }

    @Test
    public void testCpuResources() {
        assertThat(testCpuResources(
                new ResourceRequirementsBuilder()
                        .addToRequests(singletonMap("cpu", new Quantity("1000m")))
                        .addToRequests(singletonMap("memory", new Quantity("1.1Gi")))
                        .addToLimits(singletonMap("cpu", new Quantity("1000m")))
                        .addToLimits(singletonMap("memory", new Quantity("500Mi")))
                        .build(),
                new ResourceRequirementsBuilder()
                        .addToRequests(singletonMap("cpu", new Quantity("1")))
                        .addToRequests(singletonMap("memory", new Quantity("1181116006")))
                        .addToLimits(singletonMap("cpu", new Quantity("1")))
                        .addToLimits(singletonMap("memory", new Quantity("524288000")))
                        .build()).isEmpty(), is(true));

        assertThat(testCpuResources(
                new ResourceRequirementsBuilder()
                        .addToRequests(singletonMap("cpu", new Quantity("1001m")))
                        .build(),
                new ResourceRequirementsBuilder()
                        .addToRequests(singletonMap("cpu", new Quantity("1")))
                        .build()).isEmpty(), is(false));

        assertThat(testCpuResources(
                new ResourceRequirementsBuilder()
                        .addToRequests(singletonMap("memory", new Quantity("1.1Gi")))
                        .build(),
                new ResourceRequirementsBuilder()
                        .addToRequests(singletonMap("memory", new Quantity("1181116007")))
                        .build()).isEmpty(), is(false));

        assertThat(testCpuResources(
                new ResourceRequirementsBuilder()
                        .build(),
                new ResourceRequirementsBuilder()
                        .addToRequests(singletonMap("memory", new Quantity("1181116007")))
                        .build()).isEmpty(), is(false));

        assertThat(testCpuResources(
                new ResourceRequirementsBuilder()
                        .build(),
                new ResourceRequirementsBuilder()
                        .build()).isEmpty(), is(true));

        assertThat(testCpuResources(
                new ResourceRequirementsBuilder()
                        .build(),
                null).isEmpty(), is(true));
    }

    @Test
    public void testPvcSizeChangeIgnored() {
        StatefulSet ss1 = new StatefulSetBuilder()
                .withNewMetadata()
                .withNamespace("test")
                .withName("foo")
                .endMetadata()
                .withNewSpec()
                    .withNewTemplate()
                        .withNewSpec()
                            .addToVolumes(0, new VolumeBuilder()
                                    .withConfigMap(new ConfigMapVolumeSourceBuilder().withDefaultMode(1).build())
                                    .build())
                        .endSpec()
                    .endTemplate()
                    .withVolumeClaimTemplates(new PersistentVolumeClaimBuilder()
                            .withNewSpec()
                                .withNewResources()
                                    .withRequests(singletonMap("storage", new Quantity("100Gi")))
                                .endResources()
                            .endSpec()
                            .build())
                .endSpec()
                .build();
        StatefulSet ss2 = new StatefulSetBuilder()
                .withNewMetadata()
                .withNamespace("test")
                .withName("foo")
                .endMetadata()
                .withNewSpec()
                    .withNewTemplate()
                        .withNewSpec()
                            .addToVolumes(0, new VolumeBuilder()
                                    .withConfigMap(new ConfigMapVolumeSourceBuilder().withDefaultMode(2).build())
                                    .build())
                        .endSpec()
                    .endTemplate()
                    .withVolumeClaimTemplates(new PersistentVolumeClaimBuilder()
                            .withNewSpec()
                            .withNewResources()
                            .withRequests(singletonMap("storage", new Quantity("110Gi")))
                            .endResources()
                            .endSpec()
                            .build())
                .endSpec()
                .build();
        StatefulSet ss3 = new StatefulSetBuilder() // Used for millibytes test
                .withNewMetadata()
                .withNamespace("test")
                .withName("foo")
                .endMetadata()
                .withNewSpec()
                    .withNewTemplate()
                        .withNewSpec()
                            .addToVolumes(0, new VolumeBuilder()
                                    .withConfigMap(new ConfigMapVolumeSourceBuilder().withDefaultMode(2).build())
                                    .build())
                        .endSpec()
                    .endTemplate()
                    .withVolumeClaimTemplates(new PersistentVolumeClaimBuilder()
                            .withNewSpec()
                            .withNewResources()
                            .withRequests(singletonMap("storage", new Quantity("3518437208883200m")))
                            .endResources()
                            .endSpec()
                            .build())
                .endSpec()
                .build();
        assertThat(new StatefulSetDiff(Reconciliation.DUMMY_RECONCILIATION, ss1, ss2).changesVolumeClaimTemplates(), is(false));
        assertThat(new StatefulSetDiff(Reconciliation.DUMMY_RECONCILIATION, ss1, ss2).changesVolumeSize(), is(true));
        assertThat(new StatefulSetDiff(Reconciliation.DUMMY_RECONCILIATION, ss1, ss3).changesVolumeClaimTemplates(), is(false));
        assertThat(new StatefulSetDiff(Reconciliation.DUMMY_RECONCILIATION, ss1, ss3).changesVolumeSize(), is(true));
    }

    @Test
    public void testPvcSizeUnitChangeIgnored() {
        StatefulSet ss1 = new StatefulSetBuilder()
                .withNewMetadata()
                .withNamespace("test")
                .withName("foo")
                .endMetadata()
                .withNewSpec()
                .withNewTemplate()
                .withNewSpec()
                .addToVolumes(0, new VolumeBuilder()
                        .withConfigMap(new ConfigMapVolumeSourceBuilder().withDefaultMode(1).build())
                        .build())
                .endSpec()
                .endTemplate()
                .withVolumeClaimTemplates(new PersistentVolumeClaimBuilder()
                        .withNewSpec()
                        .withNewResources()
                        .withRequests(singletonMap("storage", new Quantity("3072Gi")))
                        .endResources()
                        .endSpec()
                        .build())
                .endSpec()
                .build();
        StatefulSet ss2 = new StatefulSetBuilder()
                .withNewMetadata()
                .withNamespace("test")
                .withName("foo")
                .endMetadata()
                .withNewSpec()
                .withNewTemplate()
                .withNewSpec()
                .addToVolumes(0, new VolumeBuilder()
                        .withConfigMap(new ConfigMapVolumeSourceBuilder().withDefaultMode(2).build())
                        .build())
                .endSpec()
                .endTemplate()
                .withVolumeClaimTemplates(new PersistentVolumeClaimBuilder()
                        .withNewSpec()
                        .withNewResources()
                        .withRequests(singletonMap("storage", new Quantity("3Ti")))
                        .endResources()
                        .endSpec()
                        .build())
                .endSpec()
                .build();
        assertThat(new StatefulSetDiff(Reconciliation.DUMMY_RECONCILIATION, ss1, ss2).changesVolumeClaimTemplates(), is(false));
        assertThat(new StatefulSetDiff(Reconciliation.DUMMY_RECONCILIATION, ss1, ss2).changesVolumeSize(), is(false));
    }

    @Test
    public void testNewPvcNotIgnored() {
        StatefulSet ss1 = new StatefulSetBuilder()
                .withNewMetadata()
                .withNamespace("test")
                .withName("foo")
                .endMetadata()
                .withNewSpec()
                    .withNewTemplate()
                        .withNewSpec()
                            .addToVolumes(0, new VolumeBuilder()
                                    .withConfigMap(new ConfigMapVolumeSourceBuilder().withDefaultMode(1).build())
                                    .build())
                        .endSpec()
                    .endTemplate()
                    .withVolumeClaimTemplates(new PersistentVolumeClaimBuilder()
                            .withNewSpec()
                            .withNewResources()
                            .withRequests(singletonMap("storage", new Quantity("100Gi")))
                            .endResources()
                            .endSpec()
                            .build())
                .endSpec()
                .build();
        StatefulSet ss2 = new StatefulSetBuilder()
                .withNewMetadata()
                .withNamespace("test")
                .withName("foo")
                .endMetadata()
                .withNewSpec()
                    .withNewTemplate()
                        .withNewSpec()
                            .addToVolumes(0, new VolumeBuilder()
                                    .withConfigMap(new ConfigMapVolumeSourceBuilder().withDefaultMode(2).build())
                                    .build())
                        .endSpec()
                    .endTemplate()
                    .withVolumeClaimTemplates(new PersistentVolumeClaimBuilder()
                            .withNewSpec()
                            .withNewResources()
                            .withRequests(singletonMap("storage", new Quantity("100Gi")))
                            .endResources()
                            .endSpec()
                            .build(),
                            new PersistentVolumeClaimBuilder()
                                    .withNewSpec()
                                    .withNewResources()
                                    .withRequests(singletonMap("storage", new Quantity("110Gi")))
                                    .endResources()
                                    .endSpec()
                                    .build())
                .endSpec()
                .build();
        assertThat(new StatefulSetDiff(Reconciliation.DUMMY_RECONCILIATION, ss1, ss2).changesVolumeClaimTemplates(), is(true));
        assertThat(new StatefulSetDiff(Reconciliation.DUMMY_RECONCILIATION, ss1, ss2).changesVolumeSize(), is(false));
    }
}
