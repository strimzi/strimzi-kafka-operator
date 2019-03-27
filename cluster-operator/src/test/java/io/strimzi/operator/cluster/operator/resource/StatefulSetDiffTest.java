/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import org.junit.Test;

import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
        assertFalse(new StatefulSetDiff(ss1, ss2).changesSpecTemplate());
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
        return new StatefulSetDiff(ss1, ss2);
    }

    @Test
    public void testCpuResources() {
        assertTrue(testCpuResources(
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
                        .build()).isEmpty());

        assertFalse(testCpuResources(
                new ResourceRequirementsBuilder()
                        .addToRequests(singletonMap("cpu", new Quantity("1001m")))
                        .build(),
                new ResourceRequirementsBuilder()
                        .addToRequests(singletonMap("cpu", new Quantity("1")))
                        .build()).isEmpty());

        assertFalse(testCpuResources(
                new ResourceRequirementsBuilder()
                        .addToRequests(singletonMap("memory", new Quantity("1.1Gi")))
                        .build(),
                new ResourceRequirementsBuilder()
                        .addToRequests(singletonMap("memory", new Quantity("1181116007")))
                        .build()).isEmpty());

        assertFalse(testCpuResources(
                new ResourceRequirementsBuilder()
                        .build(),
                new ResourceRequirementsBuilder()
                        .addToRequests(singletonMap("memory", new Quantity("1181116007")))
                        .build()).isEmpty());

        assertTrue(testCpuResources(
                new ResourceRequirementsBuilder()
                        .build(),
                new ResourceRequirementsBuilder()
                        .build()).isEmpty());

        assertTrue(testCpuResources(
                new ResourceRequirementsBuilder()
                        .build(),
                null).isEmpty());
    }
}
