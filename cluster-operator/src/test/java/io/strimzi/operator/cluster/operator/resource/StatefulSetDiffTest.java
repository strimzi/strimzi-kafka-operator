/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSetBuilder;
import org.junit.Test;

import java.util.Collections;

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
        assertFalse(new StatefulSetDiff(ss1, ss2).changesSpecTemplateSpec());
    }

    @Test
    public void testInitContainersDiff() {
        StatefulSet ss1 = new StatefulSetBuilder()
                .withNewMetadata()
                .withNamespace("test")
                .withName("foo")
                .endMetadata()
                .withNewSpec().
                    withNewTemplate()
                        .withNewSpec()
                            .withInitContainers(new ContainerBuilder().withName("init1").withImage("image1").build())
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
                            .withInitContainers(new ContainerBuilder().withName("init2").withImage("image2").build())
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();
        StatefulSet ss3 = new StatefulSetBuilder()
                .withNewMetadata()
                .withNamespace("test")
                .withName("foo")
                .endMetadata()
                .withNewSpec()
                    .withNewTemplate()
                        .withNewSpec()
                            .withInitContainers(Collections.emptyList())
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();
        assertTrue(new StatefulSetDiff(ss1, ss2).changesSpecTemplateSpecInitContainers());
        assertTrue(new StatefulSetDiff(ss1, ss3).changesSpecTemplateSpecInitContainers());
    }
}
