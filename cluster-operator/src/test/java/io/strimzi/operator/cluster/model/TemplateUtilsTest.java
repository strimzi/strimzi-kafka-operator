/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.strimzi.api.kafka.model.common.template.AdditionalVolume;
import io.strimzi.api.kafka.model.common.template.ResourceTemplate;
import io.strimzi.api.kafka.model.common.template.PodTemplate;
import io.strimzi.api.kafka.model.common.template.PodTemplateBuilder;
import io.strimzi.api.kafka.model.common.template.AdditionalVolumeBuilder;
import io.strimzi.api.kafka.model.common.template.ResourceTemplateBuilder;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.strimzi.operator.cluster.model.TemplateUtils.ALLOWED_MOUNT_PATH;
import static io.strimzi.operator.cluster.model.TemplateUtils.VOLUME_NAME_REGEX;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ParallelSuite
public class TemplateUtilsTest {

    @ParallelTest
    public void testMetadataWithNullTemplate() {
        assertThat(TemplateUtils.annotations(null), is(nullValue()));
        assertThat(TemplateUtils.labels(null), is(nullValue()));
    }

    @ParallelTest
    public void testMetadataWithEmptyMetadataTemplate() {
        ResourceTemplate template = new ResourceTemplateBuilder()
                .withNewMetadata()
                .endMetadata()
                .build();

        assertThat(TemplateUtils.annotations(template), is(Map.of()));
        assertThat(TemplateUtils.labels(template), is(Map.of()));
    }

    @ParallelTest
    public void testAnnotations() {
        ResourceTemplate template = new ResourceTemplateBuilder()
                .withNewMetadata()
                    .withAnnotations(Map.of("anno-1", "value-1", "anno-2", "value-2"))
                .endMetadata()
                .build();

        assertThat(TemplateUtils.annotations(template), is(Map.of("anno-1", "value-1", "anno-2", "value-2")));
    }

    @ParallelTest
    public void testLabels() {
        ResourceTemplate template = new ResourceTemplateBuilder()
                .withNewMetadata()
                    .withLabels(Map.of("label-1", "value-1", "label-2", "value-2"))
                .endMetadata()
                .build();

        assertThat(TemplateUtils.labels(template), is(Map.of("label-1", "value-1", "label-2", "value-2")));
    }

    @ParallelTest
    public void testAddAdditionalVolumes_InvalidNames() {
        List<Volume> existingVolumes = new ArrayList<>();
        existingVolumes.add(new VolumeBuilder().withName("invalid_name!").build());
        PodTemplate templatePod = new PodTemplateBuilder().withVolumes(new ArrayList<>()).build();

        InvalidResourceException exception = assertThrows(InvalidResourceException.class, () -> {
            TemplateUtils.addAdditionalVolumes(templatePod, existingVolumes);
        });

        assertThat(exception.getMessage(), is("Volume names [invalid_name!] are invalid and do not match the pattern " + VOLUME_NAME_REGEX));
    }

    @ParallelTest
    public void testAddAdditionalVolumes_DuplicateNames() {
        List<Volume> existingVolumes = new ArrayList<>();
        existingVolumes.add(new VolumeBuilder().withName("duplicate").build());
        List<AdditionalVolume> additionalVolumes = new ArrayList<>();
        additionalVolumes.add(new AdditionalVolumeBuilder().withName("duplicate").build());
        PodTemplate templatePod = new PodTemplateBuilder().withVolumes(additionalVolumes).build();

        InvalidResourceException exception = assertThrows(InvalidResourceException.class, () -> {
            TemplateUtils.addAdditionalVolumes(templatePod, existingVolumes);
        });

        assertThat(exception.getMessage(), is("Duplicate volume names found in additional volumes: [duplicate]"));
    }

    @ParallelTest
    public void testAddAdditionalVolumes_ValidInputs() {
        List<Volume> existingVolumes = new ArrayList<>();
        existingVolumes.add(new VolumeBuilder().withName("existingVolume1").build());
        List<AdditionalVolume> additionalVolumes = new ArrayList<>();
        additionalVolumes.add(new AdditionalVolumeBuilder().withName("newVolume1").build());
        additionalVolumes.add(new AdditionalVolumeBuilder().withName("newVolume2").build());
        PodTemplate templatePod = new PodTemplateBuilder().withVolumes(additionalVolumes).build();

        TemplateUtils.addAdditionalVolumes(templatePod, existingVolumes);

        assertThat(existingVolumes.size(), is(3));
        assertThat(existingVolumes.get(1).getName(), is("newVolume1"));
        assertThat(existingVolumes.get(2).getName(), is("newVolume2"));
    }


    @ParallelTest
    public void testAddAdditionalVolumeMounts_ValidInputs() {
        List<VolumeMount> volumeMounts = new ArrayList<>();
        List<VolumeMount> additionalVolumeMounts = new ArrayList<>();
        additionalVolumeMounts.add(new VolumeMountBuilder().withMountPath(ALLOWED_MOUNT_PATH + "/path1").build());
        additionalVolumeMounts.add(new VolumeMountBuilder().withMountPath(ALLOWED_MOUNT_PATH + "/path2").build());

        TemplateUtils.addAdditionalVolumeMounts(volumeMounts, additionalVolumeMounts);

        assertThat(volumeMounts.size(), is(2));
        assertThat(volumeMounts.get(0).getMountPath(), is(ALLOWED_MOUNT_PATH + "/path1"));
        assertThat(volumeMounts.get(1).getMountPath(), is(ALLOWED_MOUNT_PATH + "/path2"));
    }

    @ParallelTest
    public void testAddAdditionalVolumeMounts_InvalidInputs() {
        List<VolumeMount> volumeMounts = new ArrayList<>();
        List<VolumeMount> additionalVolumeMounts = new ArrayList<>();
        additionalVolumeMounts.add(new VolumeMountBuilder().withMountPath("/forbidden/path1").build());

        InvalidResourceException exception = assertThrows(InvalidResourceException.class, () -> {
            TemplateUtils.addAdditionalVolumeMounts(volumeMounts, additionalVolumeMounts);
        });

        assertThat(exception.getMessage(), is(String.format("Forbidden path found in additional volumes. Should start with %s", ALLOWED_MOUNT_PATH)));
    }




}
