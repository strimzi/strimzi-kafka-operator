/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.CSIVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.EmptyDirVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.ImageVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.SecretVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.strimzi.api.kafka.model.common.template.AdditionalVolume;
import io.strimzi.api.kafka.model.common.template.AdditionalVolumeBuilder;
import io.strimzi.api.kafka.model.common.template.ContainerTemplate;
import io.strimzi.api.kafka.model.common.template.PodTemplate;
import io.strimzi.api.kafka.model.common.template.PodTemplateBuilder;
import io.strimzi.api.kafka.model.common.template.ResourceTemplate;
import io.strimzi.api.kafka.model.common.template.ResourceTemplateBuilder;
import io.strimzi.operator.common.model.InvalidResourceException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.strimzi.operator.cluster.model.TemplateUtils.ALLOWED_MOUNT_PATH;
import static io.strimzi.operator.cluster.model.TemplateUtils.VOLUME_NAME_REGEX;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TemplateUtilsTest {

    @Test
    public void testMetadataWithNullTemplate() {
        assertThat(TemplateUtils.annotations(null), is(nullValue()));
        assertThat(TemplateUtils.labels(null), is(nullValue()));
    }

    @Test
    public void testMetadataWithEmptyMetadataTemplate() {
        ResourceTemplate template = new ResourceTemplateBuilder()
                .withNewMetadata()
                .endMetadata()
                .build();

        assertThat(TemplateUtils.annotations(template), is(Map.of()));
        assertThat(TemplateUtils.labels(template), is(Map.of()));
    }

    @Test
    public void testAnnotations() {
        ResourceTemplate template = new ResourceTemplateBuilder()
                .withNewMetadata()
                    .withAnnotations(Map.of("anno-1", "value-1", "anno-2", "value-2"))
                .endMetadata()
                .build();

        assertThat(TemplateUtils.annotations(template), is(Map.of("anno-1", "value-1", "anno-2", "value-2")));
    }

    @Test
    public void testLabels() {
        ResourceTemplate template = new ResourceTemplateBuilder()
                .withNewMetadata()
                    .withLabels(Map.of("label-1", "value-1", "label-2", "value-2"))
                .endMetadata()
                .build();

        assertThat(TemplateUtils.labels(template), is(Map.of("label-1", "value-1", "label-2", "value-2")));
    }

    @Test
    public void testAddAdditionalVolumes_InvalidNames() {
        List<Volume> existingVolumes = new ArrayList<>();
        existingVolumes.add(new VolumeBuilder().withName("invalid_name!").build());
        PodTemplate templatePod = new PodTemplateBuilder().withVolumes(new ArrayList<>()).build();

        InvalidResourceException exception = assertThrows(InvalidResourceException.class, () -> TemplateUtils.addAdditionalVolumes(templatePod, existingVolumes));

        assertThat(exception.getMessage(), is("Volume names [invalid_name!] are invalid and do not match the pattern " + VOLUME_NAME_REGEX));
    }

    @Test
    public void testAddAdditionalVolumes_DuplicateNames() {
        List<Volume> existingVolumes = new ArrayList<>();
        existingVolumes.add(new VolumeBuilder().withName("duplicate").build());
        List<AdditionalVolume> additionalVolumes = new ArrayList<>();
        additionalVolumes.add(new AdditionalVolumeBuilder().withName("duplicate").build());
        PodTemplate templatePod = new PodTemplateBuilder().withVolumes(additionalVolumes).build();

        InvalidResourceException exception = assertThrows(InvalidResourceException.class, () -> TemplateUtils.addAdditionalVolumes(templatePod, existingVolumes));

        assertThat(exception.getMessage(), is("Duplicate volume names found in additional volumes: [duplicate]"));
    }

    @Test
    public void testAddAdditionalVolumes_ValidInputs() {
        List<Volume> existingVolumes = new ArrayList<>();
        existingVolumes.add(new VolumeBuilder().withName("existingVolume1").build());

        PodTemplate templatePod = new PodTemplateBuilder()
                .withVolumes(
                        new AdditionalVolumeBuilder()
                                .withName("secret-volume")
                                .withSecret(new SecretVolumeSourceBuilder().withSecretName("my-secret").build())
                                .build(),
                        new AdditionalVolumeBuilder()
                                .withName("cm-volume")
                                .withConfigMap(new ConfigMapVolumeSourceBuilder().withName("my-cm").build())
                                .build(),
                        new AdditionalVolumeBuilder()
                                .withName("empty-dir-volume")
                                .withEmptyDir(new EmptyDirVolumeSourceBuilder().withMedium("Memory").withSizeLimit(new Quantity("100Mi")).build())
                                .build(),
                        new AdditionalVolumeBuilder()
                                .withName("pvc-volume")
                                .withPersistentVolumeClaim(new PersistentVolumeClaimVolumeSourceBuilder().withClaimName("my-pvc").build())
                                .build(),
                        new AdditionalVolumeBuilder()
                                .withName("csi-volume")
                                .withCsi(new CSIVolumeSourceBuilder().withDriver("csi.cert-manager.io").withReadOnly().withVolumeAttributes(Map.of("csi.cert-manager.io/issuer-name", "my-ca", "csi.cert-manager.io/dns-names", "${POD_NAME}.${POD_NAMESPACE}.svc.cluster.local")).build())
                                .build(),
                        new AdditionalVolumeBuilder()
                                .withName("oci-volume")
                                .withImage(new ImageVolumeSourceBuilder().withReference("my-custom-oci-plugin:latest").withPullPolicy("Never").build())
                                .build())
                .build();

        TemplateUtils.addAdditionalVolumes(templatePod, existingVolumes);

        assertThat(existingVolumes.size(), is(7));

        assertThat(existingVolumes.get(0).getName(), is("existingVolume1"));

        assertThat(existingVolumes.get(1).getName(), is("secret-volume"));
        assertThat(existingVolumes.get(1).getSecret().getSecretName(), is("my-secret"));
        assertThat(existingVolumes.get(1).getConfigMap(), is(nullValue()));
        assertThat(existingVolumes.get(1).getEmptyDir(), is(nullValue()));
        assertThat(existingVolumes.get(1).getPersistentVolumeClaim(), is(nullValue()));
        assertThat(existingVolumes.get(1).getCsi(), is(nullValue()));
        assertThat(existingVolumes.get(1).getImage(), is(nullValue()));

        assertThat(existingVolumes.get(2).getName(), is("cm-volume"));
        assertThat(existingVolumes.get(2).getConfigMap().getName(), is("my-cm"));
        assertThat(existingVolumes.get(2).getSecret(), is(nullValue()));
        assertThat(existingVolumes.get(2).getEmptyDir(), is(nullValue()));
        assertThat(existingVolumes.get(2).getPersistentVolumeClaim(), is(nullValue()));
        assertThat(existingVolumes.get(2).getCsi(), is(nullValue()));
        assertThat(existingVolumes.get(2).getImage(), is(nullValue()));

        assertThat(existingVolumes.get(3).getName(), is("empty-dir-volume"));
        assertThat(existingVolumes.get(3).getEmptyDir().getMedium(), is("Memory"));
        assertThat(existingVolumes.get(3).getEmptyDir().getSizeLimit(), is(new Quantity("100Mi")));
        assertThat(existingVolumes.get(3).getSecret(), is(nullValue()));
        assertThat(existingVolumes.get(3).getConfigMap(), is(nullValue()));
        assertThat(existingVolumes.get(3).getPersistentVolumeClaim(), is(nullValue()));
        assertThat(existingVolumes.get(3).getCsi(), is(nullValue()));
        assertThat(existingVolumes.get(3).getImage(), is(nullValue()));

        assertThat(existingVolumes.get(4).getName(), is("pvc-volume"));
        assertThat(existingVolumes.get(4).getPersistentVolumeClaim().getClaimName(), is("my-pvc"));
        assertThat(existingVolumes.get(4).getSecret(), is(nullValue()));
        assertThat(existingVolumes.get(4).getConfigMap(), is(nullValue()));
        assertThat(existingVolumes.get(4).getEmptyDir(), is(nullValue()));
        assertThat(existingVolumes.get(4).getCsi(), is(nullValue()));
        assertThat(existingVolumes.get(4).getImage(), is(nullValue()));

        assertThat(existingVolumes.get(5).getName(), is("csi-volume"));
        assertThat(existingVolumes.get(5).getCsi().getDriver(), is("csi.cert-manager.io"));
        assertThat(existingVolumes.get(5).getCsi().getReadOnly(), is(true));
        assertThat(existingVolumes.get(5).getCsi().getVolumeAttributes().get("csi.cert-manager.io/issuer-name"), is("my-ca"));
        assertThat(existingVolumes.get(5).getCsi().getVolumeAttributes().get("csi.cert-manager.io/dns-names"), is("${POD_NAME}.${POD_NAMESPACE}.svc.cluster.local"));
        assertThat(existingVolumes.get(5).getSecret(), is(nullValue()));
        assertThat(existingVolumes.get(5).getConfigMap(), is(nullValue()));
        assertThat(existingVolumes.get(5).getEmptyDir(), is(nullValue()));
        assertThat(existingVolumes.get(5).getPersistentVolumeClaim(), is(nullValue()));
        assertThat(existingVolumes.get(5).getImage(), is(nullValue()));

        assertThat(existingVolumes.get(6).getName(), is("oci-volume"));
        assertThat(existingVolumes.get(6).getImage().getReference(), is("my-custom-oci-plugin:latest"));
        assertThat(existingVolumes.get(6).getImage().getPullPolicy(), is("Never"));
        assertThat(existingVolumes.get(6).getSecret(), is(nullValue()));
        assertThat(existingVolumes.get(6).getConfigMap(), is(nullValue()));
        assertThat(existingVolumes.get(6).getEmptyDir(), is(nullValue()));
        assertThat(existingVolumes.get(6).getPersistentVolumeClaim(), is(nullValue()));
        assertThat(existingVolumes.get(6).getCsi(), is(nullValue()));
    }

    @Test
    public void testAddAdditionalVolumeMounts_ValidInputs() {
        List<VolumeMount> volumeMounts = new ArrayList<>();
        List<VolumeMount> additionalVolumeMounts = new ArrayList<>();
        additionalVolumeMounts.add(new VolumeMountBuilder().withMountPath(ALLOWED_MOUNT_PATH + "/path1").build());
        additionalVolumeMounts.add(new VolumeMountBuilder().withMountPath(ALLOWED_MOUNT_PATH + "/path2").build());

        ContainerTemplate containerTemplate = new ContainerTemplate();
        containerTemplate.setVolumeMounts(additionalVolumeMounts);

        TemplateUtils.addAdditionalVolumeMounts(volumeMounts, containerTemplate);

        assertThat(volumeMounts.size(), is(2));
        assertThat(volumeMounts.get(0).getMountPath(), is(ALLOWED_MOUNT_PATH + "/path1"));
        assertThat(volumeMounts.get(1).getMountPath(), is(ALLOWED_MOUNT_PATH + "/path2"));
    }

    @Test
    public void testAddAdditionalVolumeMounts_InvalidInputs() {
        List<VolumeMount> volumeMounts = new ArrayList<>();
        List<VolumeMount> additionalVolumeMounts = new ArrayList<>();
        additionalVolumeMounts.add(new VolumeMountBuilder().withMountPath("/forbidden/path1").build());
        ContainerTemplate containerTemplate = new ContainerTemplate();
        containerTemplate.setVolumeMounts(additionalVolumeMounts);

        InvalidResourceException exception = assertThrows(InvalidResourceException.class, () -> TemplateUtils.addAdditionalVolumeMounts(volumeMounts, containerTemplate));

        assertThat(exception.getMessage(), is(String.format("Forbidden path found in additional volumes. Should start with %s", ALLOWED_MOUNT_PATH)));
    }
}
