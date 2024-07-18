/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.strimzi.api.kafka.model.common.template.AdditionalVolume;
import io.strimzi.api.kafka.model.common.template.DeploymentStrategy;
import io.strimzi.api.kafka.model.common.template.DeploymentTemplate;
import io.strimzi.api.kafka.model.common.template.HasMetadataTemplate;
import io.strimzi.api.kafka.model.common.template.PodTemplate;
import io.strimzi.operator.common.model.InvalidResourceException;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Shared methods for working with Strimzi API templates
 */
public class TemplateUtils {
    /**
     * This is a constant that represents an allowed mountable path in the file system.
     * It is used to prevent the creation of volumes outside this path.
     */
    protected static final String ALLOWED_MOUNT_PATH = "/mnt";
    /**
     * This Pattern defines a regex for validating volume names with the following criteria:
     * Length: Must contain at most 63 characters.
     * Characters: Can only contain lowercase alphanumeric characters, '-', '.', or '_'.
     * Start: Must start with an alphanumeric character.
     * End: Must end with an alphanumeric character.
     **/
    public final static Pattern VOLUME_NAME_REGEX = Pattern.compile("^(?=.{0,63}$)[a-zA-Z0-9][a-zA-Z0-9-._]*[a-zA-Z0-9]$");

    /**
     * Extracts custom labels configured through the Strimzi API resource templates. This method deals the null checks
     * and makes the code using it more easy to read.
     *
     * @param template The resource template
     * @return Map with custom labels from the template or null if not set
     */
    public static Map<String, String> labels(HasMetadataTemplate template)   {
        if (template != null
                && template.getMetadata() != null) {
            return template.getMetadata().getLabels();
        } else {
            return null;
        }
    }

    /**
     * Add additional volumes for a given pod template and a list of existing volumes.
     *
     * @param templatePod     The pod template that contains the additional volumes.
     * @param existingVolumes The list of existing volumes to which the additional volumes will be added.
     */
    public static void addAdditionalVolumes(PodTemplate templatePod, List<Volume> existingVolumes) {
        if (templatePod == null || templatePod.getVolumes() == null) {
            return;
        }

        // Extract the names and paths of the existing volumes
        List<String> existingVolumeNames = existingVolumes.stream().map(Volume::getName).toList();

        // Check if there are any invalid volume names
        List<String> invalidNames = existingVolumeNames.stream().filter(name -> !VOLUME_NAME_REGEX.matcher(name).matches()).toList();

        // Find duplicate names in the additional volumes
        List<String> duplicateNames = templatePod.getVolumes().stream()
                .map(AdditionalVolume::getName)
                .filter(existingVolumeNames::contains)
                .toList();

        // Throw an exception if there are any invalid volume names
        if (!invalidNames.isEmpty()) {
            throw new InvalidResourceException("Volume names " + invalidNames + " are invalid and do not match the pattern " + VOLUME_NAME_REGEX);
        }

        // Throw an exception if duplicates are found
        if (!duplicateNames.isEmpty()) {
            throw new InvalidResourceException("Duplicate volume names found in additional volumes: " + duplicateNames);
        }

        templatePod.getVolumes().forEach(volumeConfig -> existingVolumes.add(createVolumeFromConfig(volumeConfig)));
    }

    /**
     * Add additional volume mounts to the given list of volume mounts. Validation is performed to ensure none of the
     * additional volume mount paths are forbidden.
     *
     * @param volumeMounts           The list of volume mounts to be added to
     * @param additionalVolumeMounts The list of volume mounts to add
     * @throws RuntimeException If a forbidden mount path is used.
     */
    public static void addAdditionalVolumeMounts(List<VolumeMount> volumeMounts, List<VolumeMount> additionalVolumeMounts) {
        if (additionalVolumeMounts == null || volumeMounts == null) {
            return;
        }

        boolean isForbiddenPath = additionalVolumeMounts.stream().anyMatch(additionalVolume -> !additionalVolume.getMountPath().startsWith(ALLOWED_MOUNT_PATH));

        if (isForbiddenPath) {
            throw new InvalidResourceException(String.format("Forbidden path found in additional volumes. Should start with %s", ALLOWED_MOUNT_PATH));
        }

        volumeMounts.addAll(additionalVolumeMounts);
    }

    /**
     * Creates a kubernetes Volume object from the provided Volume configuration
     *
     * @param volumeConfig The configuration for the additional volume
     * @return A Volume object
     */
    private static Volume createVolumeFromConfig(AdditionalVolume volumeConfig) {
        VolumeBuilder volumeBuilder = new VolumeBuilder().withName(volumeConfig.getName());
        if (volumeConfig.getConfigMap() != null) {
            volumeBuilder.withNewConfigMap().withName(volumeConfig.getConfigMap().getName()).endConfigMap();
        } else if (volumeConfig.getSecret() != null) {
            volumeBuilder.withNewSecret().withSecretName(volumeConfig.getSecret().getSecretName()).endSecret();
        } else if (volumeConfig.getEmptyDir() != null) {
            volumeBuilder.withNewEmptyDir().withMedium(volumeConfig.getEmptyDir().getMedium()).endEmptyDir();
        } else if (volumeConfig.getPersistentVolumeClaim() != null) {
            volumeBuilder.withPersistentVolumeClaim(volumeConfig.getPersistentVolumeClaim());
        }

        return volumeBuilder.build();
    }

    /**
     * Extracts custom annotations configured through the Strimzi API resource templates. This method deals the null
     * checks and makes the code using it more easy to read.
     *
     * @param template  The resource template
     *
     * @return  Map with custom annotations from the template or null if not set
     */
    public static Map<String, String> annotations(HasMetadataTemplate template)   {
        if (template != null
                && template.getMetadata() != null) {
            return template.getMetadata().getAnnotations();
        } else {
            return null;
        }
    }

    /**
     * Extracts the deployment strategy configuration from the Deployment template
     *
     * @param template     Deployment template which maybe contains custom deployment strategy configuration
     * @param defaultValue The default value which should be used if the deployment strategy is not set
     * @return Custom deployment strategy or default value if not defined
     */
    public static DeploymentStrategy deploymentStrategy(DeploymentTemplate template, DeploymentStrategy defaultValue) {
        return template != null && template.getDeploymentStrategy() != null ? template.getDeploymentStrategy() : defaultValue;
    }
}
