/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.strimzi.api.kafka.model.common.template.DeploymentStrategy;
import io.strimzi.api.kafka.model.common.template.DeploymentTemplate;
import io.strimzi.api.kafka.model.common.template.HasMetadataTemplate;
import io.strimzi.api.kafka.model.common.template.PodTemplate;

import java.util.List;
import java.util.Map;

/**
 * Shared methods for working with Strimzi API templates
 */
public class TemplateUtils {
    /**
     * This is a constant that represents a sensitive path in the file system.
     * It is used to prevent the creation of volumes that mount to this path.
     */
    protected static final String SENSITIVE_PATH = "/tmp";

    /**
     * Extracts custom labels configured through the Strimzi API resource templates. This method deals the null checks
     * and makes the code using it more easy to read.
     *
     * @param template  The resource template
     *
     * @return  Map with custom labels from the template or null if not set
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
     * This method is used to get additional volumes for a given pod template and a list of existing volumes. It validates
     * the additional volumes for duplicate volume names, conflicting mount paths, and sensitive paths. If the validation
     * passes, it adds the additional volume to the existing volumes list.
     *
     * @param templatePod     The pod template that contains the additional volumes.
     * @param existingVolumes The list of existing volumes to which the additional volumes will be added.
     * @return The list of volumes including the additional volumes.
     * @throws RuntimeException If there is a duplicate volume name.
     */
    public static List<Volume> getAdditionalVolumes(PodTemplate templatePod, List<Volume> existingVolumes) {
        // Extract the names of the existing volumes
        List<String> existingVolumeNames = existingVolumes.stream().map(Volume::getName).toList();

        // Check if there are any duplicates in the additional volumes' names or paths
        boolean hasDuplicate = templatePod.getAdditionalVolumes().stream().anyMatch(additionalVolume -> existingVolumeNames.contains(additionalVolume.getName()));

        // Throw an exception if there are any duplicates or sensitive paths
        if (hasDuplicate) {
            throw new RuntimeException("Duplicate volume name found in additional volumes");
        }

        // Add the additional volumes to the existing volumes list
        templatePod.getAdditionalVolumes().forEach(volumeConfig -> existingVolumes.add(createVolumeFromConfig(volumeConfig)));

        // Return the updated list of volumes
        return existingVolumes;
    }

    /**
     * Add additional volume mounts to the given list of volume mounts. Validation is performed on the additional volume
     * mounts to ensure the mount paths do not conflict with with the volume mounts in the list to be added to. Validation
     * is also performed that none of the additional volume mount paths are forbidden.
     * 
     * @param volumeMounts           The list of volume mounts to be added to
     * @param additionalVolumeMounts The list of volume mounts to add
     * @return The combined list of volume mounts
     * @throws RuntimeException If there is a conflicting mount path, or a forbidden path.
     */
    public static List<VolumeMount> addAdditionalVolumeMounts(List<VolumeMount> volumeMounts, List<VolumeMount> additionalVolumeMounts) {
        List<String> volumeMountPaths = volumeMounts.stream().map(volumeMount -> volumeMount.getMountPath()).toList();

        boolean hasDuplicate = additionalVolumeMounts.stream().anyMatch(additionalVolume -> volumeMountPaths.contains(additionalVolume.getMountPath()));
        boolean isSensitivePath = additionalVolumeMounts.stream().anyMatch(additionalVolume -> additionalVolume.getMountPath().startsWith(SENSITIVE_PATH));

        if (hasDuplicate) {
            throw new RuntimeException("Duplicate volume path found in additional volumes");
        }
        if (isSensitivePath) {
            throw new RuntimeException("Sensitive path found in additional volumes");
        }

        return volumeMounts;
    }

    /**
     * Creates a kubernetes Volume object from the provided Volume configuration
     *
     * @param volumeConfig The configuration for the additional volume
     * @return A Volume object
     */
    private static Volume createVolumeFromConfig(io.strimzi.api.kafka.model.common.template.Volume volumeConfig) {
        VolumeBuilder volumeBuilder = new VolumeBuilder().withName(volumeConfig.getName());
        if (volumeConfig.getConfigMap() != null) {
            volumeBuilder.withNewConfigMap().withName(volumeConfig.getConfigMap().getName()).endConfigMap();
        } else if (volumeConfig.getSecret() != null) {
            volumeBuilder.withNewSecret().withSecretName(volumeConfig.getSecret().getSecretName()).endSecret();
        } else if (volumeConfig.getEmptyDir() != null) {
            volumeBuilder.withNewEmptyDir().withMedium(volumeConfig.getEmptyDir().getMedium()).endEmptyDir();
        } else if (volumeConfig.getCsi() != null) {
            volumeBuilder.withCsi(volumeConfig.getCsi());
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
     * @param template      Deployment template which maybe contains custom deployment strategy configuration
     * @param defaultValue  The default value which should be used if the deployment strategy is not set
     *
     * @return  Custom deployment strategy or default value if not defined
     */
    public static DeploymentStrategy deploymentStrategy(DeploymentTemplate template, DeploymentStrategy defaultValue)  {
        return template != null && template.getDeploymentStrategy() != null ? template.getDeploymentStrategy() : defaultValue;
    }
}
