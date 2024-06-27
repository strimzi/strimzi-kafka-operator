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
     * This is a constant that represents an allowed mountable path in the file system.
     * It is used to prevent the creation of volumes outside this path.
     */
    protected static final String ALLOWED_MOUNT_PATH = "/mnt";

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
     * Add additional volumes for a given pod template and a list of existing volumes.
     *
     * @param templatePod     The pod template that contains the additional volumes.
     * @param existingVolumes The list of existing volumes to which the additional volumes will be added.
     */
    public static void addAdditionalVolumes(PodTemplate templatePod, List<Volume> existingVolumes) {
        if (templatePod.getAdditionalVolumes() != null) {
            templatePod.getAdditionalVolumes().forEach(volumeConfig -> existingVolumes.add(createVolumeFromConfig(volumeConfig)));
        }
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
        if (additionalVolumeMounts == null) {
            return;
        }
        boolean isSensitivePath = additionalVolumeMounts.stream().anyMatch(additionalVolume -> additionalVolume.getMountPath().startsWith(SENSITIVE_PATH));

        if (isSensitivePath) {
            throw new RuntimeException("Sensitive path found in additional volumes");
        }
        

        boolean isForbiddenPath = additionalVolumeMounts.stream().anyMatch(additionalVolume -> !additionalVolume.getMountPath().startsWith(ALLOWED_MOUNT_PATH));

        if (isForbiddenPath) {
            throw new RuntimeException(String.format("Forbidden path found in additional volumes. Should start with %s", ALLOWED_MOUNT_PATH));
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
        } else if (volumeConfig.getPvc() != null) {
            volumeBuilder.withPersistentVolumeClaim(volumeConfig.getPvc());
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
