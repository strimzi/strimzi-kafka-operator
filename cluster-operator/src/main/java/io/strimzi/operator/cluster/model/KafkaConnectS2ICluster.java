/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.openshift.api.model.BinaryBuildSource;
import io.fabric8.openshift.api.model.BuildConfig;
import io.fabric8.openshift.api.model.BuildConfigBuilder;
import io.fabric8.openshift.api.model.BuildTriggerPolicy;
import io.fabric8.openshift.api.model.DeploymentConfig;
import io.fabric8.openshift.api.model.DeploymentConfigBuilder;
import io.fabric8.openshift.api.model.DeploymentStrategy;
import io.fabric8.openshift.api.model.DeploymentStrategyBuilder;
import io.fabric8.openshift.api.model.DeploymentTriggerPolicy;
import io.fabric8.openshift.api.model.DeploymentTriggerPolicyBuilder;
import io.fabric8.openshift.api.model.ImageChangeTrigger;
import io.fabric8.openshift.api.model.ImageLookupPolicyBuilder;
import io.fabric8.openshift.api.model.ImageStream;
import io.fabric8.openshift.api.model.ImageStreamBuilder;
import io.fabric8.openshift.api.model.TagImportPolicyBuilder;
import io.fabric8.openshift.api.model.TagReference;
import io.fabric8.openshift.api.model.TagReferencePolicyBuilder;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaConnectS2IResources;
import io.strimzi.api.kafka.model.KafkaConnectS2ISpec;
import io.strimzi.operator.common.Util;

import java.util.List;
import java.util.Map;

import static io.strimzi.operator.cluster.model.ModelUtils.createHttpProbe;

public class KafkaConnectS2ICluster extends KafkaConnectCluster {

    // Kafka Connect S2I configuration
    protected String sourceImageBaseName;
    protected String sourceImageTag;
    protected String tag = "latest";
    protected boolean insecureSourceRepository = false;
    protected ResourceRequirements buildRequirements;

    /**
     * Constructor
     *
     * @param resource Kubernetes resource with metadata containing the namespace and cluster name
     */
    private KafkaConnectS2ICluster(HasMetadata resource) {
        super(resource, APPLICATION_NAME);
    }

    public static KafkaConnectS2ICluster fromCrd(KafkaConnectS2I kafkaConnectS2I, KafkaVersion.Lookup versions) {
        KafkaConnectS2ISpec spec = kafkaConnectS2I.getSpec();
        KafkaConnectS2ICluster cluster = fromSpec(spec, versions, new KafkaConnectS2ICluster(kafkaConnectS2I));

        cluster.setOwnerReference(kafkaConnectS2I);
        cluster.setInsecureSourceRepository(spec.isInsecureSourceRepository());
        cluster.setBuildResourceRequirements(spec.getBuildResources());

        return cluster;
    }

    /**
     * Generate new DeploymentConfig
     *
     * @param annotations The annotations.
     * @param isOpenShift Whether we're on OpenShift.
     * @param imagePullPolicy The image pull policy.
     * @param imagePullSecrets The image pull secrets.
     * @return Source ImageStream resource definition
     */
    public DeploymentConfig generateDeploymentConfig(Map<String, String> annotations, boolean isOpenShift, ImagePullPolicy imagePullPolicy,
                                                     List<LocalObjectReference> imagePullSecrets) {
        Container container = new ContainerBuilder()
                .withName(name)
                .withImage(image)
                .withEnv(getEnvVars())
                .withCommand("/opt/kafka/s2i/run")
                .withPorts(getContainerPortList())
                .withLivenessProbe(createHttpProbe(livenessPath, REST_API_PORT_NAME, livenessProbeOptions))
                .withReadinessProbe(createHttpProbe(readinessPath, REST_API_PORT_NAME, readinessProbeOptions))
                .withVolumeMounts(getVolumeMounts())
                .withResources(getResources())
                .withImagePullPolicy(determineImagePullPolicy(imagePullPolicy, image))
                .withSecurityContext(templateContainerSecurityContext)
                .build();

        DeploymentTriggerPolicy configChangeTrigger = new DeploymentTriggerPolicyBuilder()
                .withType("ConfigChange")
                .build();

        DeploymentTriggerPolicy imageChangeTrigger = new DeploymentTriggerPolicyBuilder()
                .withType("ImageChange")
                .withNewImageChangeParams()
                    .withAutomatic(true)
                    .withContainerNames(name)
                    .withNewFrom()
                        .withKind("ImageStreamTag")
                        .withName(image)
                    .endFrom()
                .endImageChangeParams()
                .build();

        DeploymentStrategy updateStrategy = new DeploymentStrategyBuilder()
                .withType("Rolling")
                .withNewRollingParams()
                    .withMaxSurge(new IntOrString(1))
                    .withMaxUnavailable(new IntOrString(0))
                .endRollingParams()
                .build();

        DeploymentConfig dc = new DeploymentConfigBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withLabels(getLabelsWithStrimziName(name, templateDeploymentLabels).toMap())
                    .withAnnotations(Util.mergeLabelsOrAnnotations(null, templateDeploymentAnnotations))
                    .withNamespace(namespace)
                    .withOwnerReferences(createOwnerReference())
                .endMetadata()
                .withNewSpec()
                    .withReplicas(replicas)
                    .withSelector(getSelectorLabels().toMap())
                    .withNewTemplate()
                        .withNewMetadata()
                            .withAnnotations(Util.mergeLabelsOrAnnotations(annotations, templatePodAnnotations))
                            .withLabels(getLabelsWithStrimziName(name, templatePodLabels).toMap())
                        .endMetadata()
                        .withNewSpec()
                            .withContainers(container)
                            .withVolumes(getVolumes(isOpenShift))
                            .withTolerations(getTolerations())
                            .withAffinity(getMergedAffinity())
                            .withTerminationGracePeriodSeconds(Long.valueOf(templateTerminationGracePeriodSeconds))
                            .withImagePullSecrets(templateImagePullSecrets != null ? templateImagePullSecrets : imagePullSecrets)
                            .withSecurityContext(templateSecurityContext)
                            .withPriorityClassName(templatePodPriorityClassName)
                            .withSchedulerName(templatePodSchedulerName)
                        .endSpec()
                    .endTemplate()
                    .withTriggers(configChangeTrigger, imageChangeTrigger)
                .withStrategy(updateStrategy)
                .endSpec()
                .build();
        return dc;
    }

    /**
     * Generate new source ImageStream
     *
     * @return      Source ImageStream resource definition
     */
    public ImageStream generateSourceImageStream() {
        ObjectReference image = new ObjectReference();
        image.setKind("DockerImage");
        image.setName(sourceImageBaseName + ":" + sourceImageTag);

        TagReference sourceTag = new TagReference();
        sourceTag.setName(sourceImageTag);
        sourceTag.setFrom(image);
        sourceTag.setReferencePolicy(new TagReferencePolicyBuilder().withType("Local").build());

        if (insecureSourceRepository)   {
            sourceTag.setImportPolicy(new TagImportPolicyBuilder().withInsecure(true).build());
        }

        ImageStream imageStream = new ImageStreamBuilder()
                .withNewMetadata()
                    .withName(KafkaConnectS2IResources.sourceImageStreamName(cluster))
                    .withNamespace(namespace)
                    .withLabels(getLabelsWithStrimziName(KafkaConnectS2IResources.sourceImageStreamName(cluster), null).toMap())
                    .withOwnerReferences(createOwnerReference())
                .endMetadata()
                .withNewSpec()
                    .withLookupPolicy(new ImageLookupPolicyBuilder().withLocal(false).build())
                    .withTags(sourceTag)
                .endSpec()
                .build();

        return imageStream;
    }

    /**
     * Generate new target ImageStream
     *
     * @return      Target ImageStream resource definition
     */
    public ImageStream generateTargetImageStream() {
        ImageStream imageStream = new ImageStreamBuilder()
                .withNewMetadata()
                    .withName(KafkaConnectS2IResources.targetImageStreamName(cluster))
                    .withNamespace(namespace)
                    .withLabels(getLabelsWithStrimziName(name, null).toMap())
                    .withOwnerReferences(createOwnerReference())
                .endMetadata()
                .withNewSpec()
                    .withLookupPolicy(new ImageLookupPolicyBuilder().withLocal(true).build())
                .endSpec()
                .build();

        return imageStream;
    }

    /**
     * Generate new BuildConfig
     *
     * @return      BuildConfig resource definition
     */
    public BuildConfig generateBuildConfig() {
        BuildTriggerPolicy triggerConfigChange = new BuildTriggerPolicy();
        triggerConfigChange.setType("ConfigChange");

        BuildTriggerPolicy triggerImageChange = new BuildTriggerPolicy();
        triggerImageChange.setType("ImageChange");
        triggerImageChange.setImageChange(new ImageChangeTrigger());

        BuildConfig build = new BuildConfigBuilder()
                .withNewMetadata()
                    .withName(KafkaConnectS2IResources.buildConfigName(cluster))
                    .withLabels(getLabelsWithStrimziName(name, null).toMap())
                    .withNamespace(namespace)
                    .withOwnerReferences(createOwnerReference())
                .endMetadata()
                .withNewSpec()
                    .withFailedBuildsHistoryLimit(5)
                    .withNewOutput()
                        .withNewTo()
                            .withKind("ImageStreamTag")
                            .withName(image)
                        .endTo()
                    .endOutput()
                    .withRunPolicy("Serial")
                    .withNewSource()
                        .withType("Binary")
                        .withBinary(new BinaryBuildSource())
                    .endSource()
                    .withNewStrategy()
                        .withType("Source")
                        .withNewSourceStrategy()
                            .withScripts("image:///opt/kafka/s2i")
                            .withNewFrom()
                                .withKind("ImageStreamTag")
                                .withName(KafkaConnectS2IResources.sourceImageStreamName(cluster) + ":" + sourceImageTag)
                            .endFrom()
                        .endSourceStrategy()
                    .endStrategy()
                    .withTriggers(triggerConfigChange, triggerImageChange)
                    .withSuccessfulBuildsHistoryLimit(5)
                    .withFailedBuildsHistoryLimit(5)
                    .withResources(buildRequirements)
                .endSpec()
                .build();

        return build;
    }

    @Override
    protected void setImage(String image) {
        this.sourceImageBaseName = image.substring(0, image.lastIndexOf(":"));
        this.sourceImageTag = image.substring(image.lastIndexOf(":") + 1);
        this.image = name + ":" + tag;
    }

    /**
     * @return true if the source repo for the S2I image should be treated as insecure in source ImageStream
     */
    public boolean isInsecureSourceRepository() {
        return insecureSourceRepository;
    }

    /**
     * Set whether the source repository for the S2I image should be treated as insecure
     *
     * @param insecureSourceRepository  Set to true for using insecure repository
     */
    public void setInsecureSourceRepository(boolean insecureSourceRepository) {
        this.insecureSourceRepository = insecureSourceRepository;
    }

    /**
     * Set resources requirements for the build
     *
     * @param buildRequirements requirements for the build
     */
    public void setBuildResourceRequirements(ResourceRequirements buildRequirements) {
        this.buildRequirements = buildRequirements;
    }
}
