/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.resources;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.openshift.api.model.DeploymentStrategy;
import io.fabric8.openshift.api.model.DeploymentStrategyBuilder;
import io.fabric8.openshift.api.model.BinaryBuildSource;
import io.fabric8.openshift.api.model.BuildConfig;
import io.fabric8.openshift.api.model.BuildConfigBuilder;
import io.fabric8.openshift.api.model.BuildTriggerPolicy;
import io.fabric8.openshift.api.model.DeploymentConfig;
import io.fabric8.openshift.api.model.DeploymentConfigBuilder;
import io.fabric8.openshift.api.model.DeploymentTriggerPolicy;
import io.fabric8.openshift.api.model.DeploymentTriggerPolicyBuilder;
import io.fabric8.openshift.api.model.ImageChangeTrigger;
import io.fabric8.openshift.api.model.ImageLookupPolicyBuilder;
import io.fabric8.openshift.api.model.ImageStream;
import io.fabric8.openshift.api.model.ImageStreamBuilder;
import io.fabric8.openshift.api.model.TagReference;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public class KafkaConnectS2ICluster extends KafkaConnectCluster {

    public static final String TYPE = "kafka-connect-s2i";

    // Kafka Connect S2I configuration
    protected String sourceImageBaseName = DEFAULT_IMAGE.substring(0, DEFAULT_IMAGE.lastIndexOf(":"));
    protected String sourceImageTag = DEFAULT_IMAGE.substring(DEFAULT_IMAGE.lastIndexOf(":") + 1);
    protected String tag = "latest";

    // Configuration defaults
    protected static final String DEFAULT_IMAGE = "strimzi/kafka-connect-s2i:latest";

    /**
     * Constructor
     *
     * @param namespace Kubernetes/OpenShift namespace where Kafka Connect cluster resources are going to be created
     * @param cluster   overall cluster name
     */
    private KafkaConnectS2ICluster(String namespace, String cluster, Labels labels) {
        super(namespace, cluster, labels);
        setImage(DEFAULT_IMAGE);
    }

    /**
     * Create a Kafka Connect cluster from the related ConfigMap resource
     *
     * @param cm ConfigMap with cluster configuration
     * @return Kafka Connect cluster instance
     */
    public static KafkaConnectS2ICluster fromConfigMap(ConfigMap cm) {
        KafkaConnectS2ICluster kafkaConnect = new KafkaConnectS2ICluster(cm.getMetadata().getNamespace(), cm.getMetadata().getName(), Labels.fromResource(cm));

        kafkaConnect.setReplicas(Integer.parseInt(cm.getData().getOrDefault(KEY_REPLICAS, String.valueOf(DEFAULT_REPLICAS))));
        kafkaConnect.setImage(cm.getData().getOrDefault(KEY_IMAGE, DEFAULT_IMAGE));
        kafkaConnect.setHealthCheckInitialDelay(Integer.parseInt(cm.getData().getOrDefault(KEY_HEALTHCHECK_DELAY, String.valueOf(DEFAULT_HEALTHCHECK_DELAY))));
        kafkaConnect.setHealthCheckTimeout(Integer.parseInt(cm.getData().getOrDefault(KEY_HEALTHCHECK_TIMEOUT, String.valueOf(DEFAULT_HEALTHCHECK_TIMEOUT))));

        kafkaConnect.setBootstrapServers(cm.getData().getOrDefault(KEY_BOOTSTRAP_SERVERS, DEFAULT_BOOTSTRAP_SERVERS));
        kafkaConnect.setGroupId(cm.getData().getOrDefault(KEY_GROUP_ID, DEFAULT_GROUP_ID));
        kafkaConnect.setKeyConverter(cm.getData().getOrDefault(KEY_KEY_CONVERTER, DEFAULT_KEY_CONVERTER));
        kafkaConnect.setKeyConverterSchemasEnable(Boolean.parseBoolean(cm.getData().getOrDefault(KEY_KEY_CONVERTER_SCHEMAS_EXAMPLE, String.valueOf(DEFAULT_KEY_CONVERTER_SCHEMAS_EXAMPLE))));
        kafkaConnect.setValueConverter(cm.getData().getOrDefault(KEY_VALUE_CONVERTER, DEFAULT_VALUE_CONVERTER));
        kafkaConnect.setValueConverterSchemasEnable(Boolean.parseBoolean(cm.getData().getOrDefault(KEY_VALUE_CONVERTER_SCHEMAS_EXAMPLE, String.valueOf(DEFAULT_VALUE_CONVERTER_SCHEMAS_EXAMPLE))));
        kafkaConnect.setConfigStorageReplicationFactor(Integer.parseInt(cm.getData().getOrDefault(KEY_CONFIG_STORAGE_REPLICATION_FACTOR, String.valueOf(DEFAULT_CONFIG_STORAGE_REPLICATION_FACTOR))));
        kafkaConnect.setOffsetStorageReplicationFactor(Integer.parseInt(cm.getData().getOrDefault(KEY_OFFSET_STORAGE_REPLICATION_FACTOR, String.valueOf(DEFAULT_OFFSET_STORAGE_REPLICATION_FACTOR))));
        kafkaConnect.setStatusStorageReplicationFactor(Integer.parseInt(cm.getData().getOrDefault(KEY_STATUS_STORAGE_REPLICATION_FACTOR, String.valueOf(DEFAULT_STATUS_STORAGE_REPLICATION_FACTOR))));

        return kafkaConnect;
    }

    /**
     * Create a Kafka Connect cluster from the deployed Deployment resource
     *
     * @param namespace Kubernetes/OpenShift namespace where cluster resources belong to
     * @param cluster   overall cluster name
     * @param dep The deployment from which to recover the cluster state
     * @param sis ImageStream
     * @return  Kafka Connect cluster instance
     */
    public static KafkaConnectS2ICluster fromDeployment(
            String namespace, String cluster,
            DeploymentConfig dep,
            ImageStream sis) {

        KafkaConnectS2ICluster kafkaConnect =  new KafkaConnectS2ICluster(namespace, cluster, Labels.fromResource(dep));

        kafkaConnect.setReplicas(dep.getSpec().getReplicas());
        kafkaConnect.setHealthCheckInitialDelay(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds());
        kafkaConnect.setHealthCheckTimeout(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds());

        Map<String, String> vars = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().stream().collect(
                Collectors.toMap(EnvVar::getName, EnvVar::getValue));

        kafkaConnect.setBootstrapServers(vars.getOrDefault(KEY_BOOTSTRAP_SERVERS, DEFAULT_BOOTSTRAP_SERVERS));
        kafkaConnect.setGroupId(vars.getOrDefault(KEY_GROUP_ID, DEFAULT_GROUP_ID));
        kafkaConnect.setKeyConverter(vars.getOrDefault(KEY_KEY_CONVERTER, DEFAULT_KEY_CONVERTER));
        kafkaConnect.setKeyConverterSchemasEnable(Boolean.parseBoolean(vars.getOrDefault(KEY_KEY_CONVERTER_SCHEMAS_EXAMPLE, String.valueOf(DEFAULT_KEY_CONVERTER_SCHEMAS_EXAMPLE))));
        kafkaConnect.setValueConverter(vars.getOrDefault(KEY_VALUE_CONVERTER, DEFAULT_VALUE_CONVERTER));
        kafkaConnect.setValueConverterSchemasEnable(Boolean.parseBoolean(vars.getOrDefault(KEY_VALUE_CONVERTER_SCHEMAS_EXAMPLE, String.valueOf(DEFAULT_VALUE_CONVERTER_SCHEMAS_EXAMPLE))));
        kafkaConnect.setConfigStorageReplicationFactor(Integer.parseInt(vars.getOrDefault(KEY_CONFIG_STORAGE_REPLICATION_FACTOR, String.valueOf(DEFAULT_CONFIG_STORAGE_REPLICATION_FACTOR))));
        kafkaConnect.setOffsetStorageReplicationFactor(Integer.parseInt(vars.getOrDefault(KEY_OFFSET_STORAGE_REPLICATION_FACTOR, String.valueOf(DEFAULT_OFFSET_STORAGE_REPLICATION_FACTOR))));
        kafkaConnect.setStatusStorageReplicationFactor(Integer.parseInt(vars.getOrDefault(KEY_STATUS_STORAGE_REPLICATION_FACTOR, String.valueOf(DEFAULT_STATUS_STORAGE_REPLICATION_FACTOR))));

        String sourceImage = sis.getSpec().getTags().get(0).getFrom().getName();
        kafkaConnect.setImage(sourceImage);

        return kafkaConnect;
    }

    /**
     * Return the differences between the current Kafka Connect cluster and the deployed one
     *
     * @param dep   DeploymentConfig resource
     * @param sis   Source ImageStream resource
     * @param tis   Target ImageStream resource
     * @param bc    BuildConfig resource
     * @return  ClusterDiffResult instance with differences
     */
    public ClusterDiffResult diff(DeploymentConfig dep, ImageStream sis, ImageStream tis, BuildConfig bc) {
        boolean scaleUp = false;
        boolean scaleDown = false;
        boolean different = false;
        boolean rollingUpdate = false;
        boolean metricsChanged = false;

        if (replicas > dep.getSpec().getReplicas()) {
            log.info("Diff: Expected replicas {}, actual replicas {}", replicas, dep.getSpec().getReplicas());
            scaleUp = true;
        } else if (replicas < dep.getSpec().getReplicas()) {
            log.info("Diff: Expected replicas {}, actual replicas {}", replicas, dep.getSpec().getReplicas());
            scaleDown = true;
        }

        if (!getLabelsWithName().equals(dep.getMetadata().getLabels()))    {
            log.info("Diff: Expected labels {}, actual labels {}", getLabelsWithName(), dep.getMetadata().getLabels());
            different = true;
        }

        Map<String, String> vars = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().stream().collect(
                Collectors.toMap(EnvVar::getName, EnvVar::getValue));

        if (!bootstrapServers.equals(vars.getOrDefault(KEY_BOOTSTRAP_SERVERS, DEFAULT_BOOTSTRAP_SERVERS))
                || !groupId.equals(vars.getOrDefault(KEY_GROUP_ID, DEFAULT_GROUP_ID))
                || !keyConverter.equals(vars.getOrDefault(KEY_KEY_CONVERTER, DEFAULT_KEY_CONVERTER))
                || keyConverterSchemasEnable != Boolean.parseBoolean(vars.getOrDefault(KEY_KEY_CONVERTER_SCHEMAS_EXAMPLE, String.valueOf(DEFAULT_KEY_CONVERTER_SCHEMAS_EXAMPLE)))
                || !valueConverter.equals(vars.getOrDefault(KEY_VALUE_CONVERTER, DEFAULT_VALUE_CONVERTER))
                || valueConverterSchemasEnable != Boolean.parseBoolean(vars.getOrDefault(KEY_VALUE_CONVERTER_SCHEMAS_EXAMPLE, String.valueOf(DEFAULT_VALUE_CONVERTER_SCHEMAS_EXAMPLE)))
                || configStorageReplicationFactor != Integer.parseInt(vars.getOrDefault(KEY_CONFIG_STORAGE_REPLICATION_FACTOR, String.valueOf(DEFAULT_CONFIG_STORAGE_REPLICATION_FACTOR)))
                || offsetStorageReplicationFactor != Integer.parseInt(vars.getOrDefault(KEY_OFFSET_STORAGE_REPLICATION_FACTOR, String.valueOf(DEFAULT_OFFSET_STORAGE_REPLICATION_FACTOR)))
                || statusStorageReplicationFactor != Integer.parseInt(vars.getOrDefault(KEY_STATUS_STORAGE_REPLICATION_FACTOR, String.valueOf(DEFAULT_STATUS_STORAGE_REPLICATION_FACTOR)))) {
            log.info("Diff: Kafka Connect options changed");
            different = true;
            rollingUpdate = true;
        }

        if (healthCheckInitialDelay != dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds()
                || healthCheckTimeout != dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds()) {
            log.info("Diff: Kafka Connect healthcheck timing changed");
            different = true;
            rollingUpdate = true;
        }

        // S2I diff
        if (!getImage().equals(dep.getSpec().getTriggers().get(1).getImageChangeParams().getFrom().getName())) {
            log.info("Diff: Expected trigger from {}, actual image {}", getImage(), dep.getSpec().getTriggers().get(1).getImageChangeParams().getFrom().getName());
            different = true;
        }

        if (!getLabelsWithName(getSourceImageStreamName()).equals(sis.getMetadata().getLabels())
                || !getLabelsWithName().equals(tis.getMetadata().getLabels())
                || !getLabelsWithName().equals(bc.getMetadata().getLabels())) {
            log.info("Diff: Kafka Connect S2I labels do not match");
            different = true;
        }

        if (!image.equals(bc.getSpec().getOutput().getTo().getName())
                || !(getSourceImageStreamName() + ":" + sourceImageTag).equals(bc.getSpec().getStrategy().getSourceStrategy().getFrom().getName()))    {
            log.info("Diff: Kafka Connect S2I BuildConfig does not match");
            different = true;
        }

        if (!sourceImageTag.equals(sis.getSpec().getTags().get(0).getName())
                || !(sourceImageBaseName + ":" + sourceImageTag).equals(sis.getSpec().getTags().get(0).getFrom().getName()))   {
            log.info("Diff: Kafka Connect S2I source image name in BuildConfig or source ImageStream do not match");
            different = true;
        }

        return new ClusterDiffResult(different, rollingUpdate, scaleUp, scaleDown, metricsChanged);
    }

    /**
     * Generate new DeploymentConfig
     *
     * @return      Source ImageStream resource definition
     */
    public DeploymentConfig generateDeploymentConfig() {
        Container container = new ContainerBuilder()
                .withName(name)
                .withImage(image)
                .withEnv(getEnvVars())
                .withPorts(Collections.singletonList(createContainerPort(REST_API_PORT_NAME, REST_API_PORT, "TCP")))
                .withLivenessProbe(createHttpProbe(healthCheckPath, REST_API_PORT_NAME, healthCheckInitialDelay, healthCheckTimeout))
                .withReadinessProbe(createHttpProbe(healthCheckPath, REST_API_PORT_NAME, healthCheckInitialDelay, healthCheckTimeout))
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
                    .withLabels(getLabelsWithName())
                    .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(replicas)
                    .withNewTemplate()
                        .withNewMetadata()
                            .withLabels(getLabelsWithName())
                        .endMetadata()
                        .withNewSpec()
                            .withContainers(container)
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

        ImageStream imageStream = new ImageStreamBuilder()
                .withNewMetadata()
                    .withName(getSourceImageStreamName())
                    .withNamespace(namespace)
                    .withLabels(getLabelsWithName(getSourceImageStreamName()))
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
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(getLabelsWithName())
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
                    .withName(name)
                    .withLabels(getLabelsWithName())
                    .withNamespace(namespace)
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
                            .withNewFrom()
                                .withKind("ImageStreamTag")
                                .withName(getSourceImageStreamName() + ":" + sourceImageTag)
                            .endFrom()
                        .endSourceStrategy()
                    .endStrategy()
                    .withTriggers(triggerConfigChange, triggerImageChange)
                .endSpec()
                .build();

        return build;
    }

    /**
     * Patches DeploymentConfig
     *
     * @param dep   Exsiting DeploymentConfig which should be patched
     * @return      Source ImageStream resource definition
     */
    public DeploymentConfig patchDeploymentConfig(DeploymentConfig dep) {
        // Do not update image or trigger image - it will cause problem with rolling updates
        dep.getMetadata().setLabels(getLabelsWithName());
        dep.getSpec().getTemplate().getMetadata().setLabels(getLabelsWithName());
        dep.getSpec().getTemplate().getSpec().getContainers().get(0).setLivenessProbe(createHttpProbe(healthCheckPath, REST_API_PORT_NAME, healthCheckInitialDelay, healthCheckTimeout));
        dep.getSpec().getTemplate().getSpec().getContainers().get(0).setReadinessProbe(createHttpProbe(healthCheckPath, REST_API_PORT_NAME, healthCheckInitialDelay, healthCheckTimeout));
        dep.getSpec().getTemplate().getSpec().getContainers().get(0).setEnv(getEnvVars());

        return dep;
    }

    /**
     * Patches existing source ImageStream with latest changes
     *
     * @param is    Existing source ImageStream which should be patched
     * @return      Patched ImageStream resource definition
     */
    public ImageStream patchSourceImageStream(ImageStream is) {
        is.getMetadata().setLabels(getLabelsWithName(getSourceImageStreamName()));
        is.getSpec().getTags().get(0).setName(sourceImageTag);
        is.getSpec().getTags().get(0).getFrom().setName(sourceImageBaseName + ":" + sourceImageTag);

        return is;
    }

    /**
     * Patches existing target ImageStream with latest changes
     *
     * @param is    Existing target ImageStream which should be patched
     * @return      Patched ImageStream resource definition
     */
    public ImageStream patchTargetImageStream(ImageStream is) {
        is.getMetadata().setLabels(getLabelsWithName());

        return is;
    }

    /**
     * Patches existing BuildConfig with latest changes
     *
     * @param bc    Existing BuildConfig which should be patched
     * @return      Patched BuildConfig resource definition
     */
    public BuildConfig patchBuildConfig(BuildConfig bc) {
        bc.getMetadata().setLabels(getLabelsWithName());
        bc.getSpec().getStrategy().getSourceStrategy().getFrom().setName(getSourceImageStreamName() + ":" + sourceImageTag);

        return bc;
    }

    /**
     * Generates the name of the source ImageStream
     *
     * @return               Name of the source ImageStream instance
     */
    public String getSourceImageStreamName() {
        return getSourceImageStreamName(name);
    }

    /**
     * Generates the name of the source ImageStream
     *
     * @param baseName       Name of the Kafka Connect cluster
     * @return               Name of the source ImageStream instance
     */
    public static String getSourceImageStreamName(String baseName) {
        return baseName + "-source";
    }

    @Override
    protected void setImage(String image) {
        this.sourceImageBaseName = image.substring(0, image.lastIndexOf(":"));
        this.sourceImageTag = image.substring(image.lastIndexOf(":") + 1);
        this.image = name + ":" + tag;

    }
}
