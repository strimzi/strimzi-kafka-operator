/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.ObjectReference;
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
import io.fabric8.openshift.api.model.TagImportPolicy;
import io.fabric8.openshift.api.model.TagImportPolicyBuilder;
import io.fabric8.openshift.api.model.TagReference;
import io.fabric8.openshift.api.model.TagReferencePolicyBuilder;
import io.vertx.core.json.JsonObject;

import java.util.Map;

public class KafkaConnectS2ICluster extends KafkaConnectCluster {

    // Kafka Connect S2I configuration
    protected String sourceImageBaseName = DEFAULT_IMAGE.substring(0, DEFAULT_IMAGE.lastIndexOf(":"));
    protected String sourceImageTag = DEFAULT_IMAGE.substring(DEFAULT_IMAGE.lastIndexOf(":") + 1);
    protected String tag = "latest";
    protected boolean insecureSourceRepository = false;

    // Configuration defaults
    protected static final String DEFAULT_IMAGE =
            System.getenv().getOrDefault("STRIMZI_DEFAULT_KAFKA_CONNECT_S2I_IMAGE", "strimzi/kafka-connect-s2i:latest");

    // Configuration keys (in ConfigMap)
    public static final String KEY_INSECURE_SOURCE_REPO = "insecure-source-repo";

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

        Map<String, String> data = cm.getData();
        kafkaConnect.setReplicas(Utils.getInteger(data, KEY_REPLICAS, DEFAULT_REPLICAS));
        kafkaConnect.setImage(Utils.getNonEmptyString(data, KEY_IMAGE, DEFAULT_IMAGE));
        kafkaConnect.setResources(Resources.fromJson(data.get(KEY_RESOURCES)));
        kafkaConnect.setJvmOptions(JvmOptions.fromJson(data.get(KEY_JVM_OPTIONS)));
        kafkaConnect.setHealthCheckInitialDelay(Utils.getInteger(data, KEY_HEALTHCHECK_DELAY, DEFAULT_HEALTHCHECK_DELAY));
        kafkaConnect.setHealthCheckTimeout(Utils.getInteger(data, KEY_HEALTHCHECK_TIMEOUT, DEFAULT_HEALTHCHECK_TIMEOUT));

        kafkaConnect.setConfiguration(Utils.getKafkaConnectConfiguration(data, KEY_CONNECT_CONFIG));
        kafkaConnect.setInsecureSourceRepository(Utils.getBoolean(data, KEY_INSECURE_SOURCE_REPO, false));

        JsonObject metricsConfig = Utils.getJson(data, KEY_METRICS_CONFIG);
        kafkaConnect.setMetricsEnabled(metricsConfig != null);
        if (kafkaConnect.isMetricsEnabled()) {
            kafkaConnect.setMetricsConfig(metricsConfig);
        }

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
    public static KafkaConnectS2ICluster fromAssembly(
            String namespace, String cluster,
            DeploymentConfig dep,
            ImageStream sis) {

        KafkaConnectS2ICluster kafkaConnect =  new KafkaConnectS2ICluster(namespace, cluster, Labels.fromResource(dep));

        kafkaConnect.setReplicas(dep.getSpec().getReplicas());
        Container container = dep.getSpec().getTemplate().getSpec().getContainers().get(0);
        kafkaConnect.setHealthCheckInitialDelay(container.getReadinessProbe().getInitialDelaySeconds());
        kafkaConnect.setHealthCheckTimeout(container.getReadinessProbe().getTimeoutSeconds());

        String connectConfiguration = containerEnvVars(container).getOrDefault(ENV_VAR_KAFKA_CONNECT_CONFIGURATION, "");
        kafkaConnect.setConfiguration(new KafkaConnectConfiguration(connectConfiguration));

        String sourceImage = sis.getSpec().getTags().get(0).getFrom().getName();
        kafkaConnect.setImage(sourceImage);

        Map<String, String> vars = containerEnvVars(container);

        kafkaConnect.setMetricsEnabled(Utils.getBoolean(vars, ENV_VAR_KAFKA_CONNECT_METRICS_ENABLED, DEFAULT_KAFKA_CONNECT_METRICS_ENABLED));
        if (kafkaConnect.isMetricsEnabled()) {
            kafkaConnect.setMetricsConfigName(metricsConfigName(cluster));
        }

        TagImportPolicy policy = sis.getSpec().getTags().get(0).getImportPolicy();
        if (policy != null) {
            Boolean insecure = policy.getInsecure();
            if (insecure != null) {
                kafkaConnect.setInsecureSourceRepository(insecure);
            }
        }

        return kafkaConnect;
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
                .withPorts(getContainerPortList())
                .withLivenessProbe(createHttpProbe(healthCheckPath, REST_API_PORT_NAME, healthCheckInitialDelay, healthCheckTimeout))
                .withReadinessProbe(createHttpProbe(healthCheckPath, REST_API_PORT_NAME, healthCheckInitialDelay, healthCheckTimeout))
                .withVolumeMounts(getVolumeMounts())
                .withResources(resources())
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
                            .withVolumes(getVolumes())
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

        if (insecureSourceRepository)   {
            sourceTag.setImportPolicy(new TagImportPolicyBuilder().withInsecure(true).build());
            sourceTag.setReferencePolicy(new TagReferencePolicyBuilder().withType("Local").build());
        }

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
}
