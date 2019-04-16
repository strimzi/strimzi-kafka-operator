/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.rbac.KubernetesRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.KubernetesRoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.KubernetesRoleRef;
import io.fabric8.kubernetes.api.model.rbac.KubernetesRoleRefBuilder;
import io.fabric8.kubernetes.api.model.rbac.KubernetesSubject;
import io.fabric8.kubernetes.api.model.rbac.KubernetesSubjectBuilder;
import io.strimzi.api.kafka.model.EntityOperatorSpec;
import io.strimzi.api.kafka.model.EntityTopicOperatorSpec;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.operator.common.model.Labels;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * Represents the Topic Operator deployment
 */
public class EntityTopicOperator extends AbstractModel {

    protected static final String TOPIC_OPERATOR_CONTAINER_NAME = "topic-operator";
    private static final String NAME_SUFFIX = "-entity-topic-operator";
    protected static final String METRICS_AND_LOG_CONFIG_SUFFIX = NAME_SUFFIX + "-config";

    // Port configuration
    protected static final int HEALTHCHECK_PORT = 8080;
    protected static final String HEALTHCHECK_PORT_NAME = "healthcheck";

    // Topic Operator configuration keys
    public static final String ENV_VAR_RESOURCE_LABELS = "STRIMZI_RESOURCE_LABELS";
    public static final String ENV_VAR_KAFKA_BOOTSTRAP_SERVERS = "STRIMZI_KAFKA_BOOTSTRAP_SERVERS";
    public static final String ENV_VAR_ZOOKEEPER_CONNECT = "STRIMZI_ZOOKEEPER_CONNECT";
    public static final String ENV_VAR_WATCHED_NAMESPACE = "STRIMZI_NAMESPACE";
    public static final String ENV_VAR_FULL_RECONCILIATION_INTERVAL_MS = "STRIMZI_FULL_RECONCILIATION_INTERVAL_MS";
    public static final String ENV_VAR_ZOOKEEPER_SESSION_TIMEOUT_MS = "STRIMZI_ZOOKEEPER_SESSION_TIMEOUT_MS";
    public static final String ENV_VAR_TOPIC_METADATA_MAX_ATTEMPTS = "STRIMZI_TOPIC_METADATA_MAX_ATTEMPTS";
    public static final String ENV_VAR_TLS_ENABLED = "STRIMZI_TLS_ENABLED";

    // Kafka bootstrap servers and Zookeeper nodes can't be specified in the JSON
    private String kafkaBootstrapServers;
    private String zookeeperConnect;

    private String watchedNamespace;
    private int reconciliationIntervalMs;
    private int zookeeperSessionTimeoutMs;
    private String resourceLabels;
    private int topicMetadataMaxAttempts;

    /**
     * @param namespace Kubernetes/OpenShift namespace where cluster resources are going to be created
     * @param cluster overall cluster name
     * @param labels
     */
    protected EntityTopicOperator(String namespace, String cluster, Labels labels) {
        super(namespace, cluster, labels);
        this.name = topicOperatorName(cluster);
        this.readinessPath = "/";
        this.readinessTimeout = EntityTopicOperatorSpec.DEFAULT_HEALTHCHECK_TIMEOUT;
        this.readinessInitialDelay = EntityTopicOperatorSpec.DEFAULT_HEALTHCHECK_DELAY;
        this.livenessPath = "/";
        this.livenessTimeout = EntityTopicOperatorSpec.DEFAULT_HEALTHCHECK_TIMEOUT;
        this.livenessInitialDelay = EntityTopicOperatorSpec.DEFAULT_HEALTHCHECK_DELAY;

        // create a default configuration
        this.kafkaBootstrapServers = defaultBootstrapServers(cluster);
        this.zookeeperConnect = defaultZookeeperConnect(cluster);
        this.watchedNamespace = namespace;
        this.reconciliationIntervalMs = EntityTopicOperatorSpec.DEFAULT_FULL_RECONCILIATION_INTERVAL_SECONDS * 1_000;
        this.zookeeperSessionTimeoutMs = EntityTopicOperatorSpec.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_SECONDS * 1_000;
        this.resourceLabels = ModelUtils.defaultResourceLabels(cluster);
        this.topicMetadataMaxAttempts = EntityTopicOperatorSpec.DEFAULT_TOPIC_METADATA_MAX_ATTEMPTS;

        this.ancillaryConfigName = metricAndLogConfigsName(cluster);
        this.logAndMetricsConfigVolumeName = "entity-topic-operator-metrics-and-logging";
        this.logAndMetricsConfigMountPath = "/opt/topic-operator/custom-config/";
    }

    public void setWatchedNamespace(String watchedNamespace) {
        this.watchedNamespace = watchedNamespace;
    }

    public String getWatchedNamespace() {
        return watchedNamespace;
    }

    public void setResourceLabels(String resourceLabels) {
        this.resourceLabels = resourceLabels;
    }

    public String getResourceLabels() {
        return resourceLabels;
    }

    public void setReconciliationIntervalMs(int reconciliationIntervalMs) {
        this.reconciliationIntervalMs = reconciliationIntervalMs;
    }

    public int getReconciliationIntervalMs() {
        return reconciliationIntervalMs;
    }

    public void setZookeeperSessionTimeoutMs(int zookeeperSessionTimeoutMs) {
        this.zookeeperSessionTimeoutMs = zookeeperSessionTimeoutMs;
    }

    public int getZookeeperSessionTimeoutMs() {
        return zookeeperSessionTimeoutMs;
    }

    public void setKafkaBootstrapServers(String kafkaBootstrapServers) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
    }

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public void setTopicMetadataMaxAttempts(int topicMetadataMaxAttempts) {
        this.topicMetadataMaxAttempts = topicMetadataMaxAttempts;
    }

    public int getTopicMetadataMaxAttempts() {
        return topicMetadataMaxAttempts;
    }

    protected static String defaultZookeeperConnect(String cluster) {
        return String.format("%s:%d", "localhost", EntityTopicOperatorSpec.DEFAULT_ZOOKEEPER_PORT);
    }

    public void setZookeeperConnect(String zookeeperConnect) {
        this.zookeeperConnect = zookeeperConnect;
    }

    public String getZookeeperConnect() {
        return zookeeperConnect;
    }

    protected static String defaultBootstrapServers(String cluster) {
        return KafkaCluster.serviceName(cluster) + ":" + EntityTopicOperatorSpec.DEFAULT_BOOTSTRAP_SERVERS_PORT;
    }

    public static String topicOperatorName(String cluster) {
        return cluster + NAME_SUFFIX;
    }

    public static String metricAndLogConfigsName(String cluster) {
        return cluster + METRICS_AND_LOG_CONFIG_SUFFIX;
    }

    /**
     * Get the name of the TO role binding given the name of the {@code cluster}.
     */
    public static String roleBindingName(String cluster) {
        return "strimzi-" + cluster + "-entity-topic-operator";
    }

    @Override
    protected String getDefaultLogConfigFileName() {
        return "entityTopicOperatorDefaultLoggingProperties";
    }

    @Override
    String getAncillaryConfigMapKeyLogConfig() {
        return "log4j2.properties";
    }

    /**
     * Create an Entity Topic Operator from given desired resource
     *
     * @param kafkaAssembly desired resource with cluster configuration containing the Entity Topic Operator one
     * @return Entity Topic Operator instance, null if not configured in the ConfigMap
     */
    public static EntityTopicOperator fromCrd(Kafka kafkaAssembly) {
        EntityTopicOperator result = null;
        EntityOperatorSpec entityOperatorSpec = kafkaAssembly.getSpec().getEntityOperator();
        if (entityOperatorSpec != null) {

            EntityTopicOperatorSpec topicOperatorSpec = entityOperatorSpec.getTopicOperator();
            if (topicOperatorSpec != null) {

                String namespace = kafkaAssembly.getMetadata().getNamespace();
                result = new EntityTopicOperator(
                        namespace,
                        kafkaAssembly.getMetadata().getName(),
                        Labels.fromResource(kafkaAssembly).withKind(kafkaAssembly.getKind()));

                result.setOwnerReference(kafkaAssembly);
                String image = topicOperatorSpec.getImage();
                if (image == null) {
                    image = System.getenv().getOrDefault("STRIMZI_DEFAULT_TOPIC_OPERATOR_IMAGE", "strimzi/operator:latest");
                }
                result.setImage(image);
                result.setWatchedNamespace(topicOperatorSpec.getWatchedNamespace() != null ? topicOperatorSpec.getWatchedNamespace() : namespace);
                result.setReconciliationIntervalMs(topicOperatorSpec.getReconciliationIntervalSeconds() * 1_000);
                result.setZookeeperSessionTimeoutMs(topicOperatorSpec.getZookeeperSessionTimeoutSeconds() * 1_000);
                result.setTopicMetadataMaxAttempts(topicOperatorSpec.getTopicMetadataMaxAttempts());
                result.setLogging(topicOperatorSpec.getLogging());
                result.setGcLoggingEnabled(topicOperatorSpec.getJvmOptions() == null ? true : topicOperatorSpec.getJvmOptions().isGcLoggingEnabled());
                result.setResources(topicOperatorSpec.getResources());
            }
        }
        return result;
    }

    @Override
    protected List<Container> getContainers(ImagePullPolicy imagePullPolicy) {

        return singletonList(new ContainerBuilder()
                .withName(TOPIC_OPERATOR_CONTAINER_NAME)
                .withImage(getImage())
                .withArgs("/opt/strimzi/bin/topic_operator_run.sh")
                .withEnv(getEnvVars())
                .withPorts(singletonList(createContainerPort(HEALTHCHECK_PORT_NAME, HEALTHCHECK_PORT, "TCP")))
                .withLivenessProbe(createHttpProbe(livenessPath + "healthy", HEALTHCHECK_PORT_NAME, livenessInitialDelay, livenessTimeout))
                .withReadinessProbe(createHttpProbe(readinessPath + "ready", HEALTHCHECK_PORT_NAME, readinessInitialDelay, readinessTimeout))
                .withResources(getResources())
                .withVolumeMounts(getVolumeMounts())
                .withImagePullPolicy(determineImagePullPolicy(imagePullPolicy, getImage()))
                .build());
    }

    @Override
    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(buildEnvVar(ENV_VAR_RESOURCE_LABELS, resourceLabels));
        varList.add(buildEnvVar(ENV_VAR_KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServers));
        varList.add(buildEnvVar(ENV_VAR_ZOOKEEPER_CONNECT, zookeeperConnect));
        varList.add(buildEnvVar(ENV_VAR_WATCHED_NAMESPACE, watchedNamespace));
        varList.add(buildEnvVar(ENV_VAR_FULL_RECONCILIATION_INTERVAL_MS, Integer.toString(reconciliationIntervalMs)));
        varList.add(buildEnvVar(ENV_VAR_ZOOKEEPER_SESSION_TIMEOUT_MS, Integer.toString(zookeeperSessionTimeoutMs)));
        varList.add(buildEnvVar(ENV_VAR_TOPIC_METADATA_MAX_ATTEMPTS, String.valueOf(topicMetadataMaxAttempts)));
        varList.add(buildEnvVar(ENV_VAR_TLS_ENABLED, Boolean.toString(true)));
        varList.add(buildEnvVar(ENV_VAR_STRIMZI_GC_LOG_ENABLED, String.valueOf(gcLoggingEnabled)));
        return varList;
    }

    public List<Volume> getVolumes() {
        return singletonList(createConfigMapVolume(logAndMetricsConfigVolumeName, ancillaryConfigName));
    }

    private List<VolumeMount> getVolumeMounts() {
        return asList(createVolumeMount(logAndMetricsConfigVolumeName, logAndMetricsConfigMountPath),
            createVolumeMount(EntityOperator.TLS_SIDECAR_EO_CERTS_VOLUME_NAME, EntityOperator.TLS_SIDECAR_EO_CERTS_VOLUME_MOUNT),
            createVolumeMount(EntityOperator.TLS_SIDECAR_CA_CERTS_VOLUME_NAME, EntityOperator.TLS_SIDECAR_CA_CERTS_VOLUME_MOUNT));
    }

    public KubernetesRoleBinding generateRoleBinding(String namespace, String watchedNamespace) {
        KubernetesSubject ks = new KubernetesSubjectBuilder()
                .withKind("ServiceAccount")
                .withName(EntityOperator.entityOperatorServiceAccountName(cluster))
                .withNamespace(namespace)
                .build();

        KubernetesRoleRef roleRef = new KubernetesRoleRefBuilder()
                .withName(EntityOperator.EO_CLUSTER_ROLE_NAME)
                .withApiGroup("rbac.authorization.k8s.io")
                .withKind("ClusterRole")
                .build();

        KubernetesRoleBinding rb = new KubernetesRoleBindingBuilder()
                .withNewMetadata()
                    .withName(roleBindingName(cluster))
                    .withNamespace(watchedNamespace)
                    .withOwnerReferences(createOwnerReference())
                    .withLabels(labels.toMap())
                .endMetadata()
                .withRoleRef(roleRef)
                .withSubjects(singletonList(ks))
                .build();

        return rb;
    }
}
