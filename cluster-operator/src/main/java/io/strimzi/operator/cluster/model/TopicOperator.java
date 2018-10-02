/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;


import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.DeploymentStrategy;
import io.fabric8.kubernetes.api.model.extensions.DeploymentStrategyBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.Resources;
import io.strimzi.api.kafka.model.TlsSidecar;
import io.strimzi.api.kafka.model.TopicOperatorSpec;
import io.strimzi.certs.CertAndKey;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.RoleBindingOperator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;

/**
 * Represents the topic operator deployment
 */
@Deprecated
public class TopicOperator extends AbstractModel {

    /**
     * The default kind of CMs that the Topic Operator will be configured to watch for
     */
    public static final String TOPIC_CM_KIND = "topic";

    protected static final String TOPIC_OPERATOR_NAME = "topic-operator";
    private static final String NAME_SUFFIX = "-topic-operator";
    private static final String CERTS_SUFFIX = NAME_SUFFIX + "-certs";
    protected static final String TLS_SIDECAR_NAME = "tls-sidecar";
    protected static final String TLS_SIDECAR_EO_CERTS_VOLUME_NAME = "eo-certs";
    protected static final String TLS_SIDECAR_EO_CERTS_VOLUME_MOUNT = "/etc/tls-sidecar/eo-certs/";
    protected static final String TLS_SIDECAR_CA_CERTS_VOLUME_NAME = "cluster-ca-certs";
    protected static final String TLS_SIDECAR_CA_CERTS_VOLUME_MOUNT = "/etc/tls-sidecar/cluster-ca-certs/";


    protected static final String METRICS_AND_LOG_CONFIG_SUFFIX = NAME_SUFFIX + "-config";

    // Port configuration
    protected static final int HEALTHCHECK_PORT = 8080;
    protected static final String HEALTHCHECK_PORT_NAME = "healthcheck";

    // Configuration defaults


    // Topic Operator configuration keys
    public static final String ENV_VAR_RESOURCE_LABELS = "STRIMZI_RESOURCE_LABELS";
    public static final String ENV_VAR_KAFKA_BOOTSTRAP_SERVERS = "STRIMZI_KAFKA_BOOTSTRAP_SERVERS";
    public static final String ENV_VAR_ZOOKEEPER_CONNECT = "STRIMZI_ZOOKEEPER_CONNECT";
    public static final String ENV_VAR_WATCHED_NAMESPACE = "STRIMZI_NAMESPACE";
    public static final String ENV_VAR_FULL_RECONCILIATION_INTERVAL_MS = "STRIMZI_FULL_RECONCILIATION_INTERVAL_MS";
    public static final String ENV_VAR_ZOOKEEPER_SESSION_TIMEOUT_MS = "STRIMZI_ZOOKEEPER_SESSION_TIMEOUT_MS";
    public static final String ENV_VAR_TOPIC_METADATA_MAX_ATTEMPTS = "STRIMZI_TOPIC_METADATA_MAX_ATTEMPTS";
    public static final String ENV_VAR_TLS_ENABLED = "STRIMZI_TLS_ENABLED";
    public static final String TO_CLUSTER_ROLE_NAME = "strimzi-topic-operator";

    // Kafka bootstrap servers and Zookeeper nodes can't be specified in the JSON
    private String kafkaBootstrapServers;
    private String zookeeperConnect;

    private String watchedNamespace;
    private int reconciliationIntervalMs;
    private int zookeeperSessionTimeoutMs;
    private String topicConfigMapLabels;
    private int topicMetadataMaxAttempts;

    private TlsSidecar tlsSidecar;

    /**
     * @param namespace Kubernetes/OpenShift namespace where cluster resources are going to be created
     * @param cluster   overall cluster name
     */
    protected TopicOperator(String namespace, String cluster, Labels labels) {

        super(namespace, cluster, labels);
        this.name = topicOperatorName(cluster);
        this.image = TopicOperatorSpec.DEFAULT_IMAGE;
        this.replicas = TopicOperatorSpec.DEFAULT_REPLICAS;
        this.readinessPath = "/";
        this.readinessTimeout = TopicOperatorSpec.DEFAULT_HEALTHCHECK_TIMEOUT;
        this.readinessInitialDelay = TopicOperatorSpec.DEFAULT_HEALTHCHECK_DELAY;
        this.livenessPath = "/";
        this.livenessTimeout = TopicOperatorSpec.DEFAULT_HEALTHCHECK_TIMEOUT;
        this.livenessInitialDelay = TopicOperatorSpec.DEFAULT_HEALTHCHECK_DELAY;

        // create a default configuration
        this.kafkaBootstrapServers = defaultBootstrapServers(cluster);
        this.zookeeperConnect = defaultZookeeperConnect(cluster);
        this.watchedNamespace = namespace;
        this.reconciliationIntervalMs = TopicOperatorSpec.DEFAULT_FULL_RECONCILIATION_INTERVAL_SECONDS * 1_000;
        this.zookeeperSessionTimeoutMs = TopicOperatorSpec.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_SECONDS * 1_000;
        this.topicConfigMapLabels = defaultTopicConfigMapLabels(cluster);
        this.topicMetadataMaxAttempts = TopicOperatorSpec.DEFAULT_TOPIC_METADATA_MAX_ATTEMPTS;

        this.ancillaryConfigName = metricAndLogConfigsName(cluster);
        this.logAndMetricsConfigVolumeName = "topic-operator-metrics-and-logging";
        this.logAndMetricsConfigMountPath = "/opt/topic-operator/custom-config/";
        this.validLoggerFields = getDefaultLogConfig();
    }


    public void setWatchedNamespace(String watchedNamespace) {
        this.watchedNamespace = watchedNamespace;
    }

    public String getWatchedNamespace() {
        return watchedNamespace;
    }

    public void setTopicConfigMapLabels(String topicConfigMapLabels) {
        this.topicConfigMapLabels = topicConfigMapLabels;
    }

    public String getTopicConfigMapLabels() {
        return topicConfigMapLabels;
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

    public void setZookeeperConnect(String zookeeperConnect) {
        this.zookeeperConnect = zookeeperConnect;
    }

    public String getZookeeperConnect() {
        return zookeeperConnect;
    }

    public void setTopicMetadataMaxAttempts(int topicMetadataMaxAttempts) {
        this.topicMetadataMaxAttempts = topicMetadataMaxAttempts;
    }

    public int getTopicMetadataMaxAttempts() {
        return topicMetadataMaxAttempts;
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
        return "strimzi-" + cluster + "-topic-operator";
    }

    protected static String defaultZookeeperConnect(String cluster) {
        return ZookeeperCluster.serviceName(cluster) + ":" + TopicOperatorSpec.DEFAULT_ZOOKEEPER_PORT;
    }

    protected static String defaultBootstrapServers(String cluster) {
        return KafkaCluster.serviceName(cluster) + ":" + TopicOperatorSpec.DEFAULT_BOOTSTRAP_SERVERS_PORT;
    }

    protected static String defaultTopicConfigMapLabels(String cluster) {
        return String.format("%s=%s",
                Labels.STRIMZI_CLUSTER_LABEL, cluster);
    }

    public static String secretName(String cluster) {
        return cluster + io.strimzi.operator.cluster.model.TopicOperator.CERTS_SUFFIX;
    }

    /**
     * Create a Topic Operator from given desired resource
     *
     * @param kafkaAssembly desired resource with cluster configuration containing the topic operator one
     * @return Topic Operator instance, null if not configured in the ConfigMap
     */
    public static TopicOperator fromCrd(Kafka kafkaAssembly) {
        TopicOperator result;
        if (kafkaAssembly.getSpec().getTopicOperator() != null) {
            String namespace = kafkaAssembly.getMetadata().getNamespace();
            result = new TopicOperator(
                    namespace,
                    kafkaAssembly.getMetadata().getName(),
                    Labels.fromResource(kafkaAssembly).withKind(kafkaAssembly.getKind()));
            TopicOperatorSpec tcConfig = kafkaAssembly.getSpec().getTopicOperator();

            result.setOwnerReference(kafkaAssembly);
            result.setImage(tcConfig.getImage());
            result.setWatchedNamespace(tcConfig.getWatchedNamespace() != null ? tcConfig.getWatchedNamespace() : namespace);
            result.setReconciliationIntervalMs(tcConfig.getReconciliationIntervalSeconds() * 1_000);
            result.setZookeeperSessionTimeoutMs(tcConfig.getZookeeperSessionTimeoutSeconds() * 1_000);
            result.setTopicMetadataMaxAttempts(tcConfig.getTopicMetadataMaxAttempts());
            result.setLogging(tcConfig.getLogging());
            result.setResources(tcConfig.getResources());
            result.setUserAffinity(tcConfig.getAffinity());
            result.setTlsSidecar(tcConfig.getTlsSidecar());
        } else {
            result = null;
        }
        return result;
    }

    public Deployment generateDeployment() {
        DeploymentStrategy updateStrategy = new DeploymentStrategyBuilder()
                .withType("Recreate")
                .build();

        return createDeployment(
                updateStrategy,
                Collections.emptyMap(),
                Collections.emptyMap(),
                getMergedAffinity(),
                getInitContainers(),
                getContainers(),
                getVolumes()
        );
    }

    @Override
    protected List<Container> getContainers() {
        List<Container> containers = new ArrayList<>();
        Container container = new ContainerBuilder()
                .withName(TOPIC_OPERATOR_NAME)
                .withImage(getImage())
                .withEnv(getEnvVars())
                .withPorts(singletonList(createContainerPort(HEALTHCHECK_PORT_NAME, HEALTHCHECK_PORT, "TCP")))
                .withLivenessProbe(createHttpProbe(livenessPath + "healthy", HEALTHCHECK_PORT_NAME, livenessInitialDelay, livenessTimeout))
                .withReadinessProbe(createHttpProbe(readinessPath + "ready", HEALTHCHECK_PORT_NAME, readinessInitialDelay, readinessTimeout))
                .withResources(resources(getResources()))
                .withVolumeMounts(getVolumeMounts())
                .build();

        String tlsSidecarImage = (tlsSidecar != null && tlsSidecar.getImage() != null) ?
                tlsSidecar.getImage() : TopicOperatorSpec.DEFAULT_TLS_SIDECAR_IMAGE;

        Resources tlsSidecarResources = (tlsSidecar != null) ? tlsSidecar.getResources() : null;

        Container tlsSidecarContainer = new ContainerBuilder()
                .withName(TLS_SIDECAR_NAME)
                .withImage(tlsSidecarImage)
                .withResources(resources(tlsSidecarResources))
                .withEnv(singletonList(buildEnvVar(ENV_VAR_ZOOKEEPER_CONNECT, zookeeperConnect)))
                .withVolumeMounts(createVolumeMount(TLS_SIDECAR_EO_CERTS_VOLUME_NAME, TLS_SIDECAR_EO_CERTS_VOLUME_MOUNT),
                        createVolumeMount(TLS_SIDECAR_CA_CERTS_VOLUME_NAME, TLS_SIDECAR_CA_CERTS_VOLUME_MOUNT))
                .build();

        containers.add(container);
        containers.add(tlsSidecarContainer);

        return containers;
    }

    @Override
    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(buildEnvVar(ENV_VAR_RESOURCE_LABELS, topicConfigMapLabels));
        varList.add(buildEnvVar(ENV_VAR_KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServers));
        varList.add(buildEnvVar(ENV_VAR_ZOOKEEPER_CONNECT, String.format("%s:%d", "localhost", TopicOperatorSpec.DEFAULT_ZOOKEEPER_PORT)));
        varList.add(buildEnvVar(ENV_VAR_WATCHED_NAMESPACE, watchedNamespace));
        varList.add(buildEnvVar(ENV_VAR_FULL_RECONCILIATION_INTERVAL_MS, Integer.toString(reconciliationIntervalMs)));
        varList.add(buildEnvVar(ENV_VAR_ZOOKEEPER_SESSION_TIMEOUT_MS, Integer.toString(zookeeperSessionTimeoutMs)));
        varList.add(buildEnvVar(ENV_VAR_TOPIC_METADATA_MAX_ATTEMPTS, String.valueOf(topicMetadataMaxAttempts)));
        varList.add(buildEnvVar(ENV_VAR_TLS_ENABLED, Boolean.toString(true)));

        return varList;
    }

    /**
     * Get the name of the topic operator service account given the name of the {@code cluster}.
     */
    public static String topicOperatorServiceAccountName(String cluster) {
        return topicOperatorName(cluster);
    }

    @Override
    protected String getServiceAccountName() {
        return topicOperatorServiceAccountName(cluster);
    }

    public ServiceAccount generateServiceAccount() {
        return new ServiceAccountBuilder()
                .withNewMetadata()
                    .withName(getServiceAccountName())
                    .withNamespace(namespace)
                    .withOwnerReferences(createOwnerReference())
                .endMetadata()
            .build();
    }

    public RoleBindingOperator.RoleBinding generateRoleBinding(String namespace) {
        return new RoleBindingOperator.RoleBinding(roleBindingName(cluster), TO_CLUSTER_ROLE_NAME, namespace, getServiceAccountName(), createOwnerReference());
    }

    @Override
    protected String getDefaultLogConfigFileName() {
        return "topicOperatorDefaultLoggingProperties";
    }

    @Override
    String getAncillaryConfigMapKeyLogConfig() {
        return "log4j2.properties";
    }

    private List<Volume> getVolumes() {
        List<Volume> volumeList = new ArrayList<>();
        volumeList.add(createConfigMapVolume(logAndMetricsConfigVolumeName, ancillaryConfigName));
        volumeList.add(createSecretVolume(TLS_SIDECAR_EO_CERTS_VOLUME_NAME, TopicOperator.secretName(cluster)));
        volumeList.add(createSecretVolume(TLS_SIDECAR_CA_CERTS_VOLUME_NAME, AbstractModel.getClusterCaName(cluster)));
        return volumeList;
    }

    private List<VolumeMount> getVolumeMounts() {
        List<VolumeMount> volumeMountList = new ArrayList<>();
        volumeMountList.add(createVolumeMount(logAndMetricsConfigVolumeName, logAndMetricsConfigMountPath));
        volumeMountList.add(createVolumeMount(TLS_SIDECAR_EO_CERTS_VOLUME_NAME, TLS_SIDECAR_EO_CERTS_VOLUME_MOUNT));
        volumeMountList.add(createVolumeMount(TLS_SIDECAR_CA_CERTS_VOLUME_NAME, TLS_SIDECAR_CA_CERTS_VOLUME_MOUNT));
        return volumeMountList;
    }

    /**
     * Generate the Secret containing CA self-signed certificates for internal communication
     * It also contains the private key-certificate (signed by internal CA) for communicating with Zookeeper and Kafka
     * @return The generated Secret
     */
    public Secret generateSecret(ClusterCa clusterCa) {
        Map<String, String> data = new HashMap<>();
        Secret topicOperatorSecret = clusterCa.topicOperatorSecret();
        if (topicOperatorSecret == null || clusterCa.certRenewed()) {
            log.debug("Generating certificates");
            try {
                log.debug("Topic Operator certificate to generate");
                CertAndKey toCertAndKey = clusterCa.generateSignedCert(name, Ca.IO_STRIMZI);
                data.put("entity-operator.crt", toCertAndKey.certAsBase64String());
                data.put("entity-operator.key", toCertAndKey.keyAsBase64String());
            } catch (IOException e) {
                log.warn("Error while generating certificates", e);
            }
            log.debug("End generating certificates");
        } else {
            data.put("entity-operator.crt", topicOperatorSecret.getData().get("entity-operator.crt"));
            data.put("entity-operator.key", topicOperatorSecret.getData().get("entity-operator.key"));
        }
        return createSecret(TopicOperator.secretName(cluster), data);
    }

    protected void setTlsSidecar(TlsSidecar tlsSidecar) {
        this.tlsSidecar = tlsSidecar;
    }
}
