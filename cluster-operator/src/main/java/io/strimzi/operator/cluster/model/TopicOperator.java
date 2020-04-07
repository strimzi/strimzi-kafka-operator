/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;


import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LifecycleBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentStrategy;
import io.fabric8.kubernetes.api.model.apps.DeploymentStrategyBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleRef;
import io.fabric8.kubernetes.api.model.rbac.RoleRefBuilder;
import io.fabric8.kubernetes.api.model.rbac.Subject;
import io.fabric8.kubernetes.api.model.rbac.SubjectBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaClusterSpec;
import io.strimzi.api.kafka.model.Probe;
import io.strimzi.api.kafka.model.ProbeBuilder;
import io.strimzi.api.kafka.model.TlsSidecar;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.common.model.Labels;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.strimzi.operator.cluster.model.ModelUtils.createHttpProbe;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * Represents the topic operator deployment
 */
@Deprecated
@SuppressWarnings("deprecation")
public class TopicOperator extends AbstractModel {
    protected static final String APPLICATION_NAME = "topic-operator";

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
    public static final Probe READINESS_PROBE_OPTIONS = new ProbeBuilder().withTimeoutSeconds(io.strimzi.api.kafka.model.TopicOperatorSpec.DEFAULT_HEALTHCHECK_TIMEOUT).withInitialDelaySeconds(io.strimzi.api.kafka.model.TopicOperatorSpec.DEFAULT_HEALTHCHECK_DELAY).build();

    // Kafka bootstrap servers and Zookeeper nodes can't be specified in the JSON
    private String kafkaBootstrapServers;
    private String zookeeperConnect;

    private String watchedNamespace;
    private int reconciliationIntervalMs;
    private int zookeeperSessionTimeoutMs;
    private String topicConfigMapLabels;
    private int topicMetadataMaxAttempts;

    private TlsSidecar tlsSidecar;
    private String tlsSidecarImage;

    /**
     * @param resource Kubernetes/OpenShift resource with metadata containing the namespace and cluster name
     */
    protected TopicOperator(HasMetadata resource) {

        super(resource, APPLICATION_NAME);
        this.name = topicOperatorName(cluster);
        this.replicas = io.strimzi.api.kafka.model.TopicOperatorSpec.DEFAULT_REPLICAS;
        this.readinessPath = "/";
        this.readinessProbeOptions = READINESS_PROBE_OPTIONS;
        this.livenessPath = "/";
        this.livenessProbeOptions = READINESS_PROBE_OPTIONS;

        // create a default configuration
        this.kafkaBootstrapServers = defaultBootstrapServers(cluster);
        this.zookeeperConnect = defaultZookeeperConnect(cluster);
        this.watchedNamespace = namespace;
        this.reconciliationIntervalMs = io.strimzi.api.kafka.model.TopicOperatorSpec.DEFAULT_FULL_RECONCILIATION_INTERVAL_SECONDS * 1_000;
        this.zookeeperSessionTimeoutMs = io.strimzi.api.kafka.model.TopicOperatorSpec.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_SECONDS * 1_000;
        this.topicConfigMapLabels = defaultTopicConfigMapLabels(cluster);
        this.topicMetadataMaxAttempts = io.strimzi.api.kafka.model.TopicOperatorSpec.DEFAULT_TOPIC_METADATA_MAX_ATTEMPTS;

        this.ancillaryConfigName = metricAndLogConfigsName(cluster);
        this.logAndMetricsConfigVolumeName = "topic-operator-metrics-and-logging";
        this.logAndMetricsConfigMountPath = "/opt/topic-operator/custom-config/";
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
     * @param cluster The cluster name.
     * @return The role binding name.
     */
    public static String roleBindingName(String cluster) {
        return "strimzi-" + cluster + "-topic-operator";
    }

    protected static String defaultZookeeperConnect(String cluster) {
        return ZookeeperCluster.serviceName(cluster) + ":" + io.strimzi.api.kafka.model.TopicOperatorSpec.DEFAULT_ZOOKEEPER_PORT;
    }

    protected static String defaultBootstrapServers(String cluster) {
        return KafkaCluster.serviceName(cluster) + ":" + io.strimzi.api.kafka.model.TopicOperatorSpec.DEFAULT_BOOTSTRAP_SERVERS_PORT;
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
     * @param versions The versions.
     * @return Topic Operator instance, null if not configured in the ConfigMap
     */
    public static TopicOperator fromCrd(Kafka kafkaAssembly, KafkaVersion.Lookup versions) {
        TopicOperator result;
        if (kafkaAssembly.getSpec().getTopicOperator() != null) {
            String namespace = kafkaAssembly.getMetadata().getNamespace();
            result = new TopicOperator(kafkaAssembly);

            io.strimzi.api.kafka.model.TopicOperatorSpec tcConfig = kafkaAssembly.getSpec().getTopicOperator();

            result.setOwnerReference(kafkaAssembly);
            String image = tcConfig.getImage();
            if (image == null) {
                image = System.getenv().getOrDefault(ClusterOperatorConfig.STRIMZI_DEFAULT_TOPIC_OPERATOR_IMAGE, "strimzi/operator:latest");
            }
            result.setImage(image);
            result.setWatchedNamespace(tcConfig.getWatchedNamespace() != null ? tcConfig.getWatchedNamespace() : namespace);
            result.setReconciliationIntervalMs(tcConfig.getReconciliationIntervalSeconds() * 1_000);
            result.setZookeeperSessionTimeoutMs(tcConfig.getZookeeperSessionTimeoutSeconds() * 1_000);
            result.setTopicMetadataMaxAttempts(tcConfig.getTopicMetadataMaxAttempts());
            result.setLogging(tcConfig.getLogging());
            result.setGcLoggingEnabled(tcConfig.getJvmOptions() == null ? DEFAULT_JVM_GC_LOGGING_ENABLED : tcConfig.getJvmOptions().isGcLoggingEnabled());
            result.setResources(tcConfig.getResources());
            result.setUserAffinity(tcConfig.getAffinity());
            result.setTlsSidecar(tcConfig.getTlsSidecar());
            if (tcConfig.getReadinessProbe() != null) {
                result.setReadinessProbe(tcConfig.getReadinessProbe());
            }
            if (tcConfig.getLivenessProbe() != null) {
                result.setLivenessProbe(tcConfig.getLivenessProbe());
            }

            KafkaClusterSpec kafkaClusterSpec = kafkaAssembly.getSpec().getKafka();
            result.tlsSidecarImage = versions.kafkaImage(kafkaClusterSpec.getImage(), kafkaClusterSpec.getVersion());
        } else {
            result = null;
        }
        return result;
    }

    public Deployment generateDeployment(boolean isOpenShift, ImagePullPolicy imagePullPolicy, List<LocalObjectReference> imagePullSecrets) {
        DeploymentStrategy updateStrategy = new DeploymentStrategyBuilder()
                .withType("Recreate")
                .build();

        return createDeployment(
                updateStrategy,
                Collections.emptyMap(),
                Collections.emptyMap(),
                getMergedAffinity(),
                getInitContainers(imagePullPolicy),
                getContainers(imagePullPolicy),
                getVolumes(isOpenShift),
                imagePullSecrets
        );
    }

    @Override
    protected List<Container> getContainers(ImagePullPolicy imagePullPolicy) {
        List<Container> containers = new ArrayList<>();

        Container container = new ContainerBuilder()
                .withName(TOPIC_OPERATOR_NAME)
                .withImage(getImage())
                .withArgs("/opt/strimzi/bin/topic_operator_run.sh")
                .withEnv(getEnvVars())
                .withPorts(singletonList(createContainerPort(HEALTHCHECK_PORT_NAME, HEALTHCHECK_PORT, "TCP")))
                .withLivenessProbe(createHttpProbe(livenessPath + "healthy", HEALTHCHECK_PORT_NAME, livenessProbeOptions))
                .withReadinessProbe(createHttpProbe(readinessPath + "ready", HEALTHCHECK_PORT_NAME, readinessProbeOptions))
                .withResources(getResources())
                .withVolumeMounts(getVolumeMounts())
                .withImagePullPolicy(determineImagePullPolicy(imagePullPolicy, getImage()))
                .build();

        Container tlsSidecarContainer = new ContainerBuilder()
                .withName(TLS_SIDECAR_NAME)
                .withImage(tlsSidecarImage)
                .withCommand("/opt/stunnel/entity_operator_stunnel_run.sh")
                .withLivenessProbe(ModelUtils.tlsSidecarLivenessProbe(tlsSidecar))
                .withReadinessProbe(ModelUtils.tlsSidecarReadinessProbe(tlsSidecar))
                .withResources(tlsSidecar != null ? tlsSidecar.getResources() : null)
                .withEnv(asList(ModelUtils.tlsSidecarLogEnvVar(tlsSidecar),
                        buildEnvVar(ENV_VAR_ZOOKEEPER_CONNECT, zookeeperConnect)))
                .withVolumeMounts(VolumeUtils.createVolumeMount(TLS_SIDECAR_EO_CERTS_VOLUME_NAME, TLS_SIDECAR_EO_CERTS_VOLUME_MOUNT),
                        VolumeUtils.createVolumeMount(TLS_SIDECAR_CA_CERTS_VOLUME_NAME, TLS_SIDECAR_CA_CERTS_VOLUME_MOUNT))
                .withLifecycle(new LifecycleBuilder().withNewPreStop().withNewExec()
                        .withCommand("/opt/stunnel/entity_operator_stunnel_pre_stop.sh",
                                String.valueOf(templateTerminationGracePeriodSeconds))
                        .endExec().endPreStop().build())
                .withImagePullPolicy(determineImagePullPolicy(imagePullPolicy, tlsSidecarImage))
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
        varList.add(buildEnvVar(ENV_VAR_ZOOKEEPER_CONNECT, String.format("%s:%d", "localhost", io.strimzi.api.kafka.model.TopicOperatorSpec.DEFAULT_ZOOKEEPER_PORT)));
        varList.add(buildEnvVar(ENV_VAR_WATCHED_NAMESPACE, watchedNamespace));
        varList.add(buildEnvVar(ENV_VAR_FULL_RECONCILIATION_INTERVAL_MS, Integer.toString(reconciliationIntervalMs)));
        varList.add(buildEnvVar(ENV_VAR_ZOOKEEPER_SESSION_TIMEOUT_MS, Integer.toString(zookeeperSessionTimeoutMs)));
        varList.add(buildEnvVar(ENV_VAR_TOPIC_METADATA_MAX_ATTEMPTS, String.valueOf(topicMetadataMaxAttempts)));
        varList.add(buildEnvVar(ENV_VAR_TLS_ENABLED, Boolean.toString(true)));
        varList.add(buildEnvVar(ENV_VAR_STRIMZI_GC_LOG_ENABLED, String.valueOf(gcLoggingEnabled)));

        return varList;
    }

    /**
     * Get the name of the topic operator service account given the name of the {@code cluster}.
     * @param cluster The cluster name
     * @return The service account name.
     */
    public static String topicOperatorServiceAccountName(String cluster) {
        return topicOperatorName(cluster);
    }

    @Override
    protected String getServiceAccountName() {
        return topicOperatorServiceAccountName(cluster);
    }

    public RoleBinding generateRoleBinding(String namespace, String watchedNamespace) {
        Subject ks = new SubjectBuilder()
                .withKind("ServiceAccount")
                .withName(getServiceAccountName())
                .withNamespace(namespace)
                .build();

        RoleRef roleRef = new RoleRefBuilder()
                .withName(TO_CLUSTER_ROLE_NAME)
                .withApiGroup("rbac.authorization.k8s.io")
                .withKind("ClusterRole")
                .build();

        return new RoleBindingBuilder()
                .withNewMetadata()
                    .withName(roleBindingName(cluster))
                    .withOwnerReferences(createOwnerReference())
                    .withLabels(labels.toMap())
                    .withNamespace(watchedNamespace)
                .endMetadata()
                .withRoleRef(roleRef)
                .withSubjects(ks)
            .build();
    }

    @Override
    protected String getDefaultLogConfigFileName() {
        return "topicOperatorDefaultLoggingProperties";
    }

    @Override
    String getAncillaryConfigMapKeyLogConfig() {
        return "log4j2.properties";
    }

    private List<Volume> getVolumes(boolean isOpenShift) {
        List<Volume> volumeList = new ArrayList<>();
        volumeList.add(VolumeUtils.createConfigMapVolume(logAndMetricsConfigVolumeName, ancillaryConfigName));
        volumeList.add(VolumeUtils.createSecretVolume(TLS_SIDECAR_EO_CERTS_VOLUME_NAME, TopicOperator.secretName(cluster), isOpenShift));
        volumeList.add(VolumeUtils.createSecretVolume(TLS_SIDECAR_CA_CERTS_VOLUME_NAME, AbstractModel.clusterCaCertSecretName(cluster), isOpenShift));
        return volumeList;
    }

    private List<VolumeMount> getVolumeMounts() {
        List<VolumeMount> volumeMountList = new ArrayList<>();
        volumeMountList.add(VolumeUtils.createVolumeMount(logAndMetricsConfigVolumeName, logAndMetricsConfigMountPath));
        volumeMountList.add(VolumeUtils.createVolumeMount(TLS_SIDECAR_EO_CERTS_VOLUME_NAME, TLS_SIDECAR_EO_CERTS_VOLUME_MOUNT));
        volumeMountList.add(VolumeUtils.createVolumeMount(TLS_SIDECAR_CA_CERTS_VOLUME_NAME, TLS_SIDECAR_CA_CERTS_VOLUME_MOUNT));
        return volumeMountList;
    }

    /**
     * Generate the Secret containing CA self-signed certificates for internal communication
     * It also contains the private key-certificate (signed by internal CA) for communicating with Zookeeper and Kafka
     * @param clusterCa The cluster CA
     * @param isMaintenanceTimeWindowsSatisfied Indicates whether we are in the maintenance window or not.
     *                                          This is used for certificate renewals
     * @return The generated Secret
     */
    public Secret generateSecret(ClusterCa clusterCa, boolean isMaintenanceTimeWindowsSatisfied) {
        Secret topicOperatorSecret = clusterCa.topicOperatorSecret();
        // TO is using the keyCertName as "entity-operator". This is not typo.
        return ModelUtils.buildSecret(clusterCa, topicOperatorSecret, namespace, TopicOperator.secretName(cluster), name,
                "entity-operator", labels, createOwnerReference(), isMaintenanceTimeWindowsSatisfied);
    }

    protected void setTlsSidecar(TlsSidecar tlsSidecar) {
        this.tlsSidecar = tlsSidecar;
    }
}
