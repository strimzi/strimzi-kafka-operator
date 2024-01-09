/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyIngressRule;
import io.strimzi.api.kafka.model.common.Probe;
import io.strimzi.api.kafka.model.common.ProbeBuilder;
import io.strimzi.api.kafka.model.common.template.DeploymentTemplate;
import io.strimzi.api.kafka.model.common.template.PodTemplate;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaClusterSpec;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.exporter.KafkaExporterResources;
import io.strimzi.api.kafka.model.kafka.exporter.KafkaExporterSpec;
import io.strimzi.api.kafka.model.kafka.exporter.KafkaExporterTemplate;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.metrics.MetricsModel;
import io.strimzi.operator.cluster.model.securityprofiles.ContainerSecurityProviderContextImpl;
import io.strimzi.operator.cluster.model.securityprofiles.PodSecurityProviderContextImpl;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.api.kafka.model.common.template.DeploymentStrategy.ROLLING_UPDATE;

/**
 * Kafka Exporter model
 */
public class KafkaExporter extends AbstractModel {
    protected static final String COMPONENT_TYPE = "kafka-exporter";

    // Configuration for mounting certificates
    protected static final String KAFKA_EXPORTER_CERTS_VOLUME_NAME = "kafka-exporter-certs";
    protected static final String KAFKA_EXPORTER_CERTS_VOLUME_MOUNT = "/etc/kafka-exporter/kafka-exporter-certs/";
    protected static final String CLUSTER_CA_CERTS_VOLUME_NAME = "cluster-ca-certs";
    protected static final String CLUSTER_CA_CERTS_VOLUME_MOUNT = "/etc/kafka-exporter/cluster-ca-certs/";

    // Configuration defaults
    /*test*/ static final int DEFAULT_HEALTHCHECK_DELAY = 15;
    /*test*/ static final int DEFAULT_HEALTHCHECK_TIMEOUT = 15;
    /*test*/ static final int DEFAULT_HEALTHCHECK_PERIOD = 30;
    private static final Probe DEFAULT_HEALTHCHECK_OPTIONS = new ProbeBuilder().withTimeoutSeconds(DEFAULT_HEALTHCHECK_TIMEOUT).withInitialDelaySeconds(DEFAULT_HEALTHCHECK_DELAY).withPeriodSeconds(DEFAULT_HEALTHCHECK_PERIOD).build();

    protected static final String ENV_VAR_KAFKA_EXPORTER_LOGGING = "KAFKA_EXPORTER_LOGGING";
    protected static final String ENV_VAR_KAFKA_EXPORTER_KAFKA_VERSION = "KAFKA_EXPORTER_KAFKA_VERSION";
    protected static final String ENV_VAR_KAFKA_EXPORTER_GROUP_REGEX = "KAFKA_EXPORTER_GROUP_REGEX";
    protected static final String ENV_VAR_KAFKA_EXPORTER_TOPIC_REGEX = "KAFKA_EXPORTER_TOPIC_REGEX";
    protected static final String ENV_VAR_KAFKA_EXPORTER_GROUP_EXCLUDE_REGEX = "KAFKA_EXPORTER_GROUP_EXCLUDE_REGEX";
    protected static final String ENV_VAR_KAFKA_EXPORTER_TOPIC_EXCLUDE_REGEX = "KAFKA_EXPORTER_TOPIC_EXCLUDE_REGEX";
    protected static final String ENV_VAR_KAFKA_EXPORTER_KAFKA_SERVER = "KAFKA_EXPORTER_KAFKA_SERVER";
    protected static final String ENV_VAR_KAFKA_EXPORTER_ENABLE_SARAMA = "KAFKA_EXPORTER_ENABLE_SARAMA";
    protected static final String ENV_VAR_KAFKA_EXPORTER_OFFSET_SHOW_ALL = "KAFKA_EXPORTER_OFFSET_SHOW_ALL";

    protected static final String CO_ENV_VAR_CUSTOM_KAFKA_EXPORTER_POD_LABELS = "STRIMZI_CUSTOM_KAFKA_EXPORTER_LABELS";

    protected String groupRegex = ".*";
    protected String topicRegex = ".*";
    protected String groupExcludeRegex;
    protected String topicExcludeRegex;
    protected boolean saramaLoggingEnabled;
    protected boolean showAllOffsets;
    /* test */ String exporterLogging;
    protected String version;

    private DeploymentTemplate templateDeployment;
    private PodTemplate templatePod;
    private static final Map<String, String> DEFAULT_POD_LABELS = new HashMap<>();
    static {
        String value = System.getenv(CO_ENV_VAR_CUSTOM_KAFKA_EXPORTER_POD_LABELS);
        if (value != null) {
            DEFAULT_POD_LABELS.putAll(Util.parseMap(value));
        }
    }

    /**
     * Constructor
     *
     * @param reconciliation The reconciliation
     * @param resource Kubernetes resource with metadata containing the namespace and cluster name
     * @param sharedEnvironmentProvider Shared environment provider
     */
    protected KafkaExporter(Reconciliation reconciliation, HasMetadata resource, SharedEnvironmentProvider sharedEnvironmentProvider) {
        super(reconciliation, resource, KafkaExporterResources.componentName(resource.getMetadata().getName()), COMPONENT_TYPE, sharedEnvironmentProvider);

        this.saramaLoggingEnabled = false;
        this.showAllOffsets = true;
    }

    /**
     * Builds the KafkaExporter model from the Kafka custom resource. If KafkaExporter is not enabled, it will return null.
     *
     * @param reconciliation    Reconciliation marker for logging
     * @param kafkaAssembly     The Kafka CR
     * @param versions          The list of supported Kafka versions
     * @param sharedEnvironmentProvider Shared environment provider
     * @return                  KafkaExporter model object when Kafka Exporter is enabled or null if it is disabled.
     */
    public static KafkaExporter fromCrd(Reconciliation reconciliation, Kafka kafkaAssembly, KafkaVersion.Lookup versions, SharedEnvironmentProvider sharedEnvironmentProvider) {
        KafkaExporterSpec spec = kafkaAssembly.getSpec().getKafkaExporter();

        if (spec != null) {
            KafkaExporter result = new KafkaExporter(reconciliation, kafkaAssembly, sharedEnvironmentProvider);

            result.resources = spec.getResources();
            result.readinessProbeOptions = ProbeUtils.extractReadinessProbeOptionsOrDefault(spec, DEFAULT_HEALTHCHECK_OPTIONS);
            result.livenessProbeOptions = ProbeUtils.extractLivenessProbeOptionsOrDefault(spec, DEFAULT_HEALTHCHECK_OPTIONS);

            result.groupRegex = spec.getGroupRegex();
            result.topicRegex = spec.getTopicRegex();

            result.groupExcludeRegex = spec.getGroupExcludeRegex();
            result.topicExcludeRegex = spec.getTopicExcludeRegex();

            String image = spec.getImage();
            if (image == null) {
                KafkaClusterSpec kafkaClusterSpec = kafkaAssembly.getSpec().getKafka();
                image = System.getenv().getOrDefault(ClusterOperatorConfig.STRIMZI_DEFAULT_KAFKA_EXPORTER_IMAGE, versions.kafkaImage(kafkaClusterSpec.getImage(), versions.defaultVersion().version()));
            }
            result.image = image;

            result.exporterLogging = spec.getLogging();
            result.saramaLoggingEnabled = spec.getEnableSaramaLogging();
            result.showAllOffsets = spec.getShowAllOffsets();

            if (spec.getTemplate() != null) {
                KafkaExporterTemplate template = spec.getTemplate();

                result.templateDeployment = template.getDeployment();
                result.templatePod = template.getPod();
                result.templateServiceAccount = template.getServiceAccount();
                result.templateContainer = template.getContainer();
            }

            result.version = versions.supportedVersion(kafkaAssembly.getSpec().getKafka().getVersion()).version();

            return result;
        } else {
            return null;
        }
    }

    protected List<ContainerPort> getContainerPortList() {
        List<ContainerPort> portList = new ArrayList<>(1);
        portList.add(ContainerUtils.createContainerPort(MetricsModel.METRICS_PORT_NAME, MetricsModel.METRICS_PORT));
        return portList;
    }

    /**
     * Generates Kafka Exporter Deployment
     *
     * @param isOpenShift       Flag indicating whether we are on OpenShift or not
     * @param imagePullPolicy   Image pull policy
     * @param imagePullSecrets  List of Image Pull Secrets
     *
     * @return  Generated deployment
     */
    public Deployment generateDeployment(boolean isOpenShift, ImagePullPolicy imagePullPolicy, List<LocalObjectReference> imagePullSecrets) {
        return WorkloadUtils.createDeployment(
                componentName,
                namespace,
                labels,
                ownerReference,
                templateDeployment,
                1,
                null,
                WorkloadUtils.deploymentStrategy(TemplateUtils.deploymentStrategy(templateDeployment, ROLLING_UPDATE)),
                WorkloadUtils.createPodTemplateSpec(
                        componentName,
                        labels,
                        templatePod,
                        DEFAULT_POD_LABELS,
                        Map.of(),
                        templatePod != null ? templatePod.getAffinity() : null,
                        null,
                        List.of(createContainer(imagePullPolicy)),
                        getVolumes(isOpenShift),
                        imagePullSecrets,
                        securityProvider.kafkaExporterPodSecurityContext(new PodSecurityProviderContextImpl(templatePod))
                )
        );
    }

    /* test */ Container createContainer(ImagePullPolicy imagePullPolicy) {
        return ContainerUtils.createContainer(
                componentName,
                image,
                List.of("/opt/kafka-exporter/kafka_exporter_run.sh"),
                securityProvider.kafkaExporterContainerSecurityContext(new ContainerSecurityProviderContextImpl(templateContainer)),
                resources,
                getEnvVars(),
                getContainerPortList(),
                List.of(VolumeUtils.createTempDirVolumeMount(),
                        VolumeUtils.createVolumeMount(KAFKA_EXPORTER_CERTS_VOLUME_NAME, KAFKA_EXPORTER_CERTS_VOLUME_MOUNT),
                        VolumeUtils.createVolumeMount(CLUSTER_CA_CERTS_VOLUME_NAME, CLUSTER_CA_CERTS_VOLUME_MOUNT)),
                ProbeUtils.httpProbe(livenessProbeOptions, "/healthz", MetricsModel.METRICS_PORT_NAME),
                ProbeUtils.httpProbe(readinessProbeOptions, "/healthz", MetricsModel.METRICS_PORT_NAME),
                imagePullPolicy
        );
    }

    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();

        varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_EXPORTER_LOGGING, Integer.toString(loggingMapping(exporterLogging))));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_EXPORTER_KAFKA_VERSION, version));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_EXPORTER_GROUP_REGEX, groupRegex));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_EXPORTER_TOPIC_REGEX, topicRegex));
        if (groupExcludeRegex != null) {
            varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_EXPORTER_GROUP_EXCLUDE_REGEX, groupExcludeRegex));
        }
        if (topicExcludeRegex != null) {
            varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_EXPORTER_TOPIC_EXCLUDE_REGEX, topicExcludeRegex));
        }
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_EXPORTER_KAFKA_SERVER, KafkaResources.bootstrapServiceName(cluster) + ":" + KafkaCluster.REPLICATION_PORT));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_EXPORTER_ENABLE_SARAMA, String.valueOf(saramaLoggingEnabled)));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_EXPORTER_OFFSET_SHOW_ALL, String.valueOf(showAllOffsets)));

        // Add shared environment variables used for all containers
        varList.addAll(sharedEnvironmentProvider.variables());

        ContainerUtils.addContainerEnvsToExistingEnvs(reconciliation, varList, templateContainer);

        return varList;
    }

    private static int loggingMapping(String logLevel) {
        if (logLevel.equalsIgnoreCase("info")) {
            return 0;
        } else if (logLevel.equalsIgnoreCase("debug")) {
            return 1;
        } else if (logLevel.equalsIgnoreCase("trace")) {
            return 2;
        } else {
            return 0;
        }
    }

    private List<Volume> getVolumes(boolean isOpenShift) {
        List<Volume> volumeList = new ArrayList<>(3);

        volumeList.add(VolumeUtils.createTempDirVolume(templatePod));
        volumeList.add(VolumeUtils.createSecretVolume(KAFKA_EXPORTER_CERTS_VOLUME_NAME, KafkaExporterResources.secretName(cluster), isOpenShift));
        volumeList.add(VolumeUtils.createSecretVolume(CLUSTER_CA_CERTS_VOLUME_NAME, AbstractModel.clusterCaCertSecretName(cluster), isOpenShift));

        return volumeList;
    }

    /**
     * Generate the Secret containing the Kafka Exporter certificate signed by the cluster CA certificate used for TLS based
     * internal communication with Kafka and Zookeeper.
     * It also contains the related Kafka Exporter private key.
     *
     * @param clusterCa The cluster CA.
     * @param isMaintenanceTimeWindowsSatisfied Indicates whether we are in the maintenance window or not.
     *                                          This is used for certificate renewals
     * @return The generated Secret.
     */
    public Secret generateSecret(ClusterCa clusterCa, boolean isMaintenanceTimeWindowsSatisfied) {
        Secret secret = clusterCa.kafkaExporterSecret();
        return CertUtils.buildTrustedCertificateSecret(reconciliation, clusterCa, secret, namespace, KafkaExporterResources.secretName(cluster), componentName,
                "kafka-exporter", labels, ownerReference, isMaintenanceTimeWindowsSatisfied);
    }

    /**
     * Generates the NetworkPolicies relevant for Kafka Exporter
     *
     * @return The network policy.
     */
    public NetworkPolicy generateNetworkPolicy() {
        // List of network policy rules for all ports
        List<NetworkPolicyIngressRule> rules = new ArrayList<>();

        // Everyone can access metrics
        rules.add(NetworkPolicyUtils.createIngressRule(MetricsModel.METRICS_PORT, List.of()));

        // Build the final network policy with all rules covering all the ports
        return NetworkPolicyUtils.createNetworkPolicy(
                componentName,
                namespace,
                labels,
                ownerReference,
                rules
        );
    }
}
