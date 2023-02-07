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
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaClusterSpec;
import io.strimzi.api.kafka.model.KafkaExporterResources;
import io.strimzi.api.kafka.model.KafkaExporterSpec;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.Probe;
import io.strimzi.api.kafka.model.ProbeBuilder;
import io.strimzi.api.kafka.model.template.DeploymentTemplate;
import io.strimzi.api.kafka.model.template.KafkaExporterTemplate;
import io.strimzi.api.kafka.model.template.PodTemplate;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.securityprofiles.ContainerSecurityProviderContextImpl;
import io.strimzi.operator.cluster.model.securityprofiles.PodSecurityProviderContextImpl;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.api.kafka.model.template.DeploymentStrategy.ROLLING_UPDATE;

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
    private static final Probe READINESS_PROBE_OPTIONS = new ProbeBuilder().withTimeoutSeconds(DEFAULT_HEALTHCHECK_TIMEOUT).withInitialDelaySeconds(DEFAULT_HEALTHCHECK_DELAY).withPeriodSeconds(DEFAULT_HEALTHCHECK_PERIOD).build();

    protected static final String ENV_VAR_KAFKA_EXPORTER_LOGGING = "KAFKA_EXPORTER_LOGGING";
    protected static final String ENV_VAR_KAFKA_EXPORTER_KAFKA_VERSION = "KAFKA_EXPORTER_KAFKA_VERSION";
    protected static final String ENV_VAR_KAFKA_EXPORTER_GROUP_REGEX = "KAFKA_EXPORTER_GROUP_REGEX";
    protected static final String ENV_VAR_KAFKA_EXPORTER_TOPIC_REGEX = "KAFKA_EXPORTER_TOPIC_REGEX";
    protected static final String ENV_VAR_KAFKA_EXPORTER_KAFKA_SERVER = "KAFKA_EXPORTER_KAFKA_SERVER";
    protected static final String ENV_VAR_KAFKA_EXPORTER_ENABLE_SARAMA = "KAFKA_EXPORTER_ENABLE_SARAMA";

    protected static final String CO_ENV_VAR_CUSTOM_KAFKA_EXPORTER_POD_LABELS = "STRIMZI_CUSTOM_KAFKA_EXPORTER_LABELS";

    protected String groupRegex = ".*";
    protected String topicRegex = ".*";
    protected boolean saramaLoggingEnabled;
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
     */
    protected KafkaExporter(Reconciliation reconciliation, HasMetadata resource) {
        super(reconciliation, resource, KafkaExporterResources.deploymentName(resource.getMetadata().getName()), COMPONENT_TYPE);

        this.replicas = 1;
        this.readinessPath = "/healthz";
        this.readinessProbeOptions = READINESS_PROBE_OPTIONS;
        this.livenessPath = "/healthz";
        this.livenessProbeOptions = READINESS_PROBE_OPTIONS;

        this.saramaLoggingEnabled = false;
        this.mountPath = "/var/lib/kafka";

        // Kafka Exporter is all about metrics - they are always enabled
        this.isMetricsEnabled = true;

    }

    /**
     * Builds the KafkaExporter model from the Kafka custom resource. If KafkaExporter is not enabled, it will return null.
     *
     * @param reconciliation    Reconciliation marker for logging
     * @param kafkaAssembly     The Kafka CR
     * @param versions          The list of supported Kafka versions
     *
     * @return                  KafkaExporter model object when Kafka Exporter is enabled or null if it is disabled.
     */
    public static KafkaExporter fromCrd(Reconciliation reconciliation, Kafka kafkaAssembly, KafkaVersion.Lookup versions) {
        KafkaExporterSpec spec = kafkaAssembly.getSpec().getKafkaExporter();

        if (spec != null) {
            KafkaExporter kafkaExporter = new KafkaExporter(reconciliation, kafkaAssembly);

            kafkaExporter.resources = spec.getResources();

            if (spec.getReadinessProbe() != null) {
                kafkaExporter.readinessProbeOptions = spec.getReadinessProbe();
            }

            if (spec.getLivenessProbe() != null) {
                kafkaExporter.livenessProbeOptions = spec.getLivenessProbe();
            }

            kafkaExporter.groupRegex = spec.getGroupRegex();
            kafkaExporter.topicRegex = spec.getTopicRegex();

            String image = spec.getImage();
            if (image == null) {
                KafkaClusterSpec kafkaClusterSpec = kafkaAssembly.getSpec().getKafka();
                image = System.getenv().getOrDefault(ClusterOperatorConfig.STRIMZI_DEFAULT_KAFKA_EXPORTER_IMAGE, versions.kafkaImage(kafkaClusterSpec.getImage(), versions.defaultVersion().version()));
            }
            kafkaExporter.image = image;

            kafkaExporter.exporterLogging = spec.getLogging();
            kafkaExporter.saramaLoggingEnabled = spec.getEnableSaramaLogging();

            if (spec.getTemplate() != null) {
                KafkaExporterTemplate template = spec.getTemplate();

                kafkaExporter.templateDeployment = template.getDeployment();
                kafkaExporter.templatePod = template.getPod();
                kafkaExporter.templateServiceAccount = template.getServiceAccount();
                kafkaExporter.templateContainer = template.getContainer();
            }

            kafkaExporter.version = versions.supportedVersion(kafkaAssembly.getSpec().getKafka().getVersion()).version();

            return kafkaExporter;
        } else {
            return null;
        }
    }

    protected List<ContainerPort> getContainerPortList() {
        List<ContainerPort> portList = new ArrayList<>(1);
        portList.add(ContainerUtils.createContainerPort(METRICS_PORT_NAME, METRICS_PORT));
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
                replicas,
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
                ProbeGenerator.httpProbe(livenessProbeOptions, livenessPath, METRICS_PORT_NAME),
                ProbeGenerator.httpProbe(readinessProbeOptions, readinessPath, METRICS_PORT_NAME),
                imagePullPolicy
        );
    }

    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();

        varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_EXPORTER_LOGGING, Integer.toString(loggingMapping(exporterLogging))));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_EXPORTER_KAFKA_VERSION, version));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_EXPORTER_GROUP_REGEX, groupRegex));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_EXPORTER_TOPIC_REGEX, topicRegex));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_EXPORTER_KAFKA_SERVER, KafkaResources.bootstrapServiceName(cluster) + ":" + KafkaCluster.REPLICATION_PORT));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_EXPORTER_ENABLE_SARAMA, String.valueOf(saramaLoggingEnabled)));

        // Add shared environment variables used for all containers
        varList.addAll(ContainerUtils.requiredEnvVars());

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
        return ModelUtils.buildSecret(reconciliation, clusterCa, secret, namespace, KafkaExporterResources.secretName(cluster), componentName,
                "kafka-exporter", labels, ownerReference, isMaintenanceTimeWindowsSatisfied);
    }
}
