/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentStrategy;
import io.fabric8.kubernetes.api.model.apps.DeploymentStrategyBuilder;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudget;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaExporterResources;
import io.strimzi.api.kafka.model.KafkaExporterSpec;
import io.strimzi.api.kafka.model.Probe;
import io.strimzi.api.kafka.model.ProbeBuilder;
import io.strimzi.api.kafka.model.template.KafkaExporterTemplate;
import io.strimzi.operator.common.model.Labels;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class KafkaExporter extends AbstractModel {

    private static final String NAME_SUFFIX = "-kafka-exporter";
    protected static final String METRICS_AND_LOG_CONFIG_SUFFIX = NAME_SUFFIX + "-config";

    // Configuration defaults
    protected static final int DEFAULT_REPLICAS = 1;
    private static final int DEFAULT_HEALTHCHECK_DELAY = 15;
    private static final int DEFAULT_HEALTHCHECK_TIMEOUT = 5;
    private static final int DEFAULT_HEALTHCHECK_PERIOD = 10;
    public static final Probe READINESS_PROBE_OPTIONS = new ProbeBuilder().withTimeoutSeconds(DEFAULT_HEALTHCHECK_TIMEOUT).withInitialDelaySeconds(DEFAULT_HEALTHCHECK_DELAY).build();

    protected static final String ENV_VAR_KAFKA_EXPORTER_LOGGING = "KAFKA_EXPORTER_LOGGING";
    protected static final String ENV_VAR_KAFKA_EXPORTER_KAFKA_VERSION = "KAFKA_EXPORTER_KAFKA_VERSION";
    protected static final String ENV_VAR_KAFKA_EXPORTER_GROUP_REGEX = "KAFKA_EXPORTER_GROUP_REGEX";
    protected static final String ENV_VAR_KAFKA_EXPORTER_TOPIC_REGEX = "KAFKA_EXPORTER_TOPIC_REGEX";
    protected static final String ENV_VAR_KAFKA_EXPORTER_KAFKA_SERVER = "KAFKA_EXPORTER_KAFKA_SERVER";
    protected static final String ENV_VAR_KAFKA_EXPORTER_ENABLE_SARAMA = "KAFKA_EXPORTER_ENABLE_SARAMA";

    protected static final String ENV_VAR_STRIMZI_READINESS_PERIOD = "STRIMZI_READINESS_PERIOD";
    protected static final String ENV_VAR_STRIMZI_LIVENESS_PERIOD = "STRIMZI_LIVENESS_PERIOD";

    protected String groupRegex = ".*";
    protected String topicRegex = ".*";
    private boolean saramaLoggingEnabled;
    private String logging;
    private String version;

    /**
     * Constructor
     *
     * @param namespace Kubernetes/OpenShift namespace where Kafka Mirror Maker cluster resources are going to be created
     * @param cluster   overall cluster name
     * @param labels    labels to add to the cluster
     */
    protected KafkaExporter(String namespace, String cluster, Labels labels) {
        super(namespace, cluster, labels);
        this.name = KafkaExporterResources.deploymentName(cluster);
        this.serviceName = KafkaExporterResources.serviceName(cluster);
        this.replicas = DEFAULT_REPLICAS;
        this.readinessPath = "/metrics";
        this.readinessProbeOptions = READINESS_PROBE_OPTIONS;
        this.livenessPath = "/metrics";
        this.livenessProbeOptions = READINESS_PROBE_OPTIONS;

        this.saramaLoggingEnabled = false;
        this.mountPath = "/var/lib/kafka";
    }

    public static KafkaExporter fromCrd(Kafka kafkaAssembly) {
        if (kafkaAssembly.getSpec().getKafkaExporter() == null)
            return null;

        KafkaExporter kafkaExporter = new KafkaExporter(kafkaAssembly.getMetadata().getNamespace(),
                kafkaAssembly.getMetadata().getName(),
                Labels.fromResource(kafkaAssembly).withKind(kafkaAssembly.getKind()));

        KafkaExporterSpec spec = kafkaAssembly.getSpec().getKafkaExporter();
        if (spec != null) {
            kafkaExporter.setResources(spec.getResources());

            if (spec.getReadinessProbe() != null) {
                kafkaExporter.setReadinessProbe(spec.getReadinessProbe());
            }

            if (spec.getLivenessProbe() != null) {
                kafkaExporter.setLivenessProbe(spec.getLivenessProbe());
            }

            kafkaExporter.setGroupRegex(spec.getGroupRegex());
            kafkaExporter.setTopicRegex(spec.getTopicRegex());

            String image = spec.getImage();
            if (image == null) {
                image = System.getenv().get("STRIMZI_DEFAULT_KAFKA_EXPORTER_IMAGE");
            }
            kafkaExporter.setImage(image);

            kafkaExporter.setLogging(spec.getLogging());
            kafkaExporter.setSaramaLoggingEnabled(spec.getEnableSaramaLogging());


            if (spec.getTemplate() != null) {
                KafkaExporterTemplate template = spec.getTemplate();

                if (template.getDeployment() != null && template.getDeployment().getMetadata() != null) {
                    kafkaExporter.templateDeploymentLabels = template.getDeployment().getMetadata().getLabels();
                    kafkaExporter.templateDeploymentAnnotations = template.getDeployment().getMetadata().getAnnotations();

                    if (template.getKafkaExporterService() != null && template.getKafkaExporterService().getMetadata() != null)  {
                        kafkaExporter.templateServiceLabels = template.getKafkaExporterService().getMetadata().getLabels();
                        kafkaExporter.templateServiceAnnotations = template.getKafkaExporterService().getMetadata().getAnnotations();
                    }
                }

                ModelUtils.parsePodTemplate(kafkaExporter, template.getPod());
            }

            kafkaExporter.setUserAffinity(affinity(spec));
            kafkaExporter.setTolerations(tolerations(spec));
            kafkaExporter.setVersion(kafkaAssembly.getSpec().getKafka().getVersion());
        }

        kafkaExporter.setOwnerReference(kafkaAssembly);
        return kafkaExporter;
    }

    protected void setSaramaLoggingEnabled(boolean saramaLoggingEnabled) {
        this.saramaLoggingEnabled = saramaLoggingEnabled;
    }

    static List<Toleration> tolerations(KafkaExporterSpec spec) {
        if (spec.getTemplate() != null
                && spec.getTemplate().getPod() != null
                && spec.getTemplate().getPod().getTolerations() != null) {
            return spec.getTemplate().getPod().getTolerations();
        } else {
            return null;
        }
    }

    static Affinity affinity(KafkaExporterSpec spec) {
        if (spec.getTemplate() != null
                && spec.getTemplate().getPod() != null
                && spec.getTemplate().getPod().getAffinity() != null) {
            return spec.getTemplate().getPod().getAffinity();
        } else {
            return null;
        }
    }

    public Service generateService() {
        List<ServicePort> ports = new ArrayList<>(1);

        ports.add(createServicePort(METRICS_PORT_NAME, METRICS_PORT, METRICS_PORT, "TCP"));
        return createService("ClusterIP", ports, getPrometheusAnnotations());

    }

    protected List<ContainerPort> getContainerPortList() {
        List<ContainerPort> portList = new ArrayList<>(1);
        portList.add(createContainerPort(METRICS_PORT_NAME, METRICS_PORT, "TCP"));
        return portList;
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
                Collections.emptyList(),
                imagePullSecrets
        );
    }

    @Override
    protected List<Container> getContainers(ImagePullPolicy imagePullPolicy) {

        List<Container> containers = new ArrayList<>();

        Container container = new ContainerBuilder()
                .withName(name)
                .withImage(getImage())
                .withCommand("/opt/kafka-exporter/kafka_exporter_run.sh")
                .withEnv(getEnvVars())
                .withPorts(getContainerPortList())
                .withLivenessProbe(ModelUtils.createHttpProbe(livenessPath, METRICS_PORT_NAME, livenessProbeOptions))
                .withReadinessProbe(ModelUtils.createHttpProbe(readinessPath, METRICS_PORT_NAME, readinessProbeOptions))
                .withResources(getResources())
                .withImagePullPolicy(determineImagePullPolicy(imagePullPolicy, getImage()))
                .build();

        containers.add(container);

        return containers;
    }

    @Override
    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();

        varList.add(buildEnvVar(ENV_VAR_KAFKA_EXPORTER_LOGGING, logging));
        varList.add(buildEnvVar(ENV_VAR_KAFKA_EXPORTER_KAFKA_VERSION, version));
        varList.add(buildEnvVar(ENV_VAR_KAFKA_EXPORTER_GROUP_REGEX, groupRegex));
        varList.add(buildEnvVar(ENV_VAR_KAFKA_EXPORTER_TOPIC_REGEX, topicRegex));
        varList.add(buildEnvVar(ENV_VAR_KAFKA_EXPORTER_KAFKA_SERVER, KafkaCluster.serviceName(cluster) + ":9092"));
        varList.add(buildEnvVar(ENV_VAR_KAFKA_EXPORTER_ENABLE_SARAMA, String.valueOf(saramaLoggingEnabled)));

        varList.add(buildEnvVar(ENV_VAR_STRIMZI_LIVENESS_PERIOD,
                String.valueOf(livenessProbeOptions.getPeriodSeconds() != null ? livenessProbeOptions.getPeriodSeconds() : DEFAULT_HEALTHCHECK_PERIOD)));
        varList.add(buildEnvVar(ENV_VAR_STRIMZI_READINESS_PERIOD,
                String.valueOf(readinessProbeOptions.getPeriodSeconds() != null ? readinessProbeOptions.getPeriodSeconds() : DEFAULT_HEALTHCHECK_PERIOD)));

        return varList;
    }

    /**
     * Generates the PodDisruptionBudget.
     *
     * @return The PodDisruptionBudget.
     */
    public PodDisruptionBudget generatePodDisruptionBudget() {
        return createPodDisruptionBudget();
    }

    public static String entityOperatorName(String cluster) {
        return KafkaExporterResources.deploymentName(cluster);
    }

    /**
     * Get the name of the Entity Operator service account given the name of the {@code cluster}.
     * @param cluster The cluster name
     * @return The name of the EO service account.
     */
    public static String containerServiceAccountName(String cluster) {
        return entityOperatorName(cluster);
    }

    @Override
    protected String getDefaultLogConfigFileName() {
        return null;
    }

    public void setGroupRegex(String groupRegex) {
        this.groupRegex = groupRegex;
    }

    protected String getGroupRegex() {
        return groupRegex;
    }

    public void setTopicRegex(String topicRegex) {
        this.topicRegex = topicRegex;
    }

    protected String getTopicRegex() {
        return topicRegex;
    }

    public static String metricAndLogConfigsName(String cluster) {
        return cluster + METRICS_AND_LOG_CONFIG_SUFFIX;
    }

    @Override
    protected String getServiceAccountName() {
        return KafkaExporterResources.serviceAccountName(cluster);
    }

    public static String exporterOperatorName(String cluster) {
        return cluster + NAME_SUFFIX;
    }

    public void setLogging(String logging) {
        this.logging = logging;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getExporterName(String cluster) {
        return KafkaExporterResources.deploymentName(cluster);
    }
}
