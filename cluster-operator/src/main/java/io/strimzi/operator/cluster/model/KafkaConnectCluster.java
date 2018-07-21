/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.DeploymentStrategy;
import io.fabric8.kubernetes.api.model.extensions.DeploymentStrategyBuilder;
import io.fabric8.kubernetes.api.model.extensions.RollingUpdateDeploymentBuilder;
import io.strimzi.api.kafka.model.KafkaConnectAssembly;
import io.strimzi.api.kafka.model.KafkaConnectAssemblySpec;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptySet;

public class KafkaConnectCluster extends AbstractModel {

    // Port configuration
    protected static final int REST_API_PORT = 8083;
    protected static final String REST_API_PORT_NAME = "rest-api";
    protected static final int METRICS_PORT = 9404;
    protected static final String METRICS_PORT_NAME = "metrics";

    private static final String NAME_SUFFIX = "-connect";
    private static final String SERVICE_NAME_SUFFIX = NAME_SUFFIX + "-api";

    private static final String METRICS_AND_LOG_CONFIG_SUFFIX = NAME_SUFFIX + "-config";

    // Configuration defaults
    protected static final String DEFAULT_IMAGE =
            System.getenv().getOrDefault("STRIMZI_DEFAULT_KAFKA_CONNECT_IMAGE", "strimzi/kafka-connect:latest");
    protected static final int DEFAULT_REPLICAS = 3;
    protected static final int DEFAULT_HEALTHCHECK_DELAY = 60;
    protected static final int DEFAULT_HEALTHCHECK_TIMEOUT = 5;
    protected static final boolean DEFAULT_KAFKA_CONNECT_METRICS_ENABLED = false;

    // Kafka Connect configuration keys (EnvVariables)
    protected static final String ENV_VAR_KAFKA_CONNECT_CONFIGURATION = "KAFKA_CONNECT_CONFIGURATION";
    protected static final String ENV_VAR_KAFKA_CONNECT_METRICS_ENABLED = "KAFKA_CONNECT_METRICS_ENABLED";
    protected static final String ENV_VAR_KAFKA_CONNECT_LOGGING = "KAFKA_CONNECT_LOGGING";

    /**
     * Constructor
     *
     * @param namespace Kubernetes/OpenShift namespace where Kafka Connect cluster resources are going to be created
     * @param cluster   overall cluster name
     */
    protected KafkaConnectCluster(String namespace, String cluster, Labels labels) {
        super(namespace, cluster, labels);
        this.name = kafkaConnectClusterName(cluster);
        this.serviceName = serviceName(cluster);
        this.validLoggerFields = getDefaultLogConfig();
        this.ancillaryConfigName = logAndMetricsConfigName(cluster);
        this.image = DEFAULT_IMAGE;
        this.replicas = DEFAULT_REPLICAS;
        this.readinessPath = "/";
        this.readinessTimeout = DEFAULT_HEALTHCHECK_TIMEOUT;
        this.readinessInitialDelay = DEFAULT_HEALTHCHECK_DELAY;
        this.livenessPath = "/";
        this.livenessTimeout = DEFAULT_HEALTHCHECK_TIMEOUT;
        this.livenessInitialDelay = DEFAULT_HEALTHCHECK_DELAY;
        this.isMetricsEnabled = DEFAULT_KAFKA_CONNECT_METRICS_ENABLED;

        this.mountPath = "/var/lib/kafka";
        this.logAndMetricsConfigVolumeName = "kafka-metrics-and-logging";
        this.logAndMetricsConfigMountPath = "/opt/kafka/custom-config/";
    }

    public static String kafkaConnectClusterName(String cluster) {
        return cluster + KafkaConnectCluster.NAME_SUFFIX;
    }

    public static String serviceName(String cluster) {
        return cluster + KafkaConnectCluster.SERVICE_NAME_SUFFIX;
    }

    public static String logAndMetricsConfigName(String cluster) {
        return cluster + KafkaConnectCluster.METRICS_AND_LOG_CONFIG_SUFFIX;
    }

    public static KafkaConnectCluster fromCrd(KafkaConnectAssembly kafkaConnectAssembly) {
        return fromSpec(kafkaConnectAssembly.getSpec(),
                new KafkaConnectCluster(kafkaConnectAssembly.getMetadata().getNamespace(),
                    kafkaConnectAssembly.getMetadata().getName(),
                    Labels.fromResource(kafkaConnectAssembly).withKind(kafkaConnectAssembly.getKind())));
    }

    /**
     * Abstracts the calling of setters on a (subclass of) KafkaConnectCluster
     * from the instantiation of the (subclass of) KafkaConnectCluster,
     * thus permitting reuse of the setter-calling code for subclasses.
     */
    protected static <C extends KafkaConnectCluster> C fromSpec(KafkaConnectAssemblySpec spec, C kafkaConnect) {
        kafkaConnect.setReplicas(spec != null && spec.getReplicas() > 0 ? spec.getReplicas() : DEFAULT_REPLICAS);
        kafkaConnect.setConfiguration(new KafkaConnectConfiguration(spec != null ? spec.getConfig().entrySet() : emptySet()));
        if (spec != null) {
            if (spec.getImage() != null) {
                kafkaConnect.setImage(spec.getImage());
            }

            kafkaConnect.setResources(spec.getResources());
            kafkaConnect.setLogging(spec.getLogging());
            kafkaConnect.setJvmOptions(spec.getJvmOptions());
            if (spec.getReadinessProbe() != null) {
                kafkaConnect.setReadinessInitialDelay(spec.getReadinessProbe().getInitialDelaySeconds());
                kafkaConnect.setReadinessTimeout(spec.getReadinessProbe().getTimeoutSeconds());
            }
            if (spec.getLivenessProbe() != null) {
                kafkaConnect.setLivenessInitialDelay(spec.getLivenessProbe().getInitialDelaySeconds());
                kafkaConnect.setLivenessTimeout(spec.getLivenessProbe().getTimeoutSeconds());
            }

            Map<String, Object> metrics = spec.getMetrics();
            if (metrics != null && !metrics.isEmpty()) {
                kafkaConnect.setMetricsEnabled(true);
                kafkaConnect.setMetricsConfig(metrics.entrySet());
            }
            kafkaConnect.setUserAffinity(spec.getAffinity());
            kafkaConnect.setTolerations(spec.getTolerations());
        }
        return kafkaConnect;
    }

    public Service generateService() {
        List<ServicePort> ports = new ArrayList<>(2);
        ports.add(createServicePort(REST_API_PORT_NAME, REST_API_PORT, REST_API_PORT, "TCP"));
        if (isMetricsEnabled()) {
            ports.add(createServicePort(METRICS_PORT_NAME, METRICS_PORT, METRICS_PORT, "TCP"));
        }

        return createService("ClusterIP", ports, getPrometheusAnnotations());
    }

    protected List<ContainerPort> getContainerPortList() {
        List<ContainerPort> portList = new ArrayList<>(2);
        portList.add(createContainerPort(REST_API_PORT_NAME, REST_API_PORT, "TCP"));
        if (isMetricsEnabled) {
            portList.add(createContainerPort(metricsPortName, metricsPort, "TCP"));
        }

        return portList;
    }

    protected List<Volume> getVolumes() {
        List<Volume> volumeList = new ArrayList<>(1);
        volumeList.add(createConfigMapVolume(logAndMetricsConfigVolumeName, ancillaryConfigName));

        return volumeList;
    }

    protected List<VolumeMount> getVolumeMounts() {
        List<VolumeMount> volumeMountList = new ArrayList<>(1);
        volumeMountList.add(createVolumeMount(logAndMetricsConfigVolumeName, logAndMetricsConfigMountPath));

        return volumeMountList;
    }

    public Deployment generateDeployment() {
        DeploymentStrategy updateStrategy = new DeploymentStrategyBuilder()
                .withType("RollingUpdate")
                .withRollingUpdate(new RollingUpdateDeploymentBuilder()
                        .withMaxSurge(new IntOrString(1))
                        .withMaxUnavailable(new IntOrString(0))
                        .build())
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
                .withName(name)
                .withImage(getImage())
                .withEnv(getEnvVars())
                .withPorts(getContainerPortList())
                .withLivenessProbe(createHttpProbe(livenessPath, REST_API_PORT_NAME, livenessInitialDelay, livenessTimeout))
                .withReadinessProbe(createHttpProbe(readinessPath, REST_API_PORT_NAME, readinessInitialDelay, readinessTimeout))
                .withVolumeMounts(getVolumeMounts())
                .withResources(resources(getResources()))
                .build();

        containers.add(container);

        return containers;
    }

    @Override
    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(buildEnvVar(ENV_VAR_KAFKA_CONNECT_CONFIGURATION, configuration.getConfiguration()));
        varList.add(buildEnvVar(ENV_VAR_KAFKA_CONNECT_METRICS_ENABLED, String.valueOf(isMetricsEnabled)));
        heapOptions(varList, 1.0, 0L);
        jvmPerformanceOptions(varList);
        if (getLogging() != null && getLogging().getCm() != null) {
            varList.add(buildEnvVar(ENV_VAR_KAFKA_CONNECT_LOGGING, getLogging().getCm().toString()));
        }
        return varList;
    }

    @Override
    protected String getDefaultLogConfigFileName() {
        return "kafkaConnectDefaultLoggingProperties";
    }
}
