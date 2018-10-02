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
import io.strimzi.api.kafka.model.CertAndKeySecretSource;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectAuthenticationScramSha512;
import io.strimzi.api.kafka.model.KafkaConnectAuthenticationTls;
import io.strimzi.api.kafka.model.KafkaConnectSpec;
import io.strimzi.api.kafka.model.PasswordSecretSource;
import io.strimzi.operator.common.model.Labels;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptySet;

public class KafkaConnectCluster extends AbstractModel {

    // Port configuration
    protected static final int REST_API_PORT = 8083;
    protected static final String REST_API_PORT_NAME = "rest-api";

    private static final String NAME_SUFFIX = "-connect";
    private static final String SERVICE_NAME_SUFFIX = NAME_SUFFIX + "-api";

    private static final String METRICS_AND_LOG_CONFIG_SUFFIX = NAME_SUFFIX + "-config";
    protected static final String TLS_CERTS_BASE_VOLUME_MOUNT = "/opt/kafka/connect-certs/";
    protected static final String PASSWORD_VOLUME_MOUNT = "/opt/kafka/connect-password/";

    // Configuration defaults
    protected static final int DEFAULT_REPLICAS = 3;
    protected static final int DEFAULT_HEALTHCHECK_DELAY = 60;
    protected static final int DEFAULT_HEALTHCHECK_TIMEOUT = 5;
    protected static final boolean DEFAULT_KAFKA_CONNECT_METRICS_ENABLED = false;

    // Kafka Connect configuration keys (EnvVariables)
    protected static final String ENV_VAR_KAFKA_CONNECT_CONFIGURATION = "KAFKA_CONNECT_CONFIGURATION";
    protected static final String ENV_VAR_KAFKA_CONNECT_METRICS_ENABLED = "KAFKA_CONNECT_METRICS_ENABLED";
    protected static final String ENV_VAR_KAFKA_CONNECT_BOOTSTRAP_SERVERS = "KAFKA_CONNECT_BOOTSTRAP_SERVERS";
    protected static final String ENV_VAR_KAFKA_CONNECT_TRUSTED_CERTS = "KAFKA_CONNECT_TRUSTED_CERTS";
    protected static final String ENV_VAR_KAFKA_CONNECT_TLS_AUTH_CERT = "KAFKA_CONNECT_TLS_AUTH_CERT";
    protected static final String ENV_VAR_KAFKA_CONNECT_TLS_AUTH_KEY = "KAFKA_CONNECT_TLS_AUTH_KEY";
    protected static final String ENV_VAR_KAFKA_CONNECT_SASL_PASSWORD_FILE = "KAFKA_CONNECT_SASL_PASSWORD_FILE";
    protected static final String ENV_VAR_KAFKA_CONNECT_SASL_USERNAME = "KAFKA_CONNECT_SASL_USERNAME";

    protected String bootstrapServers;

    private List<CertSecretSource> trustedCertificates;
    private CertAndKeySecretSource tlsAuthCertAndKey;
    private PasswordSecretSource passwordSecret;
    private String username;

    /**
     * Constructor
     *
     * @param namespace Kubernetes/OpenShift namespace where Kafka Connect cluster resources are going to be created
     * @param cluster   overall cluster name
     * @param labels    labels to add to the cluster
     */
    protected KafkaConnectCluster(String namespace, String cluster, Labels labels) {
        super(namespace, cluster, labels);
        this.name = kafkaConnectClusterName(cluster);
        this.serviceName = serviceName(cluster);
        this.validLoggerFields = getDefaultLogConfig();
        this.ancillaryConfigName = logAndMetricsConfigName(cluster);
        this.image = KafkaConnectSpec.DEFAULT_IMAGE;
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

    public static KafkaConnectCluster fromCrd(KafkaConnect kafkaConnect) {
        KafkaConnectCluster cluster = fromSpec(kafkaConnect.getSpec(), new KafkaConnectCluster(kafkaConnect.getMetadata().getNamespace(),
                kafkaConnect.getMetadata().getName(), Labels.fromResource(kafkaConnect).withKind(kafkaConnect.getKind())));

        cluster.setOwnerReference(kafkaConnect);

        return cluster;
    }

    /**
     * Abstracts the calling of setters on a (subclass of) KafkaConnectCluster
     * from the instantiation of the (subclass of) KafkaConnectCluster,
     * thus permitting reuse of the setter-calling code for subclasses.
     */
    protected static <C extends KafkaConnectCluster> C fromSpec(KafkaConnectSpec spec, C kafkaConnect) {
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
            kafkaConnect.setBootstrapServers(spec.getBootstrapServers());

            if (spec.getTls() != null) {
                kafkaConnect.setTrustedCertificates(spec.getTls().getTrustedCertificates());
            }

            if (spec.getAuthentication() != null)   {
                if (spec.getAuthentication() instanceof KafkaConnectAuthenticationTls) {
                    KafkaConnectAuthenticationTls auth = (KafkaConnectAuthenticationTls) spec.getAuthentication();
                    if (auth.getCertificateAndKey() != null) {
                        kafkaConnect.setTlsAuthCertAndKey(auth.getCertificateAndKey());
                        if (spec.getTls() == null) {
                            log.warn("TLS configuration missing: related TLS client authentication will not work properly");
                        }
                    } else {
                        log.warn("TLS Client authentication selected, but no certificate and key configured.");
                        throw new InvalidResourceException("TLS Client authentication selected, but no certificate and key configured.");
                    }
                } else if (spec.getAuthentication() instanceof KafkaConnectAuthenticationScramSha512)    {
                    KafkaConnectAuthenticationScramSha512 auth = (KafkaConnectAuthenticationScramSha512) spec.getAuthentication();
                    if (auth.getUsername() != null && auth.getPasswordSecret() != null) {
                        kafkaConnect.setUsernameAndPassword(auth.getUsername(), auth.getPasswordSecret());
                    } else  {
                        log.warn("SCRAM-SHA-512 authentication selected, but no username and password configured.");
                        throw new InvalidResourceException("SCRAM-SHA-512 authentication selected, but no username and password configured.");
                    }
                }
            }
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
            portList.add(createContainerPort(METRICS_PORT_NAME, METRICS_PORT, "TCP"));
        }

        return portList;
    }

    protected List<Volume> getVolumes() {
        List<Volume> volumeList = new ArrayList<>(1);
        volumeList.add(createConfigMapVolume(logAndMetricsConfigVolumeName, ancillaryConfigName));
        if (trustedCertificates != null && trustedCertificates.size() > 0) {
            for (CertSecretSource certSecretSource: trustedCertificates) {
                // skipping if a volume with same Secret name was already added
                if (!volumeList.stream().anyMatch(v -> v.getName().equals(certSecretSource.getSecretName()))) {
                    volumeList.add(createSecretVolume(certSecretSource.getSecretName(), certSecretSource.getSecretName()));
                }
            }
        }
        if (tlsAuthCertAndKey != null) {
            // skipping if a volume with same Secret name was already added
            if (!volumeList.stream().anyMatch(v -> v.getName().equals(tlsAuthCertAndKey.getSecretName()))) {
                volumeList.add(createSecretVolume(tlsAuthCertAndKey.getSecretName(), tlsAuthCertAndKey.getSecretName()));
            }
        } else if (passwordSecret != null)  {
            volumeList.add(createSecretVolume(passwordSecret.getSecretName(), passwordSecret.getSecretName()));
        }

        return volumeList;
    }

    protected List<VolumeMount> getVolumeMounts() {
        List<VolumeMount> volumeMountList = new ArrayList<>(1);
        volumeMountList.add(createVolumeMount(logAndMetricsConfigVolumeName, logAndMetricsConfigMountPath));
        if (trustedCertificates != null && trustedCertificates.size() > 0) {
            for (CertSecretSource certSecretSource: trustedCertificates) {
                // skipping if a volume mount with same Secret name was already added
                if (!volumeMountList.stream().anyMatch(vm -> vm.getName().equals(certSecretSource.getSecretName()))) {
                    volumeMountList.add(createVolumeMount(certSecretSource.getSecretName(),
                            TLS_CERTS_BASE_VOLUME_MOUNT + certSecretSource.getSecretName()));
                }
            }
        }
        if (tlsAuthCertAndKey != null) {
            // skipping if a volume mount with same Secret name was already added
            if (!volumeMountList.stream().anyMatch(vm -> vm.getName().equals(tlsAuthCertAndKey.getSecretName()))) {
                volumeMountList.add(createVolumeMount(tlsAuthCertAndKey.getSecretName(),
                        TLS_CERTS_BASE_VOLUME_MOUNT + tlsAuthCertAndKey.getSecretName()));
            }
        } else if (passwordSecret != null)  {
            volumeMountList.add(createVolumeMount(passwordSecret.getSecretName(),
                    PASSWORD_VOLUME_MOUNT + passwordSecret.getSecretName()));
        }

        return volumeMountList;
    }

    public Deployment generateDeployment(Map<String, String> annotations) {
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
                annotations,
                getMergedAffinity(),
                getInitContainers(),
                getContainers(),
                getVolumes());
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
        varList.add(buildEnvVar(ENV_VAR_KAFKA_CONNECT_BOOTSTRAP_SERVERS, bootstrapServers));
        heapOptions(varList, 1.0, 0L);
        jvmPerformanceOptions(varList);
        if (trustedCertificates != null && trustedCertificates.size() > 0) {
            StringBuilder sb = new StringBuilder();
            boolean separator = false;
            for (CertSecretSource certSecretSource: trustedCertificates) {
                if (separator) {
                    sb.append(";");
                }
                sb.append(certSecretSource.getSecretName() + "/" + certSecretSource.getCertificate());
                separator = true;
            }
            varList.add(buildEnvVar(ENV_VAR_KAFKA_CONNECT_TRUSTED_CERTS, sb.toString()));
        }
        if (tlsAuthCertAndKey != null) {
            varList.add(buildEnvVar(ENV_VAR_KAFKA_CONNECT_TLS_AUTH_CERT,
                    String.format("%s/%s", tlsAuthCertAndKey.getSecretName(), tlsAuthCertAndKey.getCertificate())));
            varList.add(buildEnvVar(ENV_VAR_KAFKA_CONNECT_TLS_AUTH_KEY,
                    String.format("%s/%s", tlsAuthCertAndKey.getSecretName(), tlsAuthCertAndKey.getKey())));
        } else if (passwordSecret != null)  {
            varList.add(buildEnvVar(ENV_VAR_KAFKA_CONNECT_SASL_USERNAME, username));
            varList.add(buildEnvVar(ENV_VAR_KAFKA_CONNECT_SASL_PASSWORD_FILE,
                    String.format("%s/%s", passwordSecret.getSecretName(), passwordSecret.getPassword())));
        }

        return varList;
    }

    @Override
    protected String getDefaultLogConfigFileName() {
        return "kafkaConnectDefaultLoggingProperties";
    }

    /**
     * Set the bootstrap servers to connect to
     * @param bootstrapServers bootstrap servers comma separated list
     */
    protected void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    /**
     * Set the trusted certificates with the certificate to trust
     * @param trustedCertificates trusted certificates list
     */
    protected void setTrustedCertificates(List<CertSecretSource> trustedCertificates) {
        this.trustedCertificates = trustedCertificates;
    }

    /**
     * Set the certificate and related private key for TLS based authentication
     * @param tlsAuthCertAndKey certificate and private key bundle
     */
    protected void setTlsAuthCertAndKey(CertAndKeySecretSource tlsAuthCertAndKey) {
        this.tlsAuthCertAndKey = tlsAuthCertAndKey;
    }

    /**
     * Set the username and password for SASL SCRAM-SHA-512 based authentication
     *
     * @param username          Username
     * @param passwordSecret    Secret with password
     */
    protected void setUsernameAndPassword(String username, PasswordSecretSource passwordSecret) {
        this.username = username;
        this.passwordSecret = passwordSecret;
    }
}
