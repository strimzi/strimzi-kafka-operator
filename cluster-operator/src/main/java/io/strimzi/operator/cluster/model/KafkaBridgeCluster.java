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
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentStrategy;
import io.fabric8.kubernetes.api.model.apps.DeploymentStrategyBuilder;
import io.fabric8.kubernetes.api.model.apps.RollingUpdateDeploymentBuilder;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudget;
import io.strimzi.api.kafka.model.CertAndKeySecretSource;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.Http;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.KafkaBridgeAuthenticationPlain;
import io.strimzi.api.kafka.model.KafkaBridgeAuthenticationScramSha512;
import io.strimzi.api.kafka.model.KafkaBridgeAuthenticationTls;
import io.strimzi.api.kafka.model.KafkaBridgeSpec;
import io.strimzi.api.kafka.model.KafkaBridgeTls;
import io.strimzi.api.kafka.model.PasswordSecretSource;
import io.strimzi.api.kafka.model.template.KafkaBridgeTemplate;
import io.strimzi.operator.common.model.Labels;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class KafkaBridgeCluster extends AbstractModel {

    // Port configuration
    protected static final int DEFAULT_REST_API_PORT = 8080;
    protected static final String REST_API_PORT_NAME = "rest-api";

    private static final String NAME_SUFFIX = "-bridge";
    private static final String SERVICE_NAME_SUFFIX = NAME_SUFFIX + "-api";

    private static final String METRICS_AND_LOG_CONFIG_SUFFIX = NAME_SUFFIX + "-config";
    protected static final String TLS_CERTS_BASE_VOLUME_MOUNT = "/opt/bridge/bridge-certs/";
    protected static final String PASSWORD_VOLUME_MOUNT = "/opt/bridge/bridge-password/";
    protected static final String DEFAULT_BRIDGE_IMAGE = "strimzi/kafka-bridge:latest";


    // Configuration defaults
    protected static final int DEFAULT_REPLICAS = 1;
    protected static final int DEFAULT_HEALTHCHECK_DELAY = 15;
    protected static final int DEFAULT_HEALTHCHECK_TIMEOUT = 5;
    protected static final boolean DEFAULT_KAFKA_BRIDGE_METRICS_ENABLED = false;

    // Kafka Bridge configuration keys (EnvVariables)
    protected static final String ENV_VAR_KAFKA_BRIDGE_METRICS_ENABLED = "KAFKA_BRIDGE_METRICS_ENABLED";
    protected static final String ENV_VAR_KAFKA_BRIDGE_BOOTSTRAP_SERVERS = "KAFKA_BRIDGE_BOOTSTRAP_SERVERS";
    protected static final String ENV_VAR_KAFKA_BRIDGE_TLS = "KAFKA_BRIDGE_TLS";
    protected static final String ENV_VAR_KAFKA_BRIDGE_TRUSTED_CERTS = "KAFKA_BRIDGE_TRUSTED_CERTS";
    protected static final String ENV_VAR_KAFKA_BRIDGE_TLS_AUTH_CERT = "KAFKA_BRIDGE_TLS_AUTH_CERT";
    protected static final String ENV_VAR_KAFKA_BRIDGE_TLS_AUTH_KEY = "KAFKA_BRIDGE_TLS_AUTH_KEY";
    protected static final String ENV_VAR_KAFKA_BRIDGE_SASL_PASSWORD_FILE = "KAFKA_BRIDGE_SASL_PASSWORD_FILE";
    protected static final String ENV_VAR_KAFKA_BRIDGE_SASL_USERNAME = "KAFKA_BRIDGE_SASL_USERNAME";
    protected static final String ENV_VAR_KAFKA_BRIDGE_SASL_MECHANISM = "KAFKA_BRIDGE_SASL_MECHANISM";

    protected String bootstrapServers;
    private KafkaBridgeTls tls;
    private CertAndKeySecretSource tlsAuthCertAndKey;
    private PasswordSecretSource passwordSecret;
    private String username;
    private String saslMechanism;
    private Http http;

    /**
     * Constructor
     *
     * @param namespace Kubernetes/OpenShift namespace where Kafka Bridge cluster resources are going to be created
     * @param cluster   overall cluster name
     * @param labels    labels to add to the cluster
     */
    protected KafkaBridgeCluster(String namespace, String cluster, Labels labels) {
        super(namespace, cluster, labels);
        this.name = kafkaBridgeClusterName(cluster);
        this.serviceName = serviceName(cluster);
        this.ancillaryConfigName = logAndMetricsConfigName(cluster);
        this.replicas = DEFAULT_REPLICAS;
        this.readinessPath = "/";
        this.readinessTimeout = DEFAULT_HEALTHCHECK_TIMEOUT;
        this.readinessInitialDelay = DEFAULT_HEALTHCHECK_DELAY;
        this.livenessPath = "/";
        this.livenessTimeout = DEFAULT_HEALTHCHECK_TIMEOUT;
        this.livenessInitialDelay = DEFAULT_HEALTHCHECK_DELAY;
        this.isMetricsEnabled = DEFAULT_KAFKA_BRIDGE_METRICS_ENABLED;

        this.mountPath = "/var/lib/bridge";
        this.logAndMetricsConfigVolumeName = "kafka-metrics-and-logging";
        this.logAndMetricsConfigMountPath = "/opt/bridge/custom-config/";
    }

    public static String kafkaBridgeClusterName(String cluster) {
        return cluster + KafkaBridgeCluster.NAME_SUFFIX;
    }

    public static String serviceName(String cluster) {
        return cluster + KafkaBridgeCluster.SERVICE_NAME_SUFFIX;
    }

    public static String logAndMetricsConfigName(String cluster) {
        return cluster + KafkaBridgeCluster.METRICS_AND_LOG_CONFIG_SUFFIX;
    }

    public static KafkaBridgeCluster fromCrd(KafkaBridge kafkaBridge, KafkaVersion.Lookup versions) {

        KafkaBridgeCluster kafkaBridgeCluster = new KafkaBridgeCluster(kafkaBridge.getMetadata().getNamespace(),
                kafkaBridge.getMetadata().getName(), Labels.fromResource(kafkaBridge).withKind(kafkaBridge.getKind()));

        KafkaBridgeSpec spec = kafkaBridge.getSpec();
        kafkaBridgeCluster.setResources(spec.getResources());
        kafkaBridgeCluster.setLogging(spec.getLogging());
        kafkaBridgeCluster.setGcLoggingEnabled(spec.getJvmOptions() == null ? true : spec.getJvmOptions().isGcLoggingEnabled());
        kafkaBridgeCluster.setJvmOptions(spec.getJvmOptions());
        kafkaBridgeCluster.setImage(spec.getImage() == null ? DEFAULT_BRIDGE_IMAGE : spec.getImage());
        kafkaBridgeCluster.setReplicas(spec != null && spec.getReplicas() > 0 ? spec.getReplicas() : DEFAULT_REPLICAS);

        Map<String, Object> metrics = spec.getMetrics();
        if (metrics != null) {
            kafkaBridgeCluster.setMetricsEnabled(true);
            kafkaBridgeCluster.setMetricsConfig(metrics.entrySet());
        }

        kafkaBridgeCluster.setBootstrapServers(spec.getBootstrapServers());

        kafkaBridgeCluster.setTls(spec.getTls());

        if (spec.getAuthentication() != null)   {
            if (spec.getAuthentication() instanceof KafkaBridgeAuthenticationTls) {
                KafkaBridgeAuthenticationTls auth = (KafkaBridgeAuthenticationTls) spec.getAuthentication();
                if (auth.getCertificateAndKey() != null) {
                    kafkaBridgeCluster.setTlsAuthCertAndKey(auth.getCertificateAndKey());
                    if (spec.getTls() == null) {
                        log.warn("TLS configuration missing: related TLS client authentication will not work properly");
                    }
                } else {
                    log.warn("TLS Client authentication selected, but no certificate and key configured.");
                    throw new InvalidResourceException("TLS Client authentication selected, but no certificate and key configured.");
                }
            } else if (spec.getAuthentication() instanceof KafkaBridgeAuthenticationScramSha512)    {
                KafkaBridgeAuthenticationScramSha512 auth = (KafkaBridgeAuthenticationScramSha512) spec.getAuthentication();
                if (auth.getUsername() != null && auth.getPasswordSecret() != null) {
                    kafkaBridgeCluster.setUsernameAndPassword(auth.getUsername(), auth.getPasswordSecret());
                    kafkaBridgeCluster.setSaslMechanism(auth.getType());
                } else  {
                    log.warn("SCRAM-SHA-512 authentication selected, but no username and password configured.");
                    throw new InvalidResourceException("SCRAM-SHA-512 authentication selected, but no username and password configured.");
                }
            } else if (spec.getAuthentication() instanceof KafkaBridgeAuthenticationPlain) {
                KafkaBridgeAuthenticationPlain auth = (KafkaBridgeAuthenticationPlain) spec.getAuthentication();
                if (auth.getUsername() != null && auth.getPasswordSecret() != null) {
                    kafkaBridgeCluster.setUsernameAndPassword(auth.getUsername(), auth.getPasswordSecret());
                    kafkaBridgeCluster.setSaslMechanism(auth.getType());
                } else  {
                    log.warn("PLAIN authentication selected, but no username and password configured.");
                    throw new InvalidResourceException("PLAIN authentication selected, but no username and password configured.");
                }
            }
        }

        if (spec.getTemplate() != null) {
            KafkaBridgeTemplate template = spec.getTemplate();

            if (template.getDeployment() != null && template.getDeployment().getMetadata() != null)  {
                kafkaBridgeCluster.templateDeploymentLabels = template.getDeployment().getMetadata().getLabels();
                kafkaBridgeCluster.templateDeploymentAnnotations = template.getDeployment().getMetadata().getAnnotations();
            }

            ModelUtils.parsePodTemplate(kafkaBridgeCluster, template.getPod());

            if (template.getApiService() != null && template.getApiService().getMetadata() != null)  {
                kafkaBridgeCluster.templateServiceLabels = template.getApiService().getMetadata().getLabels();
                kafkaBridgeCluster.templateServiceAnnotations = template.getApiService().getMetadata().getAnnotations();
            }

            ModelUtils.parsePodDisruptionBudgetTemplate(kafkaBridgeCluster, template.getPodDisruptionBudget());
        }

        kafkaBridgeCluster.setHttp(spec.getHttp());
        kafkaBridgeCluster.setOwnerReference(kafkaBridge);

        return kafkaBridgeCluster;
    }
    
    public Service generateService() {
        List<ServicePort> ports = new ArrayList<>(2);
        int port = DEFAULT_REST_API_PORT;
        if (http != null) {
            port = http.getPort();
        }
        ports.add(createServicePort(REST_API_PORT_NAME, port, port, "TCP"));
        if (isMetricsEnabled()) {
            ports.add(createServicePort(METRICS_PORT_NAME, METRICS_PORT, METRICS_PORT, "TCP"));
        }

        return createService("ClusterIP", ports, templateServiceAnnotations);
    }

    protected List<ContainerPort> getContainerPortList() {
        List<ContainerPort> portList = new ArrayList<>(2);
        portList.add(createContainerPort(REST_API_PORT_NAME, DEFAULT_REST_API_PORT, "TCP"));
        if (isMetricsEnabled) {
            portList.add(createContainerPort(METRICS_PORT_NAME, METRICS_PORT, "TCP"));
        }

        return portList;
    }

    protected List<Volume> getVolumes(boolean isOpenShift) {
        List<Volume> volumeList = new ArrayList<>(1);
        volumeList.add(createConfigMapVolume(logAndMetricsConfigVolumeName, ancillaryConfigName));

        if (tls != null) {
            List<CertSecretSource> trustedCertificates = tls.getTrustedCertificates();

            if (trustedCertificates != null && trustedCertificates.size() > 0) {
                for (CertSecretSource certSecretSource : trustedCertificates) {
                    // skipping if a volume with same Secret name was already added
                    if (!volumeList.stream().anyMatch(v -> v.getName().equals(certSecretSource.getSecretName()))) {
                        volumeList.add(createSecretVolume(certSecretSource.getSecretName(), certSecretSource.getSecretName(), isOpenShift));
                    }
                }
            }
        }

        if (tlsAuthCertAndKey != null) {
            // skipping if a volume with same Secret name was already added
            if (!volumeList.stream().anyMatch(v -> v.getName().equals(tlsAuthCertAndKey.getSecretName()))) {
                volumeList.add(createSecretVolume(tlsAuthCertAndKey.getSecretName(), tlsAuthCertAndKey.getSecretName(), isOpenShift));
            }
        } else if (passwordSecret != null)  {
            volumeList.add(createSecretVolume(passwordSecret.getSecretName(), passwordSecret.getSecretName(), isOpenShift));
        }

        return volumeList;
    }


    protected List<VolumeMount> getVolumeMounts() {
        List<VolumeMount> volumeMountList = new ArrayList<>(1);
        volumeMountList.add(createVolumeMount(logAndMetricsConfigVolumeName, logAndMetricsConfigMountPath));

        if (tls != null) {
            List<CertSecretSource> trustedCertificates = tls.getTrustedCertificates();

            if (trustedCertificates != null && trustedCertificates.size() > 0) {
                for (CertSecretSource certSecretSource : trustedCertificates) {
                    // skipping if a volume mount with same Secret name was already added
                    if (!volumeMountList.stream().anyMatch(vm -> vm.getName().equals(certSecretSource.getSecretName()))) {
                        volumeMountList.add(createVolumeMount(certSecretSource.getSecretName(),
                                TLS_CERTS_BASE_VOLUME_MOUNT + certSecretSource.getSecretName()));
                    }
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

    public Deployment generateDeployment(Map<String, String> annotations, boolean isOpenShift, ImagePullPolicy imagePullPolicy, List<LocalObjectReference> imagePullSecrets) {
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
                getInitContainers(imagePullPolicy),
                getContainers(imagePullPolicy),
                getVolumes(isOpenShift),
                imagePullSecrets);
    }

    @Override
    protected List<Container> getContainers(ImagePullPolicy imagePullPolicy) {

        List<Container> containers = new ArrayList<>();

        Container container = new ContainerBuilder()
                .withName(name)
                .withImage(getImage())
                .withCommand("/run_bridge.sh")
                .withEnv(getEnvVars())
                .withPorts(getContainerPortList())
                .withLivenessProbe(createHttpProbe(livenessPath, REST_API_PORT_NAME, livenessInitialDelay, livenessTimeout))
                .withReadinessProbe(createHttpProbe(readinessPath, REST_API_PORT_NAME, readinessInitialDelay, readinessTimeout))
                .withVolumeMounts(getVolumeMounts())
                .withResources(getResources())
                .withImagePullPolicy(determineImagePullPolicy(imagePullPolicy, getImage()))
                .build();

        containers.add(container);

        return containers;
    }

    @Override
    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(buildEnvVar(ENV_VAR_KAFKA_BRIDGE_METRICS_ENABLED, String.valueOf(isMetricsEnabled)));
        varList.add(buildEnvVar(ENV_VAR_KAFKA_BRIDGE_BOOTSTRAP_SERVERS, bootstrapServers));
        varList.add(buildEnvVar(ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED, String.valueOf(gcLoggingEnabled)));

        heapOptions(varList, 1.0, 0L);
        jvmPerformanceOptions(varList);

        if (tls != null) {
            varList.add(buildEnvVar(ENV_VAR_KAFKA_BRIDGE_TLS, "true"));

            List<CertSecretSource> trustedCertificates = tls.getTrustedCertificates();

            if (trustedCertificates != null && trustedCertificates.size() > 0) {
                StringBuilder sb = new StringBuilder();
                boolean separator = false;
                for (CertSecretSource certSecretSource : trustedCertificates) {
                    if (separator) {
                        sb.append(";");
                    }
                    sb.append(certSecretSource.getSecretName() + "/" + certSecretSource.getCertificate());
                    separator = true;
                }
                varList.add(buildEnvVar(ENV_VAR_KAFKA_BRIDGE_TRUSTED_CERTS, sb.toString()));
            }
        }

        if (tlsAuthCertAndKey != null) {
            varList.add(buildEnvVar(ENV_VAR_KAFKA_BRIDGE_TLS_AUTH_CERT,
                    String.format("%s/%s", tlsAuthCertAndKey.getSecretName(), tlsAuthCertAndKey.getCertificate())));
            varList.add(buildEnvVar(ENV_VAR_KAFKA_BRIDGE_TLS_AUTH_KEY,
                    String.format("%s/%s", tlsAuthCertAndKey.getSecretName(), tlsAuthCertAndKey.getKey())));
        } else if (passwordSecret != null) {
            varList.add(buildEnvVar(ENV_VAR_KAFKA_BRIDGE_SASL_USERNAME, username));
            varList.add(buildEnvVar(ENV_VAR_KAFKA_BRIDGE_SASL_PASSWORD_FILE,
                    String.format("%s/%s", passwordSecret.getSecretName(), passwordSecret.getPassword())));
            varList.add(buildEnvVar(ENV_VAR_KAFKA_BRIDGE_SASL_MECHANISM, saslMechanism));
        }

        return varList;
    }


    @Override
    protected String getDefaultLogConfigFileName() {
        return "kafkaBridgeDefaultLoggingProperties";
    }

    /**
     * Set the HTTP configuration
     * @param http HTTP configuration
     */
    protected void setHttp(Http http) {
        this.http = http;
    }

    /**
     * Set the bootstrap servers to connect to
     * @param bootstrapServers bootstrap servers comma separated list
     */
    protected void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    /**
     * Set the tls configuration with the certificate to trust
     *
     * @param tls trusted certificates list
     */
    protected void setTls(KafkaBridgeTls tls) {
        this.tls = tls;
    }

    /**
     * Set the certificate and related private key for TLS based authentication
     * @param tlsAuthCertAndKey certificate and private key bundle
     */
    protected void setTlsAuthCertAndKey(CertAndKeySecretSource tlsAuthCertAndKey) {
        this.tlsAuthCertAndKey = tlsAuthCertAndKey;
    }

    /**
     * Set the username and password for SASL based authentication
     *
     * @param username          Username
     * @param passwordSecret    Secret with password
     */
    protected void setUsernameAndPassword(String username, PasswordSecretSource passwordSecret) {
        this.username = username;
        this.passwordSecret = passwordSecret;
    }

    /**
     * Set the sasl mechanism, supported mechanisms including scram-sha-512 and plain
     * @param saslMechanism
     */
    protected void setSaslMechanism(String saslMechanism) {
        this.saslMechanism = saslMechanism;
    }

    /**
     * Generates the PodDisruptionBudget
     *
     * @return
     */
    public PodDisruptionBudget generatePodDisruptionBudget() {
        return createPodDisruptionBudget();
    }

    @Override
    protected String getServiceAccountName() {
        return containerServiceAccountName(cluster);
    }

    /**
     * Get the name of the bridge service account given the name of the {@code bridgeResourceName}.
     */
    public static String containerServiceAccountName(String bridgeResourceName) {
        return kafkaBridgeClusterName(bridgeResourceName);
    }
}
