/*
 * Copyright 2017-2019, Strimzi authors.
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
import io.strimzi.api.kafka.model.KafkaBridgeConsumerSpec;
import io.strimzi.api.kafka.model.KafkaBridgeHttpConfig;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.KafkaBridgeAuthenticationPlain;
import io.strimzi.api.kafka.model.KafkaBridgeAuthenticationScramSha512;
import io.strimzi.api.kafka.model.KafkaBridgeAuthenticationTls;
import io.strimzi.api.kafka.model.KafkaBridgeProducerSpec;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaBridgeSpec;
import io.strimzi.api.kafka.model.KafkaBridgeTls;
import io.strimzi.api.kafka.model.PasswordSecretSource;
import io.strimzi.api.kafka.model.Probe;
import io.strimzi.api.kafka.model.ProbeBuilder;
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

    protected static final int HEALTH_CHECK_PORT = 8081;
    protected static final String HEALTH_CHECK_PORT_NAME = "healthcheck";

    private static final String NAME_SUFFIX = "-bridge";
    private static final String SERVICE_NAME_SUFFIX = NAME_SUFFIX + "-api";

    private static final String METRICS_AND_LOG_CONFIG_SUFFIX = NAME_SUFFIX + "-config";
    protected static final String TLS_CERTS_BASE_VOLUME_MOUNT = "/opt/strimzi/bridge-certs/";
    protected static final String PASSWORD_VOLUME_MOUNT = "/opt/strimzi/bridge-password/";

    // Configuration defaults
    protected static final int DEFAULT_REPLICAS = 1;
    protected static final int DEFAULT_HEALTHCHECK_DELAY = 15;
    protected static final int DEFAULT_HEALTHCHECK_TIMEOUT = 5;
    protected static final boolean DEFAULT_KAFKA_BRIDGE_METRICS_ENABLED = false;

    public static final Probe DEFAULT_HEALTHCHECK_OPTIONS = new ProbeBuilder()
            .withTimeoutSeconds(DEFAULT_HEALTHCHECK_TIMEOUT)
            .withInitialDelaySeconds(DEFAULT_HEALTHCHECK_DELAY).build();

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

    protected static final String ENV_VAR_KAFKA_BRIDGE_PRODUCER_CONFIG = "KAFKA_BRIDGE_PRODUCER_CONFIG";
    protected static final String ENV_VAR_KAFKA_BRIDGE_CONSUMER_CONFIG = "KAFKA_BRIDGE_CONSUMER_CONFIG";

    protected static final String ENV_VAR_KAFKA_BRIDGE_AMQP_ENABLED = "KAFKA_BRIDGE_AMQP_ENABLED";
    protected static final String ENV_VAR_KAFKA_BRIDGE_AMQP_FLOW_CREDIT = "KAFKA_BRIDGE_AMQP_FLOW_CREDIT";
    protected static final String ENV_VAR_KAFKA_BRIDGE_AMQP_MODE = "KAFKA_BRIDGE_AMQP_MODE";
    protected static final String ENV_VAR_KAFKA_BRIDGE_AMQP_HOST = "KAFKA_BRIDGE_AMQP_HOST";
    protected static final String ENV_VAR_KAFKA_BRIDGE_AMQP_PORT = "KAFKA_BRIDGE_AMQP_PORT";
    protected static final String ENV_VAR_KAFKA_BRIDGE_AMQP_CERT_DIR = "KAFKA_BRIDGE_AMQP_CERT_DIR";
    protected static final String ENV_VAR_KAFKA_BRIDGE_AMQP_MESSAGE_CONNVERTER = "KAFKA_BRIDGE_AMQP_MESSAGE_CONNVERTER";

    protected static final String ENV_VAR_KAFKA_BRIDGE_HTTP_ENABLED = "KAFKA_BRIDGE_HTTP_ENABLED";
    protected static final String ENV_VAR_KAFKA_BRIDGE_HTTP_HOST = "KAFKA_BRIDGE_HTTP_HOST";
    protected static final String ENV_VAR_KAFKA_BRIDGE_HTTP_PORT = "KAFKA_BRIDGE_HTTP_PORT";

    private KafkaBridgeTls tls;
    private CertAndKeySecretSource tlsAuthCertAndKey;
    private PasswordSecretSource passwordSecret;
    private String username;
    private String saslMechanism;
    private KafkaBridgeHttpConfig http;
    private boolean httpEnabled = false;
    private boolean amqpEnabled = false;
    private String bootstrapServers;
    private KafkaBridgeConsumerSpec kafkaBridgeConsumer;
    private KafkaBridgeProducerSpec kafkaBridgeProducer;

    /**
     * Constructor
     *
     * @param namespace Kubernetes/OpenShift namespace where Kafka Bridge cluster resources are going to be created
     * @param cluster   overall cluster name
     * @param labels    labels to add to the cluster
     */
    protected KafkaBridgeCluster(String namespace, String cluster, Labels labels) {
        super(namespace, cluster, labels);
        this.name = KafkaBridgeResources.deploymentName(cluster);
        this.serviceName = name + KafkaBridgeResources.serviceName(cluster);
        this.ancillaryConfigName = KafkaBridgeResources.metricsAndLogConfigMapName(cluster);
        this.replicas = DEFAULT_REPLICAS;
        this.readinessPath = "/ready";
        this.livenessPath = "/healthy";
        this.livenessProbeOptions = DEFAULT_HEALTHCHECK_OPTIONS;
        this.readinessProbeOptions = DEFAULT_HEALTHCHECK_OPTIONS;
        this.isMetricsEnabled = DEFAULT_KAFKA_BRIDGE_METRICS_ENABLED;

        this.mountPath = "/var/lib/bridge";
        this.logAndMetricsConfigVolumeName = "kafka-metrics-and-logging";
        this.logAndMetricsConfigMountPath = "/opt/strimzi/custom-config/";
    }

    public static KafkaBridgeCluster fromCrd(KafkaBridge kafkaBridge, KafkaVersion.Lookup versions) {

        KafkaBridgeCluster kafkaBridgeCluster = new KafkaBridgeCluster(kafkaBridge.getMetadata().getNamespace(),
                kafkaBridge.getMetadata().getName(), Labels.fromResource(kafkaBridge).withKind(kafkaBridge.getKind()));

        KafkaBridgeSpec spec = kafkaBridge.getSpec();
        kafkaBridgeCluster.setResources(spec.getResources());
        kafkaBridgeCluster.setLogging(spec.getLogging());
        kafkaBridgeCluster.setGcLoggingEnabled(spec.getJvmOptions() == null ? true : spec.getJvmOptions().isGcLoggingEnabled());
        String image = spec.getImage();
        if (image == null) {
            image = System.getenv().getOrDefault("STRIMZI_DEFAULT_KAFKA_BRIDGE_IMAGE", "strimzi/kafka-bridge:latest");
        }
        kafkaBridgeCluster.setImage(image);
        kafkaBridgeCluster.setReplicas(spec.getReplicas() > 0 ? spec.getReplicas() : DEFAULT_REPLICAS);
        kafkaBridgeCluster.setBootstrapServers(spec.getBootstrapServers());
        kafkaBridgeCluster.setKafkaConsumerConfiguration(spec.getConsumer());
        kafkaBridgeCluster.setKafkaProducerConfiguration(spec.getProducer());
        if (kafkaBridge.getSpec().getLivenessProbe() != null) {
            kafkaBridgeCluster.setLivenessProbe(kafkaBridge.getSpec().getLivenessProbe());
        }

        if (kafkaBridge.getSpec().getReadinessProbe() != null) {
            kafkaBridgeCluster.setLivenessProbe(kafkaBridge.getSpec().getReadinessProbe());
        }

        Map<String, Object> metrics = spec.getMetrics();
        if (metrics != null) {
            kafkaBridgeCluster.setMetricsEnabled(true);
            kafkaBridgeCluster.setMetricsConfig(metrics.entrySet());
        }

        kafkaBridgeCluster.setTls(spec.getTls() != null ? spec.getTls() : null);

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

        if (spec.getHttp() != null) {
            kafkaBridgeCluster.setHttpEnabled(true);
            if (spec.getHttp().getPort() == HEALTH_CHECK_PORT) {
                log.warn("HTTP port cannot be set to {}. This port is already used for heath check.", HEALTH_CHECK_PORT);
                throw new InvalidResourceException("HTTP port cannot be set to " + HEALTH_CHECK_PORT + ". This port is already used for heath check.");
            } else {
                kafkaBridgeCluster.setKafkaBridgeHttpConfig(spec.getHttp());
            }
        } else {
            log.warn("No protocol specified.");
            throw new InvalidResourceException("No protocol for communication with Bridge specified. Use HTTP.");
        }
        kafkaBridgeCluster.setOwnerReference(kafkaBridge);

        return kafkaBridgeCluster;
    }
    
    public Service generateService() {
        List<ServicePort> ports = new ArrayList<>(3);
        int port = DEFAULT_REST_API_PORT;
        if (http != null) {
            port = http.getPort();
        }
        ports.add(createServicePort(REST_API_PORT_NAME, port, port, "TCP"));
        ports.add(createServicePort(HEALTH_CHECK_PORT_NAME, HEALTH_CHECK_PORT, HEALTH_CHECK_PORT, "TCP"));
        if (isMetricsEnabled()) {
            ports.add(createServicePort(METRICS_PORT_NAME, METRICS_PORT, METRICS_PORT, "TCP"));
        }

        return createService("ClusterIP", ports, templateServiceAnnotations);
    }

    protected List<ContainerPort> getContainerPortList() {
        List<ContainerPort> portList = new ArrayList<>(3);
        portList.add(createContainerPort(REST_API_PORT_NAME, DEFAULT_REST_API_PORT, "TCP"));
        portList.add(createContainerPort(HEALTH_CHECK_PORT_NAME, HEALTH_CHECK_PORT, "TCP"));
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
                .withCommand("/opt/strimzi/bin/docker/kafka_bridge_run.sh")
                .withEnv(getEnvVars())
                .withPorts(getContainerPortList())
                .withLivenessProbe(ModelUtils.createHttpProbe(livenessPath, HEALTH_CHECK_PORT_NAME, livenessProbeOptions))
                .withReadinessProbe(ModelUtils.createHttpProbe(readinessPath, HEALTH_CHECK_PORT_NAME, readinessProbeOptions))
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
        varList.add(buildEnvVar(ENV_VAR_STRIMZI_GC_LOG_ENABLED, String.valueOf(gcLoggingEnabled)));

        varList.add(buildEnvVar(ENV_VAR_KAFKA_BRIDGE_BOOTSTRAP_SERVERS, bootstrapServers));
        varList.add(buildEnvVar(ENV_VAR_KAFKA_BRIDGE_CONSUMER_CONFIG, kafkaBridgeConsumer == null ? "" : new KafkaBridgeConsumerConfiguration(kafkaBridgeConsumer.getConfig().entrySet()).getConfiguration()));
        varList.add(buildEnvVar(ENV_VAR_KAFKA_BRIDGE_PRODUCER_CONFIG, kafkaBridgeProducer == null ? "" : new KafkaBridgeProducerConfiguration(kafkaBridgeProducer.getConfig().entrySet()).getConfiguration()));

        varList.add(buildEnvVar(ENV_VAR_KAFKA_BRIDGE_HTTP_ENABLED, String.valueOf(httpEnabled)));
        varList.add(buildEnvVar(ENV_VAR_KAFKA_BRIDGE_HTTP_HOST, KafkaBridgeHttpConfig.HTTP_DEFAULT_HOST));
        varList.add(buildEnvVar(ENV_VAR_KAFKA_BRIDGE_HTTP_PORT, String.valueOf(http != null ? http.getPort() : KafkaBridgeHttpConfig.HTTP_DEFAULT_PORT)));

        varList.add(buildEnvVar(ENV_VAR_KAFKA_BRIDGE_AMQP_ENABLED, String.valueOf(amqpEnabled)));

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
     * @param kafkaBridgeHttpConfig HTTP configuration
     */
    protected void setKafkaBridgeHttpConfig(KafkaBridgeHttpConfig kafkaBridgeHttpConfig) {
        this.http = kafkaBridgeHttpConfig;
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
     * @return The pod disruption budget.
     */
    public PodDisruptionBudget generatePodDisruptionBudget() {
        return createPodDisruptionBudget();
    }

    @Override
    protected String getServiceAccountName() {
        return KafkaBridgeResources.serviceAccountName(cluster);
    }

    /**
     * Set whether the HTTP is enabled
     * @param httpEnabled HTTP enabled
     */
    protected void setHttpEnabled(boolean httpEnabled) {
        this.httpEnabled = httpEnabled;
    }

    /**
     * Set Kafka consumer's configuration
     * @param kafkaBridgeConsumer configuration
     */
    protected void setKafkaConsumerConfiguration(KafkaBridgeConsumerSpec kafkaBridgeConsumer) {
        this.kafkaBridgeConsumer = kafkaBridgeConsumer;
    }

    /**
     * Set Kafka producer's configuration
     * @param kafkaBridgeProducer configuration
     */
    protected void setKafkaProducerConfiguration(KafkaBridgeProducerSpec kafkaBridgeProducer) {
        this.kafkaBridgeProducer = kafkaBridgeProducer;
    }

    /**
     * Set Bootstrap servers for connection to cluster
     * @param bootstrapServers bootstrap servers
     */
    protected void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }
}
