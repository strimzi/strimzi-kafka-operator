/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMapVolumeSource;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.EnvVarSource;
import io.fabric8.kubernetes.api.model.EnvVarSourceBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.SecretVolumeSource;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentStrategy;
import io.fabric8.kubernetes.api.model.apps.DeploymentStrategyBuilder;
import io.fabric8.kubernetes.api.model.apps.RollingUpdateDeploymentBuilder;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudget;
import io.strimzi.api.kafka.model.CertAndKeySecretSource;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectAuthenticationScramSha512;
import io.strimzi.api.kafka.model.KafkaConnectAuthenticationTls;
import io.strimzi.api.kafka.model.KafkaConnectS2ISpec;
import io.strimzi.api.kafka.model.KafkaConnectSpec;
import io.strimzi.api.kafka.model.PasswordSecretSource;
import io.strimzi.api.kafka.model.connect.ExternalConfiguration;
import io.strimzi.api.kafka.model.connect.ExternalConfigurationEnv;
import io.strimzi.api.kafka.model.connect.ExternalConfigurationEnvVarSource;
import io.strimzi.api.kafka.model.connect.ExternalConfigurationVolumeSource;
import io.strimzi.api.kafka.model.template.KafkaConnectTemplate;
import io.strimzi.operator.common.model.Labels;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class KafkaConnectCluster extends AbstractModel {

    // Port configuration
    protected static final int REST_API_PORT = 8083;
    protected static final String REST_API_PORT_NAME = "rest-api";

    private static final String NAME_SUFFIX = "-connect";
    private static final String SERVICE_NAME_SUFFIX = NAME_SUFFIX + "-api";

    private static final String METRICS_AND_LOG_CONFIG_SUFFIX = NAME_SUFFIX + "-config";
    protected static final String TLS_CERTS_BASE_VOLUME_MOUNT = "/opt/kafka/connect-certs/";
    protected static final String PASSWORD_VOLUME_MOUNT = "/opt/kafka/connect-password/";
    protected static final String EXTERNAL_CONFIGURATION_VOLUME_MOUNT_BASE_PATH = "/opt/kafka/external-configuration/";
    protected static final String EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX = "ext-conf-";

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
    protected List<ExternalConfigurationEnv> externalEnvs = Collections.EMPTY_LIST;
    protected List<ExternalConfigurationVolumeSource> externalVolumes = Collections.EMPTY_LIST;

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
        this.ancillaryConfigName = logAndMetricsConfigName(cluster);
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

    public static KafkaConnectCluster fromCrd(KafkaConnect kafkaConnect, KafkaVersion.Lookup versions) {
        KafkaConnectCluster cluster = fromSpec(kafkaConnect.getSpec(), versions, new KafkaConnectCluster(kafkaConnect.getMetadata().getNamespace(),
                kafkaConnect.getMetadata().getName(), Labels.fromResource(kafkaConnect).withKind(kafkaConnect.getKind())));

        cluster.setOwnerReference(kafkaConnect);

        return cluster;
    }

    /**
     * Abstracts the calling of setters on a (subclass of) KafkaConnectCluster
     * from the instantiation of the (subclass of) KafkaConnectCluster,
     * thus permitting reuse of the setter-calling code for subclasses.
     */
    protected static <C extends KafkaConnectCluster> C fromSpec(KafkaConnectSpec spec,
                                                                KafkaVersion.Lookup versions,
                                                                C kafkaConnect) {
        kafkaConnect.setReplicas(spec.getReplicas() > 0 ? spec.getReplicas() : DEFAULT_REPLICAS);
        kafkaConnect.setConfiguration(new KafkaConnectConfiguration(spec.getConfig().entrySet()));

        String image = spec instanceof KafkaConnectS2ISpec ?
                versions.kafkaConnectS2iVersion(spec.getImage(), spec.getVersion())
                : versions.kafkaConnectVersion(spec.getImage(), spec.getVersion());
        if (image == null) {
            throw new InvalidResourceException("Version is not supported");
        }
        kafkaConnect.setImage(image);

        kafkaConnect.setResources(spec.getResources());
        kafkaConnect.setLogging(spec.getLogging());
        kafkaConnect.setGcLoggingEnabled(spec.getJvmOptions() == null ? true : spec.getJvmOptions().isGcLoggingEnabled());
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
        if (metrics != null) {
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

        if (spec.getTemplate() != null) {
            KafkaConnectTemplate template = spec.getTemplate();

            if (template.getDeployment() != null && template.getDeployment().getMetadata() != null)  {
                kafkaConnect.templateDeploymentLabels = template.getDeployment().getMetadata().getLabels();
                kafkaConnect.templateDeploymentAnnotations = template.getDeployment().getMetadata().getAnnotations();
            }

            ModelUtils.parsePodTemplate(kafkaConnect, template.getPod());

            if (template.getApiService() != null && template.getApiService().getMetadata() != null)  {
                kafkaConnect.templateServiceLabels = template.getApiService().getMetadata().getLabels();
                kafkaConnect.templateServiceAnnotations = template.getApiService().getMetadata().getAnnotations();
            }

            ModelUtils.parsePodDisruptionBudgetTemplate(kafkaConnect, template.getPodDisruptionBudget());
        }

        if (spec.getExternalConfiguration() != null)    {
            ExternalConfiguration externalConfiguration = spec.getExternalConfiguration();

            if (externalConfiguration.getEnv() != null && !externalConfiguration.getEnv().isEmpty())    {
                kafkaConnect.externalEnvs = externalConfiguration.getEnv();
            }

            if (externalConfiguration.getVolumes() != null && !externalConfiguration.getVolumes().isEmpty())    {
                kafkaConnect.externalVolumes = externalConfiguration.getVolumes();
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

        return createService("ClusterIP", ports, mergeAnnotations(getPrometheusAnnotations(), templateServiceAnnotations));
    }

    protected List<ContainerPort> getContainerPortList() {
        List<ContainerPort> portList = new ArrayList<>(2);
        portList.add(createContainerPort(REST_API_PORT_NAME, REST_API_PORT, "TCP"));
        if (isMetricsEnabled) {
            portList.add(createContainerPort(METRICS_PORT_NAME, METRICS_PORT, "TCP"));
        }

        return portList;
    }

    protected List<Volume> getVolumes(boolean isOpenShift) {
        List<Volume> volumeList = new ArrayList<>(1);
        volumeList.add(createConfigMapVolume(logAndMetricsConfigVolumeName, ancillaryConfigName));
        if (trustedCertificates != null && trustedCertificates.size() > 0) {
            for (CertSecretSource certSecretSource: trustedCertificates) {
                // skipping if a volume with same Secret name was already added
                if (!volumeList.stream().anyMatch(v -> v.getName().equals(certSecretSource.getSecretName()))) {
                    volumeList.add(createSecretVolume(certSecretSource.getSecretName(), certSecretSource.getSecretName(), isOpenShift));
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

        volumeList.addAll(getExternalConfigurationVolumes(isOpenShift));

        return volumeList;
    }

    private List<Volume> getExternalConfigurationVolumes(boolean isOpenShift)  {
        int mode = 0444;
        if (isOpenShift) {
            mode = 0440;
        }

        List<Volume> volumeList = new ArrayList<>(0);

        for (ExternalConfigurationVolumeSource volume : externalVolumes)    {
            String name = volume.getName();

            if (name != null) {
                if (volume.getConfigMap() != null && volume.getSecret() != null) {
                    log.warn("Volume {} with external Kafka Connect configuration has to contain exactly one volume source reference to either ConfigMap or Secret", name);
                } else  {
                    if (volume.getConfigMap() != null) {
                        ConfigMapVolumeSource source = volume.getConfigMap();
                        source.setDefaultMode(mode);

                        Volume newVol = new VolumeBuilder()
                                .withName(EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + name)
                                .withConfigMap(source)
                                .build();

                        volumeList.add(newVol);
                    } else if (volume.getSecret() != null)    {
                        SecretVolumeSource source = volume.getSecret();
                        source.setDefaultMode(mode);

                        Volume newVol = new VolumeBuilder()
                                .withName(EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + name)
                                .withSecret(source)
                                .build();

                        volumeList.add(newVol);
                    }
                }
            }
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

        volumeMountList.addAll(getExternalConfigurationVolumeMounts());

        return volumeMountList;
    }

    private List<VolumeMount> getExternalConfigurationVolumeMounts()    {
        List<VolumeMount> volumeMountList = new ArrayList<>(0);

        for (ExternalConfigurationVolumeSource volume : externalVolumes)    {
            String name = volume.getName();

            if (name != null)   {
                if (volume.getConfigMap() != null && volume.getSecret() != null) {
                    log.warn("Volume {} with external Kafka Connect configuration has to contain exactly one volume source reference to either ConfigMap or Secret", name);
                } else  if (volume.getConfigMap() != null || volume.getSecret() != null) {
                    VolumeMount volumeMount = new VolumeMountBuilder()
                            .withName(EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + name)
                            .withMountPath(EXTERNAL_CONFIGURATION_VOLUME_MOUNT_BASE_PATH + name)
                            .build();

                    volumeMountList.add(volumeMount);
                }
            }
        }

        return volumeMountList;
    }

    public Deployment generateDeployment(Map<String, String> annotations, boolean isOpenShift, String imagePullPolicy) {
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
                getVolumes(isOpenShift));
    }

    @Override
    protected List<Container> getContainers(String imagePullPolicy) {

        List<Container> containers = new ArrayList<>();

        Container container = new ContainerBuilder()
                .withName(name)
                .withImage(getImage())
                .withEnv(getEnvVars())
                .withPorts(getContainerPortList())
                .withLivenessProbe(createHttpProbe(livenessPath, REST_API_PORT_NAME, livenessInitialDelay, livenessTimeout))
                .withReadinessProbe(createHttpProbe(readinessPath, REST_API_PORT_NAME, readinessInitialDelay, readinessTimeout))
                .withVolumeMounts(getVolumeMounts())
                .withResources(ModelUtils.resources(getResources()))
                .withImagePullPolicy(determineImagePullPolicy(imagePullPolicy, getImage()))
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
        varList.add(buildEnvVar(ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED, String.valueOf(gcLoggingEnabled)));

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

        varList.addAll(getExternalConfigurationEnvVars());

        return varList;
    }

    private List<EnvVar> getExternalConfigurationEnvVars()   {
        List<EnvVar> varList = new ArrayList<>();

        for (ExternalConfigurationEnv var : externalEnvs)    {
            String name = var.getName();

            if (name != null && !name.startsWith("KAFKA_") && !name.startsWith("STRIMZI_")) {
                ExternalConfigurationEnvVarSource valueFrom = var.getValueFrom();

                if (valueFrom != null)  {
                    if (valueFrom.getConfigMapKeyRef() != null && valueFrom.getSecretKeyRef() != null) {
                        log.warn("Environment variable {} with external Kafka Connect configuration has to contain exactly one reference to either ConfigMap or Secret", name);
                    } else {
                        if (valueFrom.getConfigMapKeyRef() != null) {
                            EnvVarSource envVarSource = new EnvVarSourceBuilder()
                                    .withConfigMapKeyRef(var.getValueFrom().getConfigMapKeyRef())
                                    .build();

                            varList.add(new EnvVarBuilder().withName(name).withValueFrom(envVarSource).build());
                        } else if (valueFrom.getSecretKeyRef() != null)    {
                            EnvVarSource envVarSource = new EnvVarSourceBuilder()
                                    .withSecretKeyRef(var.getValueFrom().getSecretKeyRef())
                                    .build();

                            varList.add(new EnvVarBuilder().withName(name).withValueFrom(envVarSource).build());
                        }
                    }
                }
            } else {
                log.warn("Name of an environment variable with external Kafka Connect configuration cannot start with `KAFKA_` or `STRIMZI`.");
            }
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

    /**
     * Generates the PodDisruptionBudget
     *
     * @return
     */
    public PodDisruptionBudget generatePodDisruptionBudget() {
        return createPodDisruptionBudget();
    }
}
