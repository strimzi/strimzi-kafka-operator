/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.AffinityBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSource;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.EnvVarSource;
import io.fabric8.kubernetes.api.model.EnvVarSourceBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.SecretVolumeSource;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyIngressRule;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyIngressRuleBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPeer;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPeerBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPort;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleRef;
import io.fabric8.kubernetes.api.model.rbac.RoleRefBuilder;
import io.fabric8.kubernetes.api.model.rbac.Subject;
import io.fabric8.kubernetes.api.model.rbac.SubjectBuilder;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaConnectS2ISpec;
import io.strimzi.api.kafka.model.KafkaConnectSpec;
import io.strimzi.api.kafka.model.KafkaConnectTls;
import io.strimzi.api.kafka.model.Probe;
import io.strimzi.api.kafka.model.ProbeBuilder;
import io.strimzi.api.kafka.model.Rack;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthentication;
import io.strimzi.api.kafka.model.connect.ExternalConfiguration;
import io.strimzi.api.kafka.model.connect.ExternalConfigurationEnv;
import io.strimzi.api.kafka.model.connect.ExternalConfigurationEnvVarSource;
import io.strimzi.api.kafka.model.connect.ExternalConfigurationVolumeSource;
import io.strimzi.api.kafka.model.template.KafkaConnectTemplate;
import io.strimzi.api.kafka.model.tracing.Tracing;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;

import java.nio.charset.StandardCharsets;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Base64;


@SuppressWarnings({"checkstyle:ClassFanOutComplexity"})
public class KafkaConnectCluster extends AbstractModel {
    protected static final String APPLICATION_NAME = "kafka-connect";

    // Port configuration
    public static final int REST_API_PORT = 8083;
    protected static final String REST_API_PORT_NAME = "rest-api";

    protected static final String TLS_CERTS_BASE_VOLUME_MOUNT = "/opt/kafka/connect-certs/";
    protected static final String PASSWORD_VOLUME_MOUNT = "/opt/kafka/connect-password/";
    protected static final String EXTERNAL_CONFIGURATION_VOLUME_MOUNT_BASE_PATH = "/opt/kafka/external-configuration/";
    protected static final String EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX = "ext-conf-";

    // Configuration defaults
    protected static final int DEFAULT_REPLICAS = 3;
    static final int DEFAULT_HEALTHCHECK_DELAY = 60;
    static final int DEFAULT_HEALTHCHECK_TIMEOUT = 5;
    public static final Probe DEFAULT_HEALTHCHECK_OPTIONS = new ProbeBuilder().withInitialDelaySeconds(DEFAULT_HEALTHCHECK_TIMEOUT)
            .withInitialDelaySeconds(DEFAULT_HEALTHCHECK_DELAY).build();
    protected static final boolean DEFAULT_KAFKA_CONNECT_METRICS_ENABLED = false;

    private static final String NAME_SUFFIX = "-" + APPLICATION_NAME;
    protected static final String KAFKA_CONNECT_JMX_SECRET_SUFFIX = NAME_SUFFIX + "-jmx";
    protected static final String SECRET_JMX_USERNAME_KEY = "jmx-username";
    protected static final String SECRET_JMX_PASSWORD_KEY = "jmx-password";


    // Kafka Connect configuration keys (EnvVariables)
    protected static final String ENV_VAR_PREFIX = "KAFKA_CONNECT_";
    protected static final String ENV_VAR_KAFKA_CONNECT_CONFIGURATION = "KAFKA_CONNECT_CONFIGURATION";
    protected static final String ENV_VAR_KAFKA_CONNECT_METRICS_ENABLED = "KAFKA_CONNECT_METRICS_ENABLED";
    protected static final String ENV_VAR_KAFKA_CONNECT_BOOTSTRAP_SERVERS = "KAFKA_CONNECT_BOOTSTRAP_SERVERS";
    protected static final String ENV_VAR_KAFKA_CONNECT_TLS = "KAFKA_CONNECT_TLS";
    protected static final String ENV_VAR_KAFKA_CONNECT_TRUSTED_CERTS = "KAFKA_CONNECT_TRUSTED_CERTS";
    protected static final String ENV_VAR_KAFKA_CONNECT_TLS_AUTH_CERT = "KAFKA_CONNECT_TLS_AUTH_CERT";
    protected static final String ENV_VAR_KAFKA_CONNECT_TLS_AUTH_KEY = "KAFKA_CONNECT_TLS_AUTH_KEY";
    protected static final String ENV_VAR_KAFKA_CONNECT_SASL_PASSWORD_FILE = "KAFKA_CONNECT_SASL_PASSWORD_FILE";
    protected static final String ENV_VAR_KAFKA_CONNECT_SASL_USERNAME = "KAFKA_CONNECT_SASL_USERNAME";
    protected static final String ENV_VAR_KAFKA_CONNECT_SASL_MECHANISM = "KAFKA_CONNECT_SASL_MECHANISM";
    protected static final String ENV_VAR_KAFKA_CONNECT_OAUTH_CONFIG = "KAFKA_CONNECT_OAUTH_CONFIG";
    protected static final String ENV_VAR_KAFKA_CONNECT_OAUTH_CLIENT_SECRET = "KAFKA_CONNECT_OAUTH_CLIENT_SECRET";
    protected static final String ENV_VAR_KAFKA_CONNECT_OAUTH_ACCESS_TOKEN = "KAFKA_CONNECT_OAUTH_ACCESS_TOKEN";
    protected static final String ENV_VAR_KAFKA_CONNECT_OAUTH_REFRESH_TOKEN = "KAFKA_CONNECT_OAUTH_REFRESH_TOKEN";
    protected static final String OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT = "/opt/kafka/oauth-certs/";
    protected static final String ENV_VAR_STRIMZI_TRACING = "STRIMZI_TRACING";
    protected static final String ENV_VAR_KAFKA_CONNECT_JMX_ENABLED = "KAFKA_CONNECT_JMX_ENABLED";
    protected static final String ENV_VAR_KAFKA_CONNECT_JMX_USERNAME = "KAFKA_CONNECT_JMX_USERNAME";
    protected static final String ENV_VAR_KAFKA_CONNECT_JMX_PASSWORD = "KAFKA_CONNECT_JMX_PASSWORD";

    private Rack rack;
    private String initImage;

    protected String bootstrapServers;
    protected List<ExternalConfigurationEnv> externalEnvs = Collections.emptyList();
    protected List<ExternalConfigurationVolumeSource> externalVolumes = Collections.emptyList();
    protected List<ContainerEnvVar> templateContainerEnvVars;
    protected List<ContainerEnvVar> templateInitContainerEnvVars;
    protected SecurityContext templateContainerSecurityContext;
    protected SecurityContext templateInitContainerSecurityContext;
    protected Tracing tracing;

    private KafkaConnectTls tls;
    private KafkaClientAuthentication authentication;

    private boolean isJmxEnabled;
    private boolean isJmxAuthenticated;

    /**
     * Constructor
     *
     * @param resource Kubernetes resource with metadata containing the namespace and cluster name
     */
    protected KafkaConnectCluster(HasMetadata resource) {
        this(resource, APPLICATION_NAME);
    }

    /**
     * Constructor
     *
     * @param resource Kubernetes resource with metadata containing the namespace and cluster name
     * @param applicationName configurable allow other classes to extend this class
     */
    protected KafkaConnectCluster(HasMetadata resource, String applicationName) {
        super(resource, applicationName);
        this.name = KafkaConnectResources.deploymentName(cluster);
        this.serviceName = KafkaConnectResources.serviceName(cluster);
        this.ancillaryConfigMapName = KafkaConnectResources.metricsAndLogConfigMapName(cluster);
        this.replicas = DEFAULT_REPLICAS;
        this.readinessPath = "/";
        this.readinessProbeOptions = DEFAULT_HEALTHCHECK_OPTIONS;
        this.livenessPath = "/";
        this.livenessProbeOptions = DEFAULT_HEALTHCHECK_OPTIONS;
        this.isMetricsEnabled = DEFAULT_KAFKA_CONNECT_METRICS_ENABLED;

        this.mountPath = "/var/lib/kafka";
        this.logAndMetricsConfigVolumeName = "kafka-metrics-and-logging";
        this.logAndMetricsConfigMountPath = "/opt/kafka/custom-config/";
    }

    public static KafkaConnectCluster fromCrd(KafkaConnect kafkaConnect, KafkaVersion.Lookup versions) {

        KafkaConnectCluster cluster = fromSpec(kafkaConnect.getSpec(), versions,
                new KafkaConnectCluster(kafkaConnect));

        cluster.setOwnerReference(kafkaConnect);

        return cluster;
    }

    /**
     * Abstracts the calling of setters on a (subclass of) KafkaConnectCluster
     * from the instantiation of the (subclass of) KafkaConnectCluster,
     * thus permitting reuse of the setter-calling code for subclasses.
     */
    @SuppressWarnings("deprecation")
    protected static <C extends KafkaConnectCluster> C fromSpec(KafkaConnectSpec spec,
                                                                KafkaVersion.Lookup versions,
                                                                C kafkaConnect) {
        kafkaConnect.setReplicas(spec.getReplicas() != null && spec.getReplicas() >= 0 ? spec.getReplicas() : DEFAULT_REPLICAS);
        kafkaConnect.tracing = spec.getTracing();

        AbstractConfiguration config = kafkaConnect.getConfiguration();
        if (config == null) {
            config = new KafkaConnectConfiguration(spec.getConfig().entrySet());
            kafkaConnect.setConfiguration(config);
        }
        if (kafkaConnect.tracing != null)   {
            config.setConfigOption("consumer.interceptor.classes", "io.opentracing.contrib.kafka.TracingConsumerInterceptor");
            config.setConfigOption("producer.interceptor.classes", "io.opentracing.contrib.kafka.TracingProducerInterceptor");
        }

        if (kafkaConnect.getImage() == null) {
            String image = spec instanceof KafkaConnectS2ISpec ?
                    versions.kafkaConnectS2IVersion(spec.getImage(), spec.getVersion())
                    : versions.kafkaConnectVersion(spec.getImage(), spec.getVersion());
            kafkaConnect.setImage(image);
        }

        kafkaConnect.setResources(spec.getResources());
        kafkaConnect.setLogging(spec.getLogging());
        kafkaConnect.setGcLoggingEnabled(spec.getJvmOptions() == null ? DEFAULT_JVM_GC_LOGGING_ENABLED : spec.getJvmOptions().isGcLoggingEnabled());
        if (spec.getJvmOptions() != null) {
            kafkaConnect.setJavaSystemProperties(spec.getJvmOptions().getJavaSystemProperties());
        }

        kafkaConnect.setJvmOptions(spec.getJvmOptions());

        if (spec.getJmxOptions() != null) {
            kafkaConnect.setJmxEnabled(Boolean.TRUE);
            AuthenticationUtils.configureKafkaConnectJmxOptions(spec.getJmxOptions().getAuthentication(), kafkaConnect);
        }

        if (spec.getReadinessProbe() != null) {
            kafkaConnect.setReadinessProbe(spec.getReadinessProbe());
        }
        if (spec.getLivenessProbe() != null) {
            kafkaConnect.setLivenessProbe(spec.getLivenessProbe());
        }

        kafkaConnect.setRack(spec.getRack());

        String initImage = spec.getClientRackInitImage();
        if (initImage == null) {
            initImage = System.getenv().getOrDefault(ClusterOperatorConfig.STRIMZI_DEFAULT_KAFKA_INIT_IMAGE, "quay.io/strimzi/operator:latest");
        }
        kafkaConnect.setInitImage(initImage);

        // Parse different types of metrics configurations
        ModelUtils.parseMetrics(kafkaConnect, spec);

        kafkaConnect.setBootstrapServers(spec.getBootstrapServers());

        kafkaConnect.setTls(spec.getTls());
        AuthenticationUtils.validateClientAuthentication(spec.getAuthentication(), spec.getTls() != null);
        kafkaConnect.setAuthentication(spec.getAuthentication());

        if (spec.getTemplate() != null) {
            KafkaConnectTemplate template = spec.getTemplate();

            ModelUtils.parseDeploymentTemplate(kafkaConnect, template.getDeployment());
            ModelUtils.parsePodTemplate(kafkaConnect, template.getPod());

            if (template.getApiService() != null && template.getApiService().getMetadata() != null)  {
                kafkaConnect.templateServiceLabels = template.getApiService().getMetadata().getLabels();
                kafkaConnect.templateServiceAnnotations = template.getApiService().getMetadata().getAnnotations();
            }

            if (template.getClusterRoleBinding() != null && template.getClusterRoleBinding().getMetadata() != null) {
                kafkaConnect.templateClusterRoleBindingLabels = template.getClusterRoleBinding().getMetadata().getLabels();
                kafkaConnect.templateClusterRoleBindingAnnotations = template.getClusterRoleBinding().getMetadata().getAnnotations();
            }

            if (template.getConnectContainer() != null && template.getConnectContainer().getEnv() != null) {
                kafkaConnect.templateContainerEnvVars = template.getConnectContainer().getEnv();
            }

            if (template.getInitContainer() != null && template.getInitContainer().getEnv() != null) {
                kafkaConnect.templateInitContainerEnvVars = template.getInitContainer().getEnv();
            }

            if (template.getConnectContainer() != null && template.getConnectContainer().getSecurityContext() != null) {
                kafkaConnect.templateContainerSecurityContext = template.getConnectContainer().getSecurityContext();
            }

            if (template.getInitContainer() != null && template.getInitContainer().getSecurityContext() != null) {
                kafkaConnect.templateInitContainerSecurityContext = template.getInitContainer().getSecurityContext();
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
        List<ServicePort> ports = new ArrayList<>(1);
        ports.add(createServicePort(REST_API_PORT_NAME, REST_API_PORT, REST_API_PORT, "TCP"));

        if (isJmxEnabled()) {
            ports.add(createServicePort(JMX_PORT_NAME, JMX_PORT, JMX_PORT, "TCP"));
        }

        return createService("ClusterIP", ports, Util.mergeLabelsOrAnnotations(templateServiceAnnotations));
    }

    protected List<ContainerPort> getContainerPortList() {
        List<ContainerPort> portList = new ArrayList<>(2);
        portList.add(createContainerPort(REST_API_PORT_NAME, REST_API_PORT, "TCP"));
        if (isMetricsEnabled) {
            portList.add(createContainerPort(METRICS_PORT_NAME, METRICS_PORT, "TCP"));
        }

        if (isJmxEnabled()) {
            portList.add(createContainerPort(JMX_PORT_NAME, JMX_PORT, "TCP"));
        }

        return portList;
    }

    protected List<Volume> getVolumes(boolean isOpenShift, boolean isS2I) {
        List<Volume> volumeList = new ArrayList<>(2);

        if (!isS2I) {
            volumeList.add(createTempDirVolume());
        }

        volumeList.add(VolumeUtils.createConfigMapVolume(logAndMetricsConfigVolumeName, ancillaryConfigMapName));

        if (rack != null) {
            volumeList.add(VolumeUtils.createEmptyDirVolume(INIT_VOLUME_NAME, null));
        }

        if (tls != null) {
            List<CertSecretSource> trustedCertificates = tls.getTrustedCertificates();

            if (trustedCertificates != null && trustedCertificates.size() > 0) {
                for (CertSecretSource certSecretSource : trustedCertificates) {
                    // skipping if a volume with same Secret name was already added
                    if (!volumeList.stream().anyMatch(v -> v.getName().equals(certSecretSource.getSecretName()))) {
                        volumeList.add(VolumeUtils.createSecretVolume(certSecretSource.getSecretName(), certSecretSource.getSecretName(), isOpenShift));
                    }
                }
            }
        }

        AuthenticationUtils.configureClientAuthenticationVolumes(authentication, volumeList, "oauth-certs", isOpenShift);

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
                                .withName(VolumeUtils.getValidVolumeName(EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + name))
                                .withConfigMap(source)
                                .build();

                        volumeList.add(newVol);
                    } else if (volume.getSecret() != null)    {
                        SecretVolumeSource source = volume.getSecret();
                        source.setDefaultMode(mode);

                        Volume newVol = new VolumeBuilder()
                                .withName(VolumeUtils.getValidVolumeName(EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + name))
                                .withSecret(source)
                                .build();

                        volumeList.add(newVol);
                    }
                }
            }
        }

        return volumeList;
    }

    protected List<VolumeMount> getVolumeMounts(boolean isS2I) {
        List<VolumeMount> volumeMountList = new ArrayList<>(2);

        if (!isS2I) {
            volumeMountList.add(createTempDirVolumeMount());
        }

        volumeMountList.add(VolumeUtils.createVolumeMount(logAndMetricsConfigVolumeName, logAndMetricsConfigMountPath));

        if (rack != null) {
            volumeMountList.add(VolumeUtils.createVolumeMount(INIT_VOLUME_NAME, INIT_VOLUME_MOUNT));
        }

        if (tls != null) {
            List<CertSecretSource> trustedCertificates = tls.getTrustedCertificates();

            if (trustedCertificates != null && trustedCertificates.size() > 0) {
                for (CertSecretSource certSecretSource : trustedCertificates) {
                    // skipping if a volume mount with same Secret name was already added
                    if (!volumeMountList.stream().anyMatch(vm -> vm.getName().equals(certSecretSource.getSecretName()))) {
                        volumeMountList.add(VolumeUtils.createVolumeMount(certSecretSource.getSecretName(),
                                TLS_CERTS_BASE_VOLUME_MOUNT + certSecretSource.getSecretName()));
                    }
                }
            }
        }

        AuthenticationUtils.configureClientAuthenticationVolumeMounts(authentication, volumeMountList, TLS_CERTS_BASE_VOLUME_MOUNT, PASSWORD_VOLUME_MOUNT, OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT, "oauth-certs");

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
                            .withName(VolumeUtils.getValidVolumeName(EXTERNAL_CONFIGURATION_VOLUME_NAME_PREFIX + name))
                            .withMountPath(EXTERNAL_CONFIGURATION_VOLUME_MOUNT_BASE_PATH + name)
                            .build();

                    volumeMountList.add(volumeMount);
                }
            }
        }

        return volumeMountList;
    }

    /**
     * Returns a combined affinity: Adding the affinity needed for the "kafka-rack" to the {@link #getUserAffinity()}.
     */
    @Override
    protected Affinity getMergedAffinity() {
        Affinity userAffinity = getUserAffinity();
        AffinityBuilder builder = new AffinityBuilder(userAffinity == null ? new Affinity() : userAffinity);
        if (rack != null) {
            builder = ModelUtils.populateAffinityBuilderWithRackLabelSelector(builder, userAffinity, rack.getTopologyKey());
        }
        return builder.build();
    }

    public Deployment generateDeployment(Map<String, String> annotations, boolean isOpenShift, ImagePullPolicy imagePullPolicy, List<LocalObjectReference> imagePullSecrets) {
        return createDeployment(
                getDeploymentStrategy(),
                Collections.emptyMap(),
                annotations,
                getMergedAffinity(),
                getInitContainers(imagePullPolicy),
                getContainers(imagePullPolicy),
                getVolumes(isOpenShift, false),
                imagePullSecrets);
    }

    @Override
    protected List<Container> getContainers(ImagePullPolicy imagePullPolicy) {

        List<Container> containers = new ArrayList<>(1);

        Container container = new ContainerBuilder()
                .withName(name)
                .withImage(getImage())
                .withCommand(getCommand())
                .withEnv(getEnvVars())
                .withPorts(getContainerPortList())
                .withLivenessProbe(ProbeGenerator.httpProbe(livenessProbeOptions, livenessPath, REST_API_PORT_NAME))
                .withReadinessProbe(ProbeGenerator.httpProbe(readinessProbeOptions, readinessPath, REST_API_PORT_NAME))
                .withVolumeMounts(getVolumeMounts(false))
                .withResources(getResources())
                .withImagePullPolicy(determineImagePullPolicy(imagePullPolicy, getImage()))
                .withSecurityContext(templateContainerSecurityContext)
                .build();

        containers.add(container);

        return containers;
    }

    protected List<EnvVar> getInitContainerEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(buildEnvVarFromFieldRef(ENV_VAR_KAFKA_INIT_NODE_NAME, "spec.nodeName"));

        varList.add(buildEnvVar(ENV_VAR_KAFKA_INIT_RACK_TOPOLOGY_KEY, rack.getTopologyKey()));

        // Add shared environment variables used for all containers
        varList.addAll(getRequiredEnvVars());

        addContainerEnvsToExistingEnvs(varList, templateInitContainerEnvVars);

        return varList;
    }

    @Override
    protected List<Container> getInitContainers(ImagePullPolicy imagePullPolicy) {
        List<Container> initContainers = new ArrayList<>(1);

        if (rack != null) {
            Container initContainer = new ContainerBuilder()
                    .withName(INIT_NAME)
                    .withImage(initImage)
                    .withArgs("/opt/strimzi/bin/kafka_init_run.sh")
                    .withResources(getInitContainerResourceResourceRequirements())
                    .withEnv(getInitContainerEnvVars())
                    .withVolumeMounts(VolumeUtils.createVolumeMount(INIT_VOLUME_NAME, INIT_VOLUME_MOUNT))
                    .withImagePullPolicy(determineImagePullPolicy(imagePullPolicy, initImage))
                    .withSecurityContext(templateInitContainerSecurityContext)
                    .build();

            initContainers.add(initContainer);
        }

        return initContainers;
    }

    private ResourceRequirements getInitContainerResourceResourceRequirements() {
        return new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100m"))
                .addToRequests("memory", new Quantity("128Mi"))
                .addToLimits("cpu", new Quantity("1"))
                .addToLimits("memory", new Quantity("256Mi"))
                .build();
    }

    protected String getCommand() {
        return "/opt/kafka/kafka_connect_run.sh";
    }

    @Override
    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(buildEnvVar(ENV_VAR_KAFKA_CONNECT_CONFIGURATION, configuration.getConfiguration()));
        varList.add(buildEnvVar(ENV_VAR_KAFKA_CONNECT_METRICS_ENABLED, String.valueOf(isMetricsEnabled)));
        varList.add(buildEnvVar(ENV_VAR_KAFKA_CONNECT_BOOTSTRAP_SERVERS, bootstrapServers));
        varList.add(buildEnvVar(ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED, String.valueOf(gcLoggingEnabled)));
        if (javaSystemProperties != null) {
            varList.add(buildEnvVar(ENV_VAR_STRIMZI_JAVA_SYSTEM_PROPERTIES, ModelUtils.getJavaSystemPropertiesToString(javaSystemProperties)));
        }

        heapOptions(varList, 1.0, 0L);
        jvmPerformanceOptions(varList);

        if (tls != null) {
            populateTLSEnvVars(varList);
        }

        AuthenticationUtils.configureClientAuthenticationEnvVars(authentication, varList, name -> ENV_VAR_PREFIX + name);

        if (tracing != null) {
            varList.add(buildEnvVar(ENV_VAR_STRIMZI_TRACING, tracing.getType()));
        }

        if (isJmxEnabled()) {
            varList.add(buildEnvVar(ENV_VAR_KAFKA_CONNECT_JMX_ENABLED, "true"));
            if (isJmxAuthenticated) {
                varList.add(buildEnvVarFromSecret(ENV_VAR_KAFKA_CONNECT_JMX_USERNAME, jmxSecretName(cluster), SECRET_JMX_USERNAME_KEY));
                varList.add(buildEnvVarFromSecret(ENV_VAR_KAFKA_CONNECT_JMX_PASSWORD, jmxSecretName(cluster), SECRET_JMX_PASSWORD_KEY));
            }
        }

        // Add shared environment variables used for all containers
        varList.addAll(getRequiredEnvVars());

        varList.addAll(getExternalConfigurationEnvVars());

        addContainerEnvsToExistingEnvs(varList, templateContainerEnvVars);

        return varList;
    }

    private void populateTLSEnvVars(final List<EnvVar> varList) {
        varList.add(buildEnvVar(ENV_VAR_KAFKA_CONNECT_TLS, "true"));

        List<CertSecretSource> trustedCertificates = tls.getTrustedCertificates();

        if (trustedCertificates != null && trustedCertificates.size() > 0) {
            StringBuilder sb = new StringBuilder();
            boolean separator = false;
            for (CertSecretSource certSecretSource : trustedCertificates) {
                if (separator) {
                    sb.append(";");
                }
                sb.append(certSecretSource.getSecretName()).append("/").append(certSecretSource.getCertificate());
                separator = true;
            }
            varList.add(buildEnvVar(ENV_VAR_KAFKA_CONNECT_TRUSTED_CERTS, sb.toString()));
        }
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

    protected void setRack(Rack rack) {
        this.rack = rack;
    }

    protected void setInitImage(String initImage) {
        this.initImage = initImage;
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
     * Set the tls configuration with the certificate to trust
     *
     * @param tls trusted certificates list
     */
    protected void setTls(KafkaConnectTls tls) {
        this.tls = tls;
    }

    /**
     * Sets the configured authentication
     *
     * @param authetication Authentication configuration
     */
    protected void setAuthentication(KafkaClientAuthentication authetication) {
        this.authentication = authetication;
    }

    /**
     * Generates the PodDisruptionBudget
     *
     * @return The PodDisruptionBudget.
     */
    public PodDisruptionBudget generatePodDisruptionBudget() {
        return createPodDisruptionBudget();
    }

    @Override
    public String getServiceAccountName() {
        return KafkaConnectResources.serviceAccountName(cluster);
    }

    /**
     * @return Return if the jmx has been enabled
     */
    public boolean isJmxEnabled() {
        return isJmxEnabled;
    }

    /**
     * Sets the object with jmx options enabled
     *
     * @param jmxEnabled if jmx is enabled
     */
    public void setJmxEnabled(boolean jmxEnabled) {
        isJmxEnabled = jmxEnabled;
    }

    public boolean isJmxAuthenticated() {
        return isJmxAuthenticated;
    }

    public void setJmxAuthenticated(boolean jmxAuthenticated) {
        isJmxAuthenticated = jmxAuthenticated;
    }

    /**
     * @param cluster The name of the cluster.
     * @return The name of the jmx Secret.
     */
    public static String jmxSecretName(String cluster) {
        return cluster + KafkaConnectCluster.KAFKA_CONNECT_JMX_SECRET_SUFFIX;
    }

    /**
     * Generate the Secret containing the username and password to secure the jmx port on the kafka connect workers
     *
     * @return The generated Secret
     */
    public Secret generateJmxSecret() {
        Map<String, String> data = new HashMap<>(2);
        String[] keys = {SECRET_JMX_USERNAME_KEY, SECRET_JMX_PASSWORD_KEY};
        PasswordGenerator passwordGenerator = new PasswordGenerator(16);
        for (String key : keys) {
            data.put(key, Base64.getEncoder().encodeToString(passwordGenerator.generate().getBytes(StandardCharsets.US_ASCII)));
        }

        return createSecret(KafkaConnectCluster.jmxSecretName(cluster), data);
    }



    /**
     * Generates the NetworkPolicies relevant for Kafka Connect nodes
     *
     * @param connectorOperatorEnabled Whether the ConnectorOperator is enabled or not
     * @param operatorNamespace                             Namespace where the Strimzi Cluster Operator runs. Null if not configured.
     * @param operatorNamespaceLabels                       Labels of the namespace where the Strimzi Cluster Operator runs. Null if not configured.
     *
     * @return The network policy.
     */
    public NetworkPolicy generateNetworkPolicy(boolean connectorOperatorEnabled,
                                               String operatorNamespace, Labels operatorNamespaceLabels) {
        if (connectorOperatorEnabled) {
            List<NetworkPolicyIngressRule> rules = new ArrayList<>(2);

            // Give CO access to the REST API
            NetworkPolicyIngressRule restApiRule = new NetworkPolicyIngressRuleBuilder()
                    .addNewPort()
                    .withNewPort(REST_API_PORT)
                    .endPort()
                    .build();

            // OCP 3.11 doesn't support network policies with the `from` section containing a namespace.
            // Since the CO can run in a different namespace, we have to leave it wide open on OCP 3.11
            // Therefore these rules are set only when using something else than OCP 3.11 and leaving
            // the `from` section empty on 3.11
            List<NetworkPolicyPeer> peers = new ArrayList<>(2);

            // Other connect pods in the same cluster need to talk with each other over the REST API
            NetworkPolicyPeer connectPeer = new NetworkPolicyPeerBuilder()
                    .withNewPodSelector()
                    .addToMatchLabels(getSelectorLabels().toMap())
                    .endPodSelector()
                    .build();
            peers.add(connectPeer);

            // CO needs to talk with the Connect pods to manage connectors
            NetworkPolicyPeer clusterOperatorPeer = new NetworkPolicyPeerBuilder()
                    .withNewPodSelector()
                    .addToMatchLabels(Labels.STRIMZI_KIND_LABEL, "cluster-operator")
                    .endPodSelector()
                    .build();
            ModelUtils.setClusterOperatorNetworkPolicyNamespaceSelector(clusterOperatorPeer, namespace, operatorNamespace, operatorNamespaceLabels);

            peers.add(clusterOperatorPeer);

            restApiRule.setFrom(peers);

            rules.add(restApiRule);

            // If metrics are enabled, we have to open them as well. Otherwise they will be blocked.
            if (isMetricsEnabled) {
                NetworkPolicyPort metricsPort = new NetworkPolicyPort();
                metricsPort.setPort(new IntOrString(METRICS_PORT));

                NetworkPolicyIngressRule metricsRule = new NetworkPolicyIngressRuleBuilder()
                        .withPorts(metricsPort)
                        .withFrom()
                        .build();

                rules.add(metricsRule);
            }

            NetworkPolicy networkPolicy = new NetworkPolicyBuilder()
                    .withNewMetadata()
                        .withName(name)
                        .withNamespace(namespace)
                        .withLabels(labels.toMap())
                        .withOwnerReferences(createOwnerReference())
                    .endMetadata()
                    .withNewSpec()
                        .withNewPodSelector()
                            .addToMatchLabels(getSelectorLabels().toMap())
                        .endPodSelector()
                        .withIngress(rules)
                    .endSpec()
                    .build();

            log.trace("Created network policy {}", networkPolicy);
            return networkPolicy;
        } else {
            return null;
        }
    }

    /**
     * Returns the Tracing object with tracing configuration or null if tracing was not enabled.
     *
     * @return  Tracing object with tracing configuration
     */
    public Tracing getTracing() {
        return tracing;
    }

    /**
     * Creates the ClusterRoleBinding which is used to bind the Kafka Connect SA to the ClusterRole
     * which permissions the Kafka init container to access K8S nodes (necessary for rack-awareness).
     *
     * @return The cluster role binding.
     */
    public ClusterRoleBinding generateClusterRoleBinding() {
        if (rack == null) {
            return null;
        }

        Subject subject = new SubjectBuilder()
                .withKind("ServiceAccount")
                .withName(getServiceAccountName())
                .withNamespace(namespace)
                .build();

        RoleRef roleRef = new RoleRefBuilder()
                .withName("strimzi-kafka-client")
                .withApiGroup("rbac.authorization.k8s.io")
                .withKind("ClusterRole")
                .build();

        return getClusterRoleBinding(KafkaConnectResources.initContainerClusterRoleBindingName(cluster, namespace), subject, roleRef);
    }

    @Override
    protected boolean shouldPatchLoggerAppender() {
        return true;
    }
}
