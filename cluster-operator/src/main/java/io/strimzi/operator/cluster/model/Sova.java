/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyIngressRule;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.strimzi.api.kafka.model.common.StrimziProbe;
import io.strimzi.api.kafka.model.common.StrimziProbeBuilder;
import io.strimzi.api.kafka.model.common.template.DeploymentTemplate;
import io.strimzi.api.kafka.model.common.template.PodDisruptionBudgetTemplate;
import io.strimzi.api.kafka.model.common.template.PodTemplate;
import io.strimzi.api.kafka.model.common.template.StrimziDeploymentStrategy;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaClusterSpec;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.sova.SovaResources;
import io.strimzi.api.kafka.model.kafka.sova.SovaSpec;
import io.strimzi.api.kafka.model.kafka.sova.SovaTemplate;
import io.strimzi.certs.CertAndKey;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.KafkaClusterSecurityContext;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.MtlsAuthenticationConfiguration;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.ServiceAccountAuthenticationConfiguration;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.TlsEncryptionConfiguration;
import io.strimzi.operator.cluster.model.logging.LoggingModel;
import io.strimzi.operator.cluster.model.logging.SupportsLogging;
import io.strimzi.operator.cluster.model.metrics.MetricsModel;
import io.strimzi.operator.cluster.model.securityprofiles.ContainerSecurityProviderContextImpl;
import io.strimzi.operator.cluster.model.securityprofiles.PodSecurityProviderContextImpl;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.ca.Ca;
import io.strimzi.plugin.security.profiles.PodSecurityProviderContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;

/**
 * Sova model
 */
public class Sova extends AbstractModel implements SupportsLogging {
    /**
     * Type of the component which this model class represents. It is used for labeling and naming purposes.
     */
    public static final String COMPONENT_TYPE = "sova";

    // Ports
    protected static final String MANAGEMENT_PORT_NAME = "management";
    protected static final int MANAGEMENT_PORT = 8080;

    // Logging / metrics config volume
    protected static final String LOG_AND_METRICS_CONFIG_VOLUME_NAME = "sova-metrics-and-logging";
    protected static final String LOG_AND_METRICS_CONFIG_VOLUME_MOUNT = "/opt/sova/custom-config/";

    // Custom pod labels env var
    protected static final String CO_ENV_VAR_CUSTOM_SOVA_POD_LABELS = "STRIMZI_CUSTOM_SOVA_LABELS";

    // Health check defaults
    /*test*/ static final int DEFAULT_HEALTHCHECK_DELAY = 15;
    /*test*/ static final int DEFAULT_HEALTHCHECK_TIMEOUT = 15;
    /*test*/ static final int DEFAULT_HEALTHCHECK_PERIOD = 30;
    private static final StrimziProbe DEFAULT_HEALTHCHECK_OPTIONS = new StrimziProbeBuilder()
            .withTimeoutSeconds(DEFAULT_HEALTHCHECK_TIMEOUT)
            .withInitialDelaySeconds(DEFAULT_HEALTHCHECK_DELAY)
            .withPeriodSeconds(DEFAULT_HEALTHCHECK_PERIOD)
            .build();

    // Environment variable names
    protected static final String ENV_VAR_GROUP_REGEX = "STRIMZI_GROUP_REGEX";
    protected static final String ENV_VAR_GROUP_EXCLUDE_REGEX = "STRIMZI_GROUP_EXCLUDE_REGEX";
    protected static final String ENV_VAR_TOPIC_REGEX = "STRIMZI_TOPIC_REGEX";
    protected static final String ENV_VAR_TOPIC_EXCLUDE_REGEX = "STRIMZI_TOPIC_EXCLUDE_REGEX";
    protected static final String ENV_VAR_METRICS_PORT = "STRIMZI_METRICS_PORT";
    protected static final String ENV_VAR_MANAGEMENT_PORT = "STRIMZI_MANAGEMENT_PORT";
    protected static final String ENV_VAR_KAFKA_ADMIN_CLIENT_CONFIGURATION = "STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION";

    // Placeholder used to load the values from Kubernetes Secrets using the Kubernetes Secret config provider
    private static final String PLACEHOLDER_SECRET_TEMPLATE_KUBE_CONFIG_PROVIDER = "${strimzisecrets:%s/%s:%s}";

    protected String groupRegex = ".*";
    protected String topicRegex = ".*";
    protected String groupExcludeRegex;
    protected String topicExcludeRegex;

    @SuppressFBWarnings({"UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR"}) // Initialized in fromCrd
    private KafkaClusterSecurityContext securityContext;

    private LoggingModel logging;

    private DeploymentTemplate templateDeployment;
    private PodTemplate templatePod;
    private PodDisruptionBudgetTemplate templatePodDisruptionBudget;

    private static final Map<String, String> DEFAULT_POD_LABELS = new HashMap<>();
    static {
        String value = System.getenv(CO_ENV_VAR_CUSTOM_SOVA_POD_LABELS);
        if (value != null) {
            DEFAULT_POD_LABELS.putAll(Util.parseMap(value));
        }
    }

    /**
     * Constructor
     *
     * @param reconciliation            The reconciliation
     * @param resource                  Kubernetes resource with metadata containing the namespace and cluster name
     * @param sharedEnvironmentProvider Shared environment provider
     */
    protected Sova(Reconciliation reconciliation, HasMetadata resource, SharedEnvironmentProvider sharedEnvironmentProvider) {
        super(reconciliation, resource, SovaResources.componentName(resource.getMetadata().getName()), COMPONENT_TYPE, sharedEnvironmentProvider);
    }

    /**
     * Builds the Sova model from the Kafka custom resource. If Sova is not enabled, returns null.
     *
     * @param reconciliation            Reconciliation marker for logging
     * @param kafkaAssembly             The Kafka CR
     * @param versions                  The supported Kafka versions
     * @param sharedEnvironmentProvider Shared environment provider
     * @param securityContext           Kafka cluster security context
     *
     * @return Sova model object when Sova is enabled, or null if disabled.
     */
    public static Sova fromCrd(Reconciliation reconciliation, Kafka kafkaAssembly, KafkaVersion.Lookup versions, SharedEnvironmentProvider sharedEnvironmentProvider, KafkaClusterSecurityContext securityContext) {
        SovaSpec spec = kafkaAssembly.getSpec().getExtraMetrics();

        if (spec != null) {
            ModelUtils.validateComputeResources(spec.getResources(), "Kafka.spec.extraMetrics.resources");
            Sova result = new Sova(reconciliation, kafkaAssembly, sharedEnvironmentProvider);

            result.securityContext = securityContext;
            result.resources = spec.getResources();
            result.readinessProbeOptions = ProbeUtils.extractReadinessProbeOptionsOrDefault(spec, DEFAULT_HEALTHCHECK_OPTIONS);
            result.livenessProbeOptions = ProbeUtils.extractLivenessProbeOptionsOrDefault(spec, DEFAULT_HEALTHCHECK_OPTIONS);

            result.groupRegex = spec.getGroupRegex();
            result.topicRegex = spec.getTopicRegex();
            result.groupExcludeRegex = spec.getGroupExcludeRegex();
            result.topicExcludeRegex = spec.getTopicExcludeRegex();

            result.jvmOptions = spec.getJvmOptions();

            String image = spec.getImage();
            if (image == null) {
                KafkaClusterSpec kafkaClusterSpec = kafkaAssembly.getSpec().getKafka();
                image = System.getenv().getOrDefault(ClusterOperatorConfig.STRIMZI_DEFAULT_SOVA_IMAGE, versions.kafkaImage(kafkaClusterSpec.getImage(), versions.defaultVersion().version()));
            }
            result.image = image;

            result.logging = new LoggingModel(spec, result.getClass().getSimpleName());

            if (spec.getTemplate() != null) {
                SovaTemplate template = spec.getTemplate();

                result.templateDeployment = template.getDeployment();
                result.templatePod = template.getPod();
                result.templateServiceAccount = template.getServiceAccount();
                result.templateContainer = template.getContainer();
                result.templatePodDisruptionBudget = template.getPodDisruptionBudget();
            }

            return result;
        } else {
            return null;
        }
    }

    protected List<ContainerPort> getContainerPortList() {
        return List.of(
                ContainerUtils.createContainerPort(MetricsModel.METRICS_PORT_NAME, MetricsModel.METRICS_PORT),
                ContainerUtils.createContainerPort(MANAGEMENT_PORT_NAME, MANAGEMENT_PORT));
    }

    /**
     * Generates the Sova Deployment.
     *
     * @param podAnnotations    Map with the annotations that will be used for the Pod metadata
     * @param isOpenShift       Flag indicating whether we are on OpenShift or not
     * @param imagePullPolicy   Image pull policy
     * @param imagePullSecrets  List of Image Pull Secrets
     *
     * @return Generated deployment
     */
    public Deployment generateDeployment(Map<String, String> podAnnotations, boolean isOpenShift, ImagePullPolicy imagePullPolicy, List<LocalObjectReference> imagePullSecrets) {
        PodSecurityProviderContext podSecurityProviderContext = new PodSecurityProviderContextImpl(templatePod);

        return WorkloadUtils.createDeployment(
                componentName,
                namespace,
                labels,
                ownerReference,
                templateDeployment,
                1,
                null,
                WorkloadUtils.deploymentStrategy(TemplateUtils.deploymentStrategy(templateDeployment, StrimziDeploymentStrategy.ROLLING_UPDATE)),
                WorkloadUtils.createPodTemplateSpec(
                        componentName,
                        labels,
                        templatePod,
                        DEFAULT_POD_LABELS,
                        podAnnotations,
                        templatePod != null ? templatePod.getAffinity() : null,
                        null,
                        List.of(createContainer(imagePullPolicy)),
                        getVolumes(),
                        imagePullSecrets,
                        securityProvider.sovaPodSecurityContext(podSecurityProviderContext),
                        securityProvider.sovaHostUsers(podSecurityProviderContext))
        );
    }

    /* test */ Container createContainer(ImagePullPolicy imagePullPolicy) {
        return ContainerUtils.createContainer(
                componentName,
                image,
                List.of("/opt/sova/bin/sova_run.sh"),
                securityProvider.sovaContainerSecurityContext(new ContainerSecurityProviderContextImpl(templateContainer)),
                resources,
                getEnvVars(),
                getContainerPortList(),
                getVolumeMounts(),
                ProbeUtils.httpProbe(livenessProbeOptions, "/healthy", MANAGEMENT_PORT_NAME),
                ProbeUtils.httpProbe(readinessProbeOptions, "/ready", MANAGEMENT_PORT_NAME),
                imagePullPolicy
        );
    }

    /**
     * Generates the Kafka Admin client configuration in the Java properties format. It is passed to Sova as is.
     * The certificates are not part of the configuration. They are loaded by the Kubernetes Secret config provider.
     *
     * @return  Kafka Admin client configuration as a multi-line string with key=value pairs
     */
    @SuppressWarnings("checkstyle:NoFullyQualifiedClassNames") // False positive, fully qualified class names used in strings
    /* test */ String kafkaAdminClientConfiguration() {
        boolean tls = isTlsEnabled();
        boolean sasl = securityContext.authentication() instanceof ServiceAccountAuthenticationConfiguration;

        StringBuilder config = new StringBuilder();
        config.append("bootstrap.servers=").append(KafkaResources.bootstrapServiceName(cluster)).append(":").append(KafkaCluster.REPLICATION_PORT).append("\n");
        config.append("security.protocol=").append(sasl ? "SASL_" : "").append(tls ? "SSL" : "PLAINTEXT").append("\n");

        if (tls) {
            config.append("ssl.truststore.type=PEM\n");
            config.append("ssl.truststore.certificates=").append(secretPlaceholder(KafkaResources.trustBundleSecretName(cluster), "cluster-ca.crt")).append("\n");
        }

        if (securityContext.authentication() instanceof MtlsAuthenticationConfiguration) {
            // mTLS can be used only with TLS encryption
            config.append("ssl.keystore.type=PEM\n");
            config.append("ssl.keystore.certificate.chain=").append(secretPlaceholder(SovaResources.secretName(cluster), "sova.crt")).append("\n");
            config.append("ssl.keystore.key=").append(secretPlaceholder(SovaResources.secretName(cluster), "sova.key")).append("\n");
        } else if (sasl) {
            config.append("sasl.mechanism=OAUTHBEARER\n");
            config.append("sasl.login.callback.handler.class=io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler\n");
            config.append("sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required oauth.access.token.location=\"/var/run/secrets/strimzi.io/token\";\n");
        }

        if (tls) {
            // The config provider is needed only when some values are loaded from Secrets
            config.append("config.providers=strimzisecrets\n");
            config.append("config.providers.strimzisecrets.class=io.strimzi.kafka.KubernetesSecretConfigProvider\n");
        }

        return config.toString();
    }

    private boolean isTlsEnabled() {
        return securityContext.encryption() instanceof TlsEncryptionConfiguration;
    }

    /**
     * Collects the names of the Secrets which the Kubernetes Secret config provider needs to read.
     *
     * @return  Set with the Secret names. Empty when TLS is disabled.
     */
    /* test */ Set<String> secretsToAllowAccessTo() {
        Set<String> secrets = new HashSet<>();

        if (isTlsEnabled()) {
            secrets.add(KafkaResources.trustBundleSecretName(cluster));

            if (securityContext.authentication() instanceof MtlsAuthenticationConfiguration) {
                secrets.add(SovaResources.secretName(cluster));
            }
        }

        return secrets;
    }

    /**
     * Generates the Role which allows Sova to read the Secrets with the certificates.
     *
     * @return  The Role, or null when Sova does not need to read any Secrets
     */
    public Role generateRole() {
        return AuthenticationUtils.generateAuthenticationSecretsRole(componentName, namespace, secretsToAllowAccessTo(), labels, ownerReference);
    }

    /**
     * Generates the RoleBinding which binds the Sova ServiceAccount to the Role.
     *
     * @return  The RoleBinding, or null when Sova does not need to read any Secrets
     */
    public RoleBinding generateRoleBindingForRole() {
        return AuthenticationUtils.generateAuthenticationSecretsRoleBinding(getRoleBindingName(), componentName, namespace, secretsToAllowAccessTo(), labels, ownerReference);
    }

    /**
     * Gets the name of the RoleBinding.
     *
     * @return  Name of the RoleBinding
     */
    public String getRoleBindingName() {
        return SovaResources.roleBindingName(cluster);
    }

    private String secretPlaceholder(String secretName, String key) {
        return String.format(PLACEHOLDER_SECRET_TEMPLATE_KUBE_CONFIG_PROVIDER, namespace, secretName, key);
    }

    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();

        varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_ADMIN_CLIENT_CONFIGURATION, kafkaAdminClientConfiguration()));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_GROUP_REGEX, groupRegex));
        if (groupExcludeRegex != null) {
            varList.add(ContainerUtils.createEnvVar(ENV_VAR_GROUP_EXCLUDE_REGEX, groupExcludeRegex));
        }
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_TOPIC_REGEX, topicRegex));
        if (topicExcludeRegex != null) {
            varList.add(ContainerUtils.createEnvVar(ENV_VAR_TOPIC_EXCLUDE_REGEX, topicExcludeRegex));
        }
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_METRICS_PORT, String.valueOf(MetricsModel.METRICS_PORT)));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_MANAGEMENT_PORT, String.valueOf(MANAGEMENT_PORT)));

        // JVM options
        JvmOptionUtils.heapOptions(varList, 75, 0L, jvmOptions, resources);
        JvmOptionUtils.jvmPerformanceOptions(varList, jvmOptions);
        JvmOptionUtils.jvmSystemProperties(varList, jvmOptions);

        // Shared environment variables
        varList.addAll(sharedEnvironmentProvider.variables());

        ContainerUtils.addContainerEnvsToExistingEnvs(reconciliation, varList, templateContainer);

        return varList;
    }

    private List<Volume> getVolumes() {
        List<Volume> volumeList = new ArrayList<>();

        volumeList.add(VolumeUtils.createTempDirVolume(templatePod));

        if (isTlsEnabled()) {
            // Needed by the Kubernetes Secret config provider to load the certificates from Secrets
            volumeList.add(VolumeUtils.createServiceAccountVolume());
        }

        if (securityContext.authentication() instanceof ServiceAccountAuthenticationConfiguration saAuthentication) {
            volumeList.add(VolumeUtils.createStrimziAuthenticationTokenProjection(
                    saAuthentication.audience(), saAuthentication.expirationSeconds()));
        }

        volumeList.add(VolumeUtils.createConfigMapVolume(LOG_AND_METRICS_CONFIG_VOLUME_NAME,
                SovaResources.configMapName(cluster)));

        TemplateUtils.addAdditionalVolumes(templatePod, volumeList);

        return volumeList;
    }

    private List<VolumeMount> getVolumeMounts() {
        List<VolumeMount> volumeList = new ArrayList<>();

        volumeList.add(VolumeUtils.createTempDirVolumeMount());

        if (isTlsEnabled()) {
            volumeList.add(VolumeUtils.createServiceAccountVolumeMount());
        }

        if (securityContext.authentication() instanceof ServiceAccountAuthenticationConfiguration) {
            volumeList.add(VolumeUtils.createStrimziAuthenticationTokenVolumeMount());
        }

        volumeList.add(VolumeUtils.createVolumeMount(LOG_AND_METRICS_CONFIG_VOLUME_NAME, LOG_AND_METRICS_CONFIG_VOLUME_MOUNT));

        TemplateUtils.addAdditionalVolumeMounts(volumeList, templateContainer);

        return volumeList;
    }

    /**
     * Generates the Secret containing the Sova certificate signed by the cluster CA certificate used for
     * TLS based internal communication with Kafka. It also contains the related Sova private key.
     *
     * @param clusterCa                             The cluster CA.
     * @param existingSecret                        The existing secret with certificates
     * @param isMaintenanceTimeWindowsSatisfied     Indicates whether we are in the maintenance window or not.
     *
     * @return The generated Secret.
     */
    public CompletionStage<Secret> generateCertificatesSecret(Ca clusterCa, Secret existingSecret, boolean isMaintenanceTimeWindowsSatisfied) {
        CertAndKey existingCertAndKey = CertSecretUtils.keyStoreCertAndKey(existingSecret, COMPONENT_TYPE, clusterCa.caCertGenerationAnnotation());

        return clusterCa.maybeCopyOrGenerateClientCert(reconciliation, SovaResources.secretName(cluster), componentName, existingCertAndKey, isMaintenanceTimeWindowsSatisfied, labels)
                .thenApply(updatedCert -> {
                    Map<String, String> secretData = CertSecretUtils.buildSecretData(COMPONENT_TYPE, updatedCert);
                    return ModelUtils.createSecret(
                            SovaResources.secretName(cluster),
                            namespace,
                            labels,
                            ownerReference,
                            secretData,
                            Map.of(clusterCa.caCertGenerationAnnotation(), String.valueOf(updatedCert.caCertGeneration())),
                            Map.of()
                    );
                });
    }

    /**
     * Generates the NetworkPolicy relevant for Sova.
     *
     * @return The network policy.
     */
    public NetworkPolicy generateNetworkPolicy() {
        List<NetworkPolicyIngressRule> rules = new ArrayList<>();

        // Everyone can access metrics
        rules.add(NetworkPolicyUtils.createIngressRule(MetricsModel.METRICS_PORT, List.of()));

        return NetworkPolicyUtils.createNetworkPolicy(
                componentName,
                namespace,
                labels,
                ownerReference,
                rules
        );
    }

    /**
     * Generates the PodDisruptionBudget for Sova.
     *
     * @return The PodDisruptionBudget.
     */
    public PodDisruptionBudget generatePodDisruptionBudget() {
        return PodDisruptionBudgetUtils.createPodDisruptionBudget(
                componentName,
                namespace,
                labels,
                ownerReference,
                templatePodDisruptionBudget
        );
    }

    /**
     * Generates the ConfigMap for Sova metrics and logging configuration.
     *
     * @param metricsAndLogging  The metrics and logging configuration
     *
     * @return The generated ConfigMap
     */
    public ConfigMap generateMetricsAndLogConfigMap(MetricsAndLogging metricsAndLogging) {
        return ConfigMapUtils.createConfigMap(
                SovaResources.configMapName(cluster),
                namespace,
                labels,
                ownerReference,
                ConfigMapUtils.generateMetricsAndLogConfigMapData(reconciliation, this, metricsAndLogging)
        );
    }

    @Override
    public LoggingModel logging() {
        return logging;
    }
}
