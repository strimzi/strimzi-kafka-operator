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
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyIngressRule;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPeer;
import io.strimzi.api.kafka.model.common.JvmOptions;
import io.strimzi.api.kafka.model.common.template.DeploymentTemplate;
import io.strimzi.api.kafka.model.common.template.InternalServiceTemplate;
import io.strimzi.api.kafka.model.common.template.PodTemplate;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaClusterSpec;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlResources;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlSpec;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlTemplate;
import io.strimzi.certs.CertAndKey;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.cruisecontrol.Capacity;
import io.strimzi.operator.cluster.model.logging.LoggingModel;
import io.strimzi.operator.cluster.model.logging.SupportsLogging;
import io.strimzi.operator.cluster.model.metrics.MetricsModel;
import io.strimzi.operator.cluster.model.metrics.SupportsMetrics;
import io.strimzi.operator.cluster.model.securityprofiles.ContainerSecurityProviderContextImpl;
import io.strimzi.operator.cluster.model.securityprofiles.PodSecurityProviderContextImpl;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlConfigurationParameters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.strimzi.api.kafka.model.common.template.DeploymentStrategy.ROLLING_UPDATE;
import static io.strimzi.operator.cluster.model.CruiseControlConfiguration.CRUISE_CONTROL_DEFAULT_ANOMALY_DETECTION_GOALS;
import static io.strimzi.operator.cluster.model.CruiseControlConfiguration.CRUISE_CONTROL_GOALS;
import static io.strimzi.operator.cluster.model.VolumeUtils.createConfigMapVolume;
import static io.strimzi.operator.cluster.model.VolumeUtils.createSecretVolume;
import static io.strimzi.operator.cluster.model.VolumeUtils.createVolumeMount;

/**
 * Cruise Control model
 */
public class CruiseControl extends AbstractModel implements SupportsMetrics, SupportsLogging {
    protected static final String COMPONENT_TYPE = "cruise-control";
    protected static final String CRUISE_CONTROL_CONTAINER_NAME = "cruise-control";

    // Fields used for Cruise Control API authentication
    /**
     * Name of the admin user
     */
    public static final String API_ADMIN_NAME = "admin";
    private static final String API_ADMIN_ROLE = "ADMIN";
    protected static final String API_USER_NAME = "user";
    private static final String API_USER_ROLE = "USER";

    /**
     * Key for the admin user password
     */
    public static final String API_ADMIN_PASSWORD_KEY = COMPONENT_TYPE + ".apiAdminPassword";
    private static final String API_USER_PASSWORD_KEY = COMPONENT_TYPE + ".apiUserPassword";
    private static final String API_AUTH_FILE_KEY = COMPONENT_TYPE + ".apiAuthFile";
    protected static final String API_HEALTHCHECK_PATH = "/kafkacruisecontrol/state";

    protected static final String TLS_CC_CERTS_VOLUME_NAME = "cc-certs";
    protected static final String TLS_CC_CERTS_VOLUME_MOUNT = "/etc/cruise-control/cc-certs/";
    protected static final String TLS_CA_CERTS_VOLUME_NAME = "cluster-ca-certs";
    protected static final String TLS_CA_CERTS_VOLUME_MOUNT = "/etc/cruise-control/cluster-ca-certs/";
    protected static final String CONFIG_VOLUME_NAME = "config";
    protected static final String SERVER_CONFIG_FILENAME = "cruisecontrol.properties";
    protected static final String CAPACITY_CONFIG_FILENAME = "capacity.json";
    protected static final String CONFIG_VOLUME_MOUNT = "/opt/cruise-control/custom-config/";
    protected static final String API_AUTH_CONFIG_VOLUME_NAME = "api-auth-config";
    protected static final String API_AUTH_CONFIG_VOLUME_MOUNT = "/opt/cruise-control/api-auth-config/";
    protected static final String API_AUTH_CREDENTIALS_FILE = API_AUTH_CONFIG_VOLUME_MOUNT + API_AUTH_FILE_KEY;

    protected static final String ENV_VAR_CRUISE_CONTROL_METRICS_ENABLED = "CRUISE_CONTROL_METRICS_ENABLED";

    // Configuration defaults
    protected static final boolean DEFAULT_CRUISE_CONTROL_METRICS_ENABLED = false;
    
    private boolean sslEnabled;
    private boolean authEnabled;
    @SuppressFBWarnings({"UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR"}) // This field is initialized in the fromCrd method
    protected Capacity capacity;
    @SuppressFBWarnings({"UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR"}) // This field is initialized in the fromCrd method
    private MetricsModel metrics;
    private LoggingModel logging;
    /* test */ CruiseControlConfiguration configuration;

    /**
     * Port of the Cruise Control REST API
     */
    public static final int REST_API_PORT = 9090;
    /* test */ static final String REST_API_PORT_NAME = "rest-api";

    /* test */ static final String MIN_INSYNC_REPLICAS = "min.insync.replicas";

    /* test */ Capacity getCapacity() {
        return capacity;
    }

    // Cruise Control configuration keys (EnvVariables)
    protected static final String ENV_VAR_STRIMZI_KAFKA_BOOTSTRAP_SERVERS = "STRIMZI_KAFKA_BOOTSTRAP_SERVERS";

    protected static final String ENV_VAR_API_SSL_ENABLED = "STRIMZI_CC_API_SSL_ENABLED";
    protected static final String ENV_VAR_API_AUTH_ENABLED = "STRIMZI_CC_API_AUTH_ENABLED";
    protected static final String ENV_VAR_API_USER = "API_USER";
    protected static final String ENV_VAR_API_PORT = "API_PORT";
    protected static final String ENV_VAR_API_HEALTHCHECK_PATH = "API_HEALTHCHECK_PATH";

    protected static final String CO_ENV_VAR_CUSTOM_CRUISE_CONTROL_POD_LABELS = "STRIMZI_CUSTOM_CRUISE_CONTROL_LABELS";

    // Templates
    private DeploymentTemplate templateDeployment;
    private PodTemplate templatePod;
    private InternalServiceTemplate templateService;

    private static final Map<String, String> DEFAULT_POD_LABELS = new HashMap<>();
    static {
        String value = System.getenv(CO_ENV_VAR_CUSTOM_CRUISE_CONTROL_POD_LABELS);
        if (value != null) {
            DEFAULT_POD_LABELS.putAll(Util.parseMap(value));
        }
    }

    /**
     * Constructor
     *
     * @param reconciliation The reconciliation
     * @param resource  Kubernetes resource with metadata containing the namespace and cluster name
     * @param sharedEnvironmentProvider Shared environment provider
     */
    private CruiseControl(Reconciliation reconciliation, HasMetadata resource, SharedEnvironmentProvider sharedEnvironmentProvider) {
        super(reconciliation, resource, CruiseControlResources.componentName(resource.getMetadata().getName()), COMPONENT_TYPE, sharedEnvironmentProvider);
    }

    /**
     * Creates an instance of the Cruise Control model from the custom resource. When Cruise Control is not enabled,
     * this will return null.
     *
     * @param reconciliation                Reconciliation marker used for logging
     * @param kafkaCr                       The Kafka custom resource
     * @param versions                      Supported Kafka versions
     * @param kafkaBrokerNodes              List of the broker nodes which are part of the Kafka cluster
     * @param kafkaStorage                  A map with storage configuration used by the Kafka cluster and its node pools
     * @param kafkaBrokerResources          A map with resource configuration used by the Kafka cluster and its broker pools
     * @param sharedEnvironmentProvider     Shared environment provider
     *
     * @return  Instance of the Cruise Control model
     */
    @SuppressWarnings({"checkstyle:NPathComplexity", "checkstyle:CyclomaticComplexity"})
    public static CruiseControl fromCrd(
            Reconciliation reconciliation,
            Kafka kafkaCr,
            KafkaVersion.Lookup versions,
            Set<NodeRef> kafkaBrokerNodes,
            Map<String, Storage> kafkaStorage,
            Map<String, ResourceRequirements> kafkaBrokerResources,
            SharedEnvironmentProvider sharedEnvironmentProvider
    ) {
        CruiseControlSpec ccSpec = kafkaCr.getSpec().getCruiseControl();
        KafkaClusterSpec kafkaClusterSpec = kafkaCr.getSpec().getKafka();

        if (ccSpec != null) {
            CruiseControl result = new CruiseControl(reconciliation, kafkaCr, sharedEnvironmentProvider);

            String image = ccSpec.getImage();
            if (image == null) {
                image = System.getenv().getOrDefault(ClusterOperatorConfig.STRIMZI_DEFAULT_CRUISE_CONTROL_IMAGE, versions.kafkaImage(kafkaClusterSpec.getImage(), versions.defaultVersion().version()));
            }
            result.image = image;

            result.updateConfiguration(ccSpec);
            CruiseControlConfiguration ccConfiguration = result.configuration;
            result.sslEnabled = ccConfiguration.isApiSslEnabled();
            result.authEnabled = ccConfiguration.isApiAuthEnabled();

            // To avoid illegal storage configurations provided by the user,
            // we rely on the storage configuration provided by the KafkaAssemblyOperator
            result.capacity = new Capacity(reconciliation, kafkaCr.getSpec(), kafkaBrokerNodes, kafkaStorage, kafkaBrokerResources);
            result.readinessProbeOptions = ProbeUtils.extractReadinessProbeOptionsOrDefault(ccSpec, ProbeUtils.DEFAULT_HEALTHCHECK_OPTIONS);
            result.livenessProbeOptions = ProbeUtils.extractLivenessProbeOptionsOrDefault(ccSpec, ProbeUtils.DEFAULT_HEALTHCHECK_OPTIONS);
            result.gcLoggingEnabled = ccSpec.getJvmOptions() == null ? JvmOptions.DEFAULT_GC_LOGGING_ENABLED : ccSpec.getJvmOptions().isGcLoggingEnabled();
            result.jvmOptions = ccSpec.getJvmOptions();
            result.metrics = new MetricsModel(ccSpec);
            result.logging = new LoggingModel(ccSpec, result.getClass().getSimpleName(), true, false);
            result.resources = ccSpec.getResources();

            if (ccSpec.getTemplate() != null) {
                CruiseControlTemplate template = ccSpec.getTemplate();

                result.templateDeployment = template.getDeployment();
                result.templatePod = template.getPod();
                result.templateService = template.getApiService();
                result.templateServiceAccount = template.getServiceAccount();
                result.templateContainer = template.getCruiseControlContainer();
            }

            return result;
        } else {
            return null;
        }
    }

    private void updateConfiguration(CruiseControlSpec spec) {
        CruiseControlConfiguration userConfiguration = new CruiseControlConfiguration(reconciliation, spec.getConfig().entrySet());
        for (Map.Entry<String, String> defaultEntry : CruiseControlConfiguration.getCruiseControlDefaultPropertiesMap().entrySet()) {
            if (userConfiguration.getConfigOption(defaultEntry.getKey()) == null) {
                userConfiguration.setConfigOption(defaultEntry.getKey(), defaultEntry.getValue());
            }
        }
        // Ensure that the configured anomaly.detection.goals are a sub-set of the default goals
        checkGoals(userConfiguration);
        this.configuration = userConfiguration;
    }

    /**
     *  This method ensures that the checks in cruise-control/src/main/java/com/linkedin/kafka/cruisecontrol/config/KafkaCruiseControlConfig.java
     *  sanityCheckGoalNames() method (L118)  don't fail if a user submits custom default goals that have less members then the default
     *  anomaly.detection.goals.
     * @param configuration The configuration instance to be checked.
     * @throws UnsupportedOperationException If the configuration contains self.healing.goals configurations.
     */
    public void checkGoals(CruiseControlConfiguration configuration) {
        // If self healing goals are defined then these take precedence.
        // Right now, self.healing.goals must either be null or an empty list
        if (configuration.getConfigOption(CruiseControlConfigurationParameters.SELF_HEALING_CONFIG_KEY.toString()) != null) {
            String selfHealingGoalsString = configuration.getConfigOption(CruiseControlConfigurationParameters.SELF_HEALING_CONFIG_KEY.toString());
            List<String> selfHealingGoals = Arrays.asList(selfHealingGoalsString.split("\\s*,\\s*"));
            if (!selfHealingGoals.isEmpty()) {
                throw new UnsupportedOperationException("Cruise Control's self healing functionality is not currently supported. Please remove " +
                        CruiseControlConfigurationParameters.SELF_HEALING_CONFIG_KEY + " config");
            }
        }

        // If no anomaly detection goals have been defined by the user, the defaults defined in Cruise Control will be used.
        String anomalyGoalsString = configuration.getConfigOption(CruiseControlConfigurationParameters.ANOMALY_DETECTION_CONFIG_KEY.toString(), CRUISE_CONTROL_DEFAULT_ANOMALY_DETECTION_GOALS);
        Set<String> anomalyDetectionGoals = new HashSet<>(Arrays.asList(anomalyGoalsString.split("\\s*,\\s*")));

        String defaultGoalsString = configuration.getConfigOption(CruiseControlConfigurationParameters.DEFAULT_GOALS_CONFIG_KEY.toString(), CRUISE_CONTROL_GOALS);
        Set<String> defaultGoals = new HashSet<>(Arrays.asList(defaultGoalsString.split("\\s*,\\s*")));

        // Remove all the goals which are present in the default goals set from the anomaly detection goals
        anomalyDetectionGoals.removeAll(defaultGoals);

        if (!anomalyDetectionGoals.isEmpty()) {
            // If the anomaly detection goals contain goals which are not in the default goals then the CC startup
            // checks will fail, so we make the anomaly goals match the default goals
            configuration.setConfigOption(CruiseControlConfigurationParameters.ANOMALY_DETECTION_CONFIG_KEY.toString(), defaultGoalsString);
            LOGGER.warnCr(reconciliation, "Anomaly goals contained goals which are not in the configured default goals. Anomaly goals have " +
                    "been changed to match the specified default goals.");
        }
    }

    /**
     * @return  Generates a Kubernetes Service for Cruise Control
     */
    public Service generateService() {
        return ServiceUtils.createClusterIpService(
                CruiseControlResources.serviceName(cluster),
                namespace,
                labels,
                ownerReference,
                templateService,
                List.of(ServiceUtils.createServicePort(REST_API_PORT_NAME, REST_API_PORT, REST_API_PORT, "TCP"))
        );
    }

    protected List<ContainerPort> getContainerPortList() {
        List<ContainerPort> portList = new ArrayList<>(1);

        portList.add(ContainerUtils.createContainerPort(REST_API_PORT_NAME, REST_API_PORT));

        if (metrics.isEnabled()) {
            portList.add(ContainerUtils.createContainerPort(MetricsModel.METRICS_PORT_NAME, MetricsModel.METRICS_PORT));
        }

        return portList;
    }

    protected List<Volume> getVolumes(boolean isOpenShift) {
        return List.of(VolumeUtils.createTempDirVolume(templatePod),
                createSecretVolume(TLS_CC_CERTS_VOLUME_NAME, CruiseControlResources.secretName(cluster), isOpenShift),
                createSecretVolume(TLS_CA_CERTS_VOLUME_NAME, AbstractModel.clusterCaCertSecretName(cluster), isOpenShift),
                createSecretVolume(API_AUTH_CONFIG_VOLUME_NAME, CruiseControlResources.apiSecretName(cluster), isOpenShift),
                createConfigMapVolume(CONFIG_VOLUME_NAME, CruiseControlResources.configMapName(cluster)));
    }

    protected List<VolumeMount> getVolumeMounts() {
        return List.of(VolumeUtils.createTempDirVolumeMount(),
                createVolumeMount(CruiseControl.TLS_CC_CERTS_VOLUME_NAME, CruiseControl.TLS_CC_CERTS_VOLUME_MOUNT),
                createVolumeMount(CruiseControl.TLS_CA_CERTS_VOLUME_NAME, CruiseControl.TLS_CA_CERTS_VOLUME_MOUNT),
                createVolumeMount(CruiseControl.API_AUTH_CONFIG_VOLUME_NAME, CruiseControl.API_AUTH_CONFIG_VOLUME_MOUNT),
                createVolumeMount(CONFIG_VOLUME_NAME, CONFIG_VOLUME_MOUNT));
    }

    /**
     * Generates Kubernetes Deployment for Cruise Control
     *
     * @param isOpenShift       Flag indicating if we are on OpenShift or not
     * @param imagePullPolicy   Image pull policy
     * @param imagePullSecrets  Image pull secrets
     *
     * @return  Cruise Control Kubernetes Deployment
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
                        securityProvider.cruiseControlPodSecurityContext(new PodSecurityProviderContextImpl(templatePod))
                )
        );
    }

    /* test */ Container createContainer(ImagePullPolicy imagePullPolicy) {
        return ContainerUtils.createContainer(
                CRUISE_CONTROL_CONTAINER_NAME,
                image,
                List.of("/opt/cruise-control/cruise_control_run.sh"),
                securityProvider.cruiseControlContainerSecurityContext(new ContainerSecurityProviderContextImpl(templateContainer)),
                resources,
                getEnvVars(),
                getContainerPortList(),
                getVolumeMounts(),
                ProbeUtils.defaultBuilder(livenessProbeOptions).withNewExec().withCommand("/opt/cruise-control/cruise_control_healthcheck.sh").endExec().build(),
                ProbeUtils.defaultBuilder(readinessProbeOptions).withNewExec().withCommand("/opt/cruise-control/cruise_control_healthcheck.sh").endExec().build(),
                imagePullPolicy
        );
    }

    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();

        varList.add(ContainerUtils.createEnvVar(ENV_VAR_CRUISE_CONTROL_METRICS_ENABLED, String.valueOf(metrics.isEnabled())));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_STRIMZI_KAFKA_BOOTSTRAP_SERVERS, KafkaResources.bootstrapServiceName(cluster) + ":" + KafkaCluster.REPLICATION_PORT));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED, String.valueOf(gcLoggingEnabled)));


        varList.add(ContainerUtils.createEnvVar(ENV_VAR_API_SSL_ENABLED,  String.valueOf(this.sslEnabled)));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_API_AUTH_ENABLED,  String.valueOf(this.authEnabled)));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_API_USER,  API_USER_NAME));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_API_PORT,  String.valueOf(REST_API_PORT)));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_API_HEALTHCHECK_PATH, API_HEALTHCHECK_PATH));

        JvmOptionUtils.heapOptions(varList, 75, 0L, jvmOptions, resources);
        JvmOptionUtils.jvmPerformanceOptions(varList, jvmOptions);
        JvmOptionUtils.jvmSystemProperties(varList, jvmOptions);

        // Add shared environment variables used for all containers
        varList.addAll(sharedEnvironmentProvider.variables());

        ContainerUtils.addContainerEnvsToExistingEnvs(reconciliation, varList, templateContainer);

        return varList;
    }

    /**
     * Creates Cruise Control API auth usernames, passwords, and credentials file
     *
     * @return Map containing Cruise Control API auth credentials
     */
    public static Map<String, String> generateCruiseControlApiCredentials() {
        PasswordGenerator passwordGenerator = new PasswordGenerator(16);
        String apiAdminPassword = passwordGenerator.generate();
        String apiUserPassword = passwordGenerator.generate();

        /*
         * Create Cruise Control API auth credentials file following Jetty's
         *  HashLoginService's file format: username: password [,rolename ...]
         */
        String authCredentialsFile =
                API_ADMIN_NAME + ": " + apiAdminPassword + "," + API_ADMIN_ROLE + "\n" +
                API_USER_NAME + ": " + apiUserPassword + "," + API_USER_ROLE + "\n";

        Map<String, String> data = new HashMap<>(3);
        data.put(API_ADMIN_PASSWORD_KEY, Util.encodeToBase64(apiAdminPassword));
        data.put(API_USER_PASSWORD_KEY, Util.encodeToBase64(apiUserPassword));
        data.put(API_AUTH_FILE_KEY, Util.encodeToBase64(authCredentialsFile));

        return data;
    }

    /**
     * Generate the Secret containing the Cruise Control API auth credentials.
     *
     * @return The generated Secret.
     */
    public Secret generateApiSecret() {
        return ModelUtils.createSecret(CruiseControlResources.apiSecretName(cluster), namespace, labels, ownerReference, generateCruiseControlApiCredentials(), Collections.emptyMap(), Collections.emptyMap());
    }

    /**
     * Generate the Secret containing the Cruise Control certificate signed by the cluster CA certificate used for TLS based
     * internal communication with Kafka
     * It also contains the related Cruise Control private key.
     *
     * @param namespace Namespace in which the Cruise Control cluster runs
     * @param kafkaName Name of the Kafka cluster (it is used for the SANs in the certificate)
     * @param clusterCa The cluster CA.
     * @param isMaintenanceTimeWindowsSatisfied Indicates whether we are in the maintenance window or not.
     *                                          This is used for certificate renewals
     *
     * @return The generated Secret.
     */
    public Secret generateCertificatesSecret(String namespace, String kafkaName, ClusterCa clusterCa, boolean isMaintenanceTimeWindowsSatisfied) {
        Map<String, CertAndKey> ccCerts = new HashMap<>(4);
        LOGGER.debugCr(reconciliation, "Generating certificates");
        try {
            ccCerts = clusterCa.generateCcCerts(namespace, kafkaName, isMaintenanceTimeWindowsSatisfied);
        } catch (IOException e) {
            LOGGER.warnCr(reconciliation, "Error while generating certificates", e);
        }
        LOGGER.debugCr(reconciliation, "End generating certificates");

        return ModelUtils.createSecret(CruiseControlResources.secretName(cluster), namespace, labels, ownerReference,
                CertUtils.buildSecretData(ccCerts), Map.ofEntries(clusterCa.caCertGenerationFullAnnotation()), Map.of());
    }

    /**
     * Generates the NetworkPolicies relevant for Cruise Control
     *
     * @param operatorNamespace                             Namespace where the Strimzi Cluster Operator runs. Null if not configured.
     * @param operatorNamespaceLabels                       Labels of the namespace where the Strimzi Cluster Operator runs. Null if not configured.
     *
     * @return The network policy.
     */
    public NetworkPolicy generateNetworkPolicy(String operatorNamespace, Labels operatorNamespaceLabels) {
        NetworkPolicyPeer clusterOperatorPeer = NetworkPolicyUtils.createPeer(Map.of(Labels.STRIMZI_KIND_LABEL, "cluster-operator"), NetworkPolicyUtils.clusterOperatorNamespaceSelector(namespace, operatorNamespace, operatorNamespaceLabels));

        // List of network policy rules for all ports
        List<NetworkPolicyIngressRule> rules = new ArrayList<>();

        // CO can access the REST API
        rules.add(NetworkPolicyUtils.createIngressRule(REST_API_PORT, List.of(clusterOperatorPeer)));

        // Everyone can access metrics
        if (metrics.isEnabled()) {
            rules.add(NetworkPolicyUtils.createIngressRule(MetricsModel.METRICS_PORT, List.of()));
        }

        // Build the final network policy with all rules covering all the ports
        return NetworkPolicyUtils.createNetworkPolicy(
                CruiseControlResources.networkPolicyName(cluster),
                namespace,
                labels,
                ownerReference,
                rules
        );
    }

    /**
     * @return  Metrics Model instance for configuring Prometheus metrics
     */
    public MetricsModel metrics()   {
        return metrics;
    }

    /**
     * @return  Logging Model instance for configuring logging
     */
    public LoggingModel logging()   {
        return logging;
    }

    /**
     * Generates a ConfigMap with the following:
     *
     *  (1) Cruise Control server configuration
     *  (2) Cruise Control broker capacity configuration
     *  (3) Cruise Control server metrics and logging configuration
     *
     * @param metricsAndLogging The logging and metrics configuration
     *
     * @return The generated data
     */
    public ConfigMap generateConfigMap(MetricsAndLogging metricsAndLogging) {
        Map<String, String> configMapData = new HashMap<>();
        configMapData.put(SERVER_CONFIG_FILENAME, configuration.asOrderedProperties().asPairs());
        configMapData.put(CAPACITY_CONFIG_FILENAME, capacity.toString());
        configMapData.putAll(ConfigMapUtils.generateMetricsAndLogConfigMapData(reconciliation, this, metricsAndLogging));

        return ConfigMapUtils
                .createConfigMap(
                        CruiseControlResources.configMapName(cluster),
                        namespace,
                        labels,
                        ownerReference,
                        configMapData
                );
    }
}