/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.LocalObjectReferenceBuilder;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.NoImageException;
import io.strimzi.operator.cluster.model.UnsupportedVersionException;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.AbstractResourceOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

/**
 * Cluster Operator configuration
 */
public class ClusterOperatorConfig {
    private static final Logger LOGGER = LogManager.getLogger(ClusterOperatorConfig.class.getName());

    public static final String STRIMZI_NAMESPACE = "STRIMZI_NAMESPACE";
    public static final String STRIMZI_FULL_RECONCILIATION_INTERVAL_MS = "STRIMZI_FULL_RECONCILIATION_INTERVAL_MS";
    public static final String STRIMZI_OPERATION_TIMEOUT_MS = "STRIMZI_OPERATION_TIMEOUT_MS";
    public static final String STRIMZI_ZOOKEEPER_ADMIN_SESSION_TIMEOUT_MS = "STRIMZI_ZOOKEEPER_ADMIN_SESSION_TIMEOUT_MS";
    public static final String STRIMZI_CONNECT_BUILD_TIMEOUT_MS = "STRIMZI_CONNECT_BUILD_TIMEOUT_MS";
    public static final String STRIMZI_IMAGE_PULL_POLICY = "STRIMZI_IMAGE_PULL_POLICY";
    public static final String STRIMZI_IMAGE_PULL_SECRETS = "STRIMZI_IMAGE_PULL_SECRETS";
    public static final String STRIMZI_OPERATOR_NAMESPACE = "STRIMZI_OPERATOR_NAMESPACE";
    public static final String STRIMZI_OPERATOR_NAMESPACE_LABELS = "STRIMZI_OPERATOR_NAMESPACE_LABELS";
    public static final String STRIMZI_CUSTOM_RESOURCE_SELECTOR = "STRIMZI_CUSTOM_RESOURCE_SELECTOR";
    public static final String STRIMZI_FEATURE_GATES = "STRIMZI_FEATURE_GATES";
    public static final String STRIMZI_OPERATIONS_THREAD_POOL_SIZE = "STRIMZI_OPERATIONS_THREAD_POOL_SIZE";
    public static final String STRIMZI_DNS_CACHE_TTL = "STRIMZI_DNS_CACHE_TTL";
    public static final String STRIMZI_POD_SET_RECONCILIATION_ONLY = "STRIMZI_POD_SET_RECONCILIATION_ONLY";
    public static final String STRIMZI_POD_SET_CONTROLLER_WORK_QUEUE_SIZE = "STRIMZI_POD_SET_CONTROLLER_WORK_QUEUE_SIZE";
    public static final String STRIMZI_POD_SECURITY_PROVIDER_CLASS = "STRIMZI_POD_SECURITY_PROVIDER_CLASS";

    //Used to identify which cluster operator created a Kubernetes event
    public static final String STRIMZI_OPERATOR_NAME = "STRIMZI_OPERATOR_NAME";

    // Feature Flags
    public static final String STRIMZI_CREATE_CLUSTER_ROLES = "STRIMZI_CREATE_CLUSTER_ROLES";
    public static final String STRIMZI_NETWORK_POLICY_GENERATION = "STRIMZI_NETWORK_POLICY_GENERATION";

    // Env vars for configuring images
    public static final String STRIMZI_KAFKA_IMAGES = "STRIMZI_KAFKA_IMAGES";
    public static final String STRIMZI_KAFKA_CONNECT_IMAGES = "STRIMZI_KAFKA_CONNECT_IMAGES";
    public static final String STRIMZI_KAFKA_MIRROR_MAKER_IMAGES = "STRIMZI_KAFKA_MIRROR_MAKER_IMAGES";
    public static final String STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES = "STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES";
    public static final String STRIMZI_DEFAULT_TLS_SIDECAR_ENTITY_OPERATOR_IMAGE = "STRIMZI_DEFAULT_TLS_SIDECAR_ENTITY_OPERATOR_IMAGE";
    public static final String STRIMZI_DEFAULT_TLS_SIDECAR_KAFKA_IMAGE = "STRIMZI_DEFAULT_TLS_SIDECAR_KAFKA_IMAGE"; // Used only to produce warning if defined at startup
    public static final String STRIMZI_DEFAULT_TLS_SIDECAR_CRUISE_CONTROL_IMAGE = "STRIMZI_DEFAULT_TLS_SIDECAR_CRUISE_CONTROL_IMAGE"; // Used only to produce warning if defined at startup
    public static final String STRIMZI_DEFAULT_KAFKA_EXPORTER_IMAGE = "STRIMZI_DEFAULT_KAFKA_EXPORTER_IMAGE";
    public static final String STRIMZI_DEFAULT_TOPIC_OPERATOR_IMAGE = "STRIMZI_DEFAULT_TOPIC_OPERATOR_IMAGE";
    public static final String STRIMZI_DEFAULT_USER_OPERATOR_IMAGE = "STRIMZI_DEFAULT_USER_OPERATOR_IMAGE";
    public static final String STRIMZI_DEFAULT_KAFKA_INIT_IMAGE = "STRIMZI_DEFAULT_KAFKA_INIT_IMAGE";
    public static final String STRIMZI_DEFAULT_KAFKA_BRIDGE_IMAGE = "STRIMZI_DEFAULT_KAFKA_BRIDGE_IMAGE";
    public static final String STRIMZI_DEFAULT_CRUISE_CONTROL_IMAGE = "STRIMZI_DEFAULT_CRUISE_CONTROL_IMAGE";
    public static final String STRIMZI_DEFAULT_KANIKO_EXECUTOR_IMAGE = "STRIMZI_DEFAULT_KANIKO_EXECUTOR_IMAGE";
    public static final String STRIMZI_DEFAULT_MAVEN_BUILDER = "STRIMZI_DEFAULT_MAVEN_BUILDER";

    // Env vars configured in the Cluster operator deployment but passed to all operands
    public static final String HTTP_PROXY = "HTTP_PROXY";
    public static final String HTTPS_PROXY = "HTTPS_PROXY";
    public static final String NO_PROXY = "NO_PROXY";
    public static final String FIPS_MODE = "FIPS_MODE";

    // Default values
    public static final long DEFAULT_FULL_RECONCILIATION_INTERVAL_MS = 120_000;
    public static final int DEFAULT_POD_SET_CONTROLLER_WORK_QUEUE_SIZE = 1024;
    public static final long DEFAULT_OPERATION_TIMEOUT_MS = 300_000;
    public static final String DEFAULT_OPERATOR_NAME = "cluster-operator-name-unset";
    public static final int DEFAULT_ZOOKEEPER_ADMIN_SESSION_TIMEOUT_MS = 10_000;
    public static final long DEFAULT_CONNECT_BUILD_TIMEOUT_MS = 300_000;
    public static final int DEFAULT_OPERATIONS_THREAD_POOL_SIZE = 10;
    public static final int DEFAULT_DNS_CACHE_TTL = 30;
    public static final boolean DEFAULT_NETWORK_POLICY_GENERATION = true;
    public static final boolean DEFAULT_CREATE_CLUSTER_ROLES = false;
    public static final boolean DEFAULT_POD_SET_RECONCILIATION_ONLY = false;
    public static final String DEFAULT_POD_SECURITY_PROVIDER_CLASS = "io.strimzi.plugin.security.profiles.impl.BaselinePodSecurityProvider";

    // PodSecurityPolicy shortcut keywords and the corresponding class names
    public static final String POD_SECURITY_PROVIDER_BASELINE_SHORTCUT = "baseline";
    public static final String POD_SECURITY_PROVIDER_BASELINE_CLASS = "io.strimzi.plugin.security.profiles.impl.BaselinePodSecurityProvider";
    public static final String POD_SECURITY_PROVIDER_RESTRICTED_SHORTCUT = "restricted";
    public static final String POD_SECURITY_PROVIDER_RESTRICTED_CLASS = "io.strimzi.plugin.security.profiles.impl.RestrictedPodSecurityProvider";

    private final Set<String> namespaces;
    private final long reconciliationIntervalMs;
    private final long operationTimeoutMs;
    private final int zkAdminSessionTimeoutMs;
    private final long connectBuildTimeoutMs;
    private final boolean createClusterRoles;
    private final boolean networkPolicyGeneration;
    private final KafkaVersion.Lookup versions;
    private final ImagePullPolicy imagePullPolicy;
    private final List<LocalObjectReference> imagePullSecrets;
    private final String operatorNamespace;
    private final Labels operatorNamespaceLabels;
    private final Labels customResourceSelector;
    private final FeatureGates featureGates;
    private final int operationsThreadPoolSize;
    private final int dnsCacheTtlSec;
    private final boolean podSetReconciliationOnly;
    private final int podSetControllerWorkQueueSize;
    private final String operatorName;
    private final String podSecurityProviderClass;

    /**
     * Constructor
     *
     * @param namespaces                    namespace in which the operator will run and create resources
     * @param reconciliationIntervalMs      specify every how many milliseconds the reconciliation runs
     * @param operationTimeoutMs            timeout for internal operations specified in milliseconds
     * @param connectBuildTimeoutMs         timeout used to wait for a Kafka Connect builds to finish
     * @param createClusterRoles            true to create the ClusterRoles
     * @param networkPolicyGeneration       true to generate Network Policies
     * @param versions                      The configured Kafka versions
     * @param imagePullPolicy               Image pull policy configured by the user
     * @param imagePullSecrets              Set of secrets for pulling container images from secured repositories
     * @param operatorNamespace             Name of the namespace in which the operator is running
     * @param operatorNamespaceLabels       Labels of the namespace in which the operator is running (used for network policies)
     * @param customResourceSelector        Labels used to filter the custom resources seen by the cluster operator
     * @param featureGates                  Configuration string with feature gates settings
     * @param operationsThreadPoolSize      The size of the thread pool used for various operations
     * @param zkAdminSessionTimeoutMs       Session timeout for the Zookeeper Admin client used in ZK scaling operations
     * @param dnsCacheTtlSec                Number of seconds to cache a successful DNS name lookup
     * @param podSetReconciliationOnly      Indicates whether this Cluster Operator instance should reconcile only the
     *                                      StrimziPodSet resources or not
     * @param podSetControllerWorkQueueSize Indicates the size of the StrimziPodSetController work queue
     * @param operatorName The Pod name of the cluster operator, used to identify source of K8s events the operator creates
     * @param podSecurityProviderClass      The PodSecurityProvider class which the operator should use
     */
    @SuppressWarnings("checkstyle:ParameterNumber")
    public ClusterOperatorConfig(
            Set<String> namespaces,
            long reconciliationIntervalMs,
            long operationTimeoutMs,
            long connectBuildTimeoutMs,
            boolean createClusterRoles,
            boolean networkPolicyGeneration,
            KafkaVersion.Lookup versions,
            ImagePullPolicy imagePullPolicy,
            List<LocalObjectReference> imagePullSecrets,
            String operatorNamespace,
            Labels operatorNamespaceLabels,
            Labels customResourceSelector,
            String featureGates,
            int operationsThreadPoolSize,
            int zkAdminSessionTimeoutMs,
            int dnsCacheTtlSec,
            boolean podSetReconciliationOnly,
            int podSetControllerWorkQueueSize,
            String operatorName,
            String podSecurityProviderClass
    ) {
        this.namespaces = Set.copyOf(namespaces);
        this.reconciliationIntervalMs = reconciliationIntervalMs;
        this.operationTimeoutMs = operationTimeoutMs;
        this.connectBuildTimeoutMs = connectBuildTimeoutMs;
        this.createClusterRoles = createClusterRoles;
        this.networkPolicyGeneration = networkPolicyGeneration;
        this.versions = versions;
        this.imagePullPolicy = imagePullPolicy;
        this.imagePullSecrets = imagePullSecrets;
        this.operatorNamespace = operatorNamespace;
        this.operatorNamespaceLabels = operatorNamespaceLabels;
        this.customResourceSelector = customResourceSelector;
        this.featureGates = new FeatureGates(featureGates);
        this.operationsThreadPoolSize = operationsThreadPoolSize;
        this.zkAdminSessionTimeoutMs = zkAdminSessionTimeoutMs;
        this.dnsCacheTtlSec = dnsCacheTtlSec;
        this.podSetReconciliationOnly = podSetReconciliationOnly;
        this.podSetControllerWorkQueueSize = podSetControllerWorkQueueSize;
        this.operatorName = operatorName;
        this.podSecurityProviderClass = podSecurityProviderClass;
    }

    /**
     * Loads configuration parameters from a related map
     *
     * @param map   map from which loading configuration parameters
     * @return  Cluster Operator configuration instance
     */
    public static ClusterOperatorConfig fromMap(Map<String, String> map) {
        warningsForRemovedEndVars(map);
        KafkaVersion.Lookup lookup = parseKafkaVersions(map.get(STRIMZI_KAFKA_IMAGES), map.get(STRIMZI_KAFKA_CONNECT_IMAGES), map.get(STRIMZI_KAFKA_MIRROR_MAKER_IMAGES), map.get(STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES));
        return fromMap(map, lookup);
    }

    /**
     * Logs warnings for removed / deprecated environment variables
     *
     * @param map   map from which loading configuration parameters
     */
    private static void warningsForRemovedEndVars(Map<String, String> map) {
        if (map.containsKey(STRIMZI_DEFAULT_TLS_SIDECAR_KAFKA_IMAGE))    {
            LOGGER.warn("Kafka TLS sidecar container has been removed and the environment variable {} is not used anymore. " +
                    "You can remove it from the Strimzi Cluster Operator deployment.", STRIMZI_DEFAULT_TLS_SIDECAR_KAFKA_IMAGE);
        }
        if (map.containsKey(STRIMZI_DEFAULT_TLS_SIDECAR_CRUISE_CONTROL_IMAGE))    {
            LOGGER.warn("Cruise Control TLS sidecar container has been removed and the environment variable {} is not used anymore. " +
                    "You can remove it from the Strimzi Cluster Operator deployment.", STRIMZI_DEFAULT_TLS_SIDECAR_CRUISE_CONTROL_IMAGE);
        }
    }

    /**
     * Loads configuration parameters from a related map and custom KafkaVersion.Lookup instance.
     * This is used for testing.
     *
     * @param map   map from which loading configuration parameters
     * @param lookup KafkaVersion.Lookup instance with the supported Kafka version information
     * @return  Cluster Operator configuration instance
     */
    public static ClusterOperatorConfig fromMap(Map<String, String> map, KafkaVersion.Lookup lookup) {
        Set<String> namespaces = parseNamespaceList(map.get(STRIMZI_NAMESPACE));
        long reconciliationInterval = parseReconciliationInterval(map.get(STRIMZI_FULL_RECONCILIATION_INTERVAL_MS));
        long operationTimeout = parseTimeout(map.get(STRIMZI_OPERATION_TIMEOUT_MS), DEFAULT_OPERATION_TIMEOUT_MS);
        long connectBuildTimeout = parseTimeout(map.get(STRIMZI_CONNECT_BUILD_TIMEOUT_MS), DEFAULT_CONNECT_BUILD_TIMEOUT_MS);
        boolean createClusterRoles = parseBoolean(map.get(STRIMZI_CREATE_CLUSTER_ROLES), DEFAULT_CREATE_CLUSTER_ROLES);
        boolean networkPolicyGeneration = parseBoolean(map.get(STRIMZI_NETWORK_POLICY_GENERATION), DEFAULT_NETWORK_POLICY_GENERATION);
        ImagePullPolicy imagePullPolicy = parseImagePullPolicy(map.get(STRIMZI_IMAGE_PULL_POLICY));
        List<LocalObjectReference> imagePullSecrets = parseImagePullSecrets(map.get(STRIMZI_IMAGE_PULL_SECRETS));
        String operatorNamespace = map.get(STRIMZI_OPERATOR_NAMESPACE);
        Labels operatorNamespaceLabels = parseLabels(map, STRIMZI_OPERATOR_NAMESPACE_LABELS);
        Labels customResourceSelector = parseLabels(map, STRIMZI_CUSTOM_RESOURCE_SELECTOR);
        String featureGates = map.getOrDefault(STRIMZI_FEATURE_GATES, "");
        int operationsThreadPoolSize = parseInt(map.get(STRIMZI_OPERATIONS_THREAD_POOL_SIZE), DEFAULT_OPERATIONS_THREAD_POOL_SIZE);
        int zkAdminSessionTimeout = parseInt(map.get(STRIMZI_ZOOKEEPER_ADMIN_SESSION_TIMEOUT_MS), DEFAULT_ZOOKEEPER_ADMIN_SESSION_TIMEOUT_MS);
        int dnsCacheTtlSec = parseInt(map.get(STRIMZI_DNS_CACHE_TTL), DEFAULT_DNS_CACHE_TTL);
        boolean podSetReconciliationOnly = parseBoolean(map.get(STRIMZI_POD_SET_RECONCILIATION_ONLY), DEFAULT_POD_SET_RECONCILIATION_ONLY);
        int podSetControllerWorkQueueSize = parseInt(map.get(STRIMZI_POD_SET_CONTROLLER_WORK_QUEUE_SIZE), DEFAULT_POD_SET_CONTROLLER_WORK_QUEUE_SIZE);
        String podSecurityProviderClass = parsePodSecurityProviderClass(map.get(STRIMZI_POD_SECURITY_PROVIDER_CLASS));

        //Use default to prevent existing installations breaking if CO pod template not modified to pass through pod name
        String operatorName = map.getOrDefault(STRIMZI_OPERATOR_NAME, DEFAULT_OPERATOR_NAME);

        return new ClusterOperatorConfig(
                namespaces,
                reconciliationInterval,
                operationTimeout,
                connectBuildTimeout,
                createClusterRoles,
                networkPolicyGeneration,
                lookup,
                imagePullPolicy,
                imagePullSecrets,
                operatorNamespace,
                operatorNamespaceLabels,
                customResourceSelector,
                featureGates,
                operationsThreadPoolSize,
                zkAdminSessionTimeout,
                dnsCacheTtlSec,
                podSetReconciliationOnly,
                podSetControllerWorkQueueSize,
                operatorName,
                podSecurityProviderClass);
    }

    private static Set<String> parseNamespaceList(String namespacesList)   {
        Set<String> namespaces;
        if (namespacesList == null || namespacesList.isEmpty()) {
            namespaces = Collections.singleton(AbstractResourceOperator.ANY_NAMESPACE);
        } else {
            if (namespacesList.trim().equals(AbstractResourceOperator.ANY_NAMESPACE)) {
                namespaces = Collections.singleton(AbstractResourceOperator.ANY_NAMESPACE);
            } else if (namespacesList.matches("(\\s*[a-z0-9.-]+\\s*,)*\\s*[a-z0-9.-]+\\s*")) {
                namespaces = new HashSet<>(asList(namespacesList.trim().split("\\s*,+\\s*")));
            } else {
                throw new InvalidConfigurationException(STRIMZI_NAMESPACE
                        + " is not a valid list of namespaces nor the 'any namespace' wildcard "
                        + AbstractResourceOperator.ANY_NAMESPACE);
            }
        }

        return namespaces;
    }

    private static long parseReconciliationInterval(String reconciliationIntervalEnvVar) {
        long reconciliationInterval = DEFAULT_FULL_RECONCILIATION_INTERVAL_MS;

        if (reconciliationIntervalEnvVar != null) {
            reconciliationInterval = Long.parseLong(reconciliationIntervalEnvVar);
        }

        return reconciliationInterval;
    }

    private static long parseTimeout(String timeoutEnvVar, long defaultTimeout) {
        long timeout = defaultTimeout;

        if (timeoutEnvVar != null) {
            timeout = Long.parseLong(timeoutEnvVar);
        }

        return timeout;
    }

    private static int parseInt(String envVar, int defaultValue) {
        int value = defaultValue;

        if (envVar != null) {
            value = Integer.parseInt(envVar);
        }

        return value;
    }

    /* test */ static boolean parseBoolean(String envVar, boolean defaultValue) {
        boolean value = defaultValue;

        if (envVar != null) {
            value = Boolean.parseBoolean(envVar);
        }

        return value;
    }

    private static ImagePullPolicy parseImagePullPolicy(String imagePullPolicyEnvVar) {
        ImagePullPolicy imagePullPolicy = null;

        if (imagePullPolicyEnvVar != null) {
            switch (imagePullPolicyEnvVar.trim().toLowerCase(Locale.ENGLISH)) {
                case "always":
                    imagePullPolicy = ImagePullPolicy.ALWAYS;
                    break;
                case "ifnotpresent":
                    imagePullPolicy = ImagePullPolicy.IFNOTPRESENT;
                    break;
                case "never":
                    imagePullPolicy = ImagePullPolicy.NEVER;
                    break;
                default:
                    throw new InvalidConfigurationException(imagePullPolicyEnvVar
                            + " is not a valid " + STRIMZI_IMAGE_PULL_POLICY + " value. " +
                            STRIMZI_IMAGE_PULL_POLICY + " can have one of the following values: Always, IfNotPresent, Never.");
            }
        }

        return imagePullPolicy;
    }

    private static KafkaVersion.Lookup parseKafkaVersions(String kafkaImages, String connectImages, String mirrorMakerImages, String mirrorMaker2Images) {
        KafkaVersion.Lookup lookup = new KafkaVersion.Lookup(
                Util.parseMap(kafkaImages),
                Util.parseMap(connectImages),
                Util.parseMap(mirrorMakerImages),
                Util.parseMap(mirrorMaker2Images));

        String image = "";
        String envVar = "";

        try {
            image = "Kafka";
            envVar = STRIMZI_KAFKA_IMAGES;
            lookup.validateKafkaImages(lookup.supportedVersions());

            image = "Kafka Connect";
            envVar = STRIMZI_KAFKA_CONNECT_IMAGES;
            lookup.validateKafkaConnectImages(lookup.supportedVersions());

            image = "Kafka Mirror Maker";
            envVar = STRIMZI_KAFKA_MIRROR_MAKER_IMAGES;
            lookup.validateKafkaMirrorMakerImages(lookup.supportedVersions());

            image = "Kafka Mirror Maker 2";
            envVar = STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES;
            lookup.validateKafkaMirrorMaker2Images(lookup.supportedVersionsForFeature("kafkaMirrorMaker2"));
        } catch (NoImageException | UnsupportedVersionException e) {
            throw new InvalidConfigurationException("Failed to parse default container image configuration for " + image + " from environment variable " + envVar, e);
        }
        return lookup;
    }

    private static List<LocalObjectReference> parseImagePullSecrets(String imagePullSecretList) {
        List<LocalObjectReference> imagePullSecrets = null;

        if (imagePullSecretList != null && !imagePullSecretList.isEmpty()) {
            if (imagePullSecretList.matches("(\\s*[a-z0-9.-]+\\s*,)*\\s*[a-z0-9.-]+\\s*")) {
                imagePullSecrets = Arrays.stream(imagePullSecretList.trim().split("\\s*,+\\s*")).map(secret -> new LocalObjectReferenceBuilder().withName(secret).build()).collect(Collectors.toList());
            } else {
                throw new InvalidConfigurationException(STRIMZI_IMAGE_PULL_SECRETS
                        + " is not a valid list of secret names");
            }
        }

        return imagePullSecrets;
    }

    /**
     * Parse labels from String into the Labels format.
     *
     * @param vars              Map with configuration variables
     * @param configurationKey  Key containing the string with labels
     * @return                  Labels object with the Labels or null if no labels are configured
     */
    private static Labels parseLabels(Map<String, String> vars, String configurationKey) {
        String labelsString = vars.get(configurationKey);
        Labels labels = null;

        if (labelsString != null) {
            try {
                labels = Labels.fromString(labelsString);
            } catch (Exception e) {
                throw new InvalidConfigurationException("Failed to parse labels from " + configurationKey, e);
            }
        }

        return labels;
    }

    /**
     * Parse the configuration of the Pod Security Provider class which should be used to configure the Pod and
     * Container Security Contexts
     *
     * @param envVar The value of the environment variable configuring the Pod Security Provider
     * @return The full name of the class which should be used as the Pod security Provider
     */
    /* test */ static String parsePodSecurityProviderClass(String envVar) {
        String value = envVar != null ? envVar : DEFAULT_POD_SECURITY_PROVIDER_CLASS;

        if (POD_SECURITY_PROVIDER_BASELINE_SHORTCUT.equals(value.toLowerCase(Locale.ENGLISH)))  {
            return POD_SECURITY_PROVIDER_BASELINE_CLASS;
        } else if (POD_SECURITY_PROVIDER_RESTRICTED_SHORTCUT.equals(value.toLowerCase(Locale.ENGLISH)))  {
            return POD_SECURITY_PROVIDER_RESTRICTED_CLASS;
        } else {
            return value;
        }
    }

    /**
     * @return  namespaces in which the operator runs and creates resources
     */
    public Set<String> getNamespaces() {
        return namespaces;
    }

    /**
     * @return  how many milliseconds the reconciliation runs
     */
    public long getReconciliationIntervalMs() {
        return reconciliationIntervalMs;
    }

    /**
     * @return  how many milliseconds should we wait for Kubernetes operations
     */
    public long getOperationTimeoutMs() {
        return operationTimeoutMs;
    }

    /**
     * @return  how many milliseconds should we wait for Zookeeper Admin Sessions to timeout
     */
    public int getZkAdminSessionTimeoutMs() {
        return zkAdminSessionTimeoutMs;
    }

    /**
     * @return  How many milliseconds should we wait for Kafka Connect build to complete
     */
    public long getConnectBuildTimeoutMs() {
        return connectBuildTimeoutMs;
    }

    /**
     * @return  Indicates whether Cluster Roles should be created
     */
    public boolean isCreateClusterRoles() {
        return createClusterRoles;
    }

    /**
     * @return  Indicates whether Network policies should be generated
     */
    public boolean isNetworkPolicyGeneration() {
        return networkPolicyGeneration;
    }

    public KafkaVersion.Lookup versions() {
        return versions;
    }

    /**
     * @return  The user-configure image pull policy. Null if it was not configured.
     */
    public ImagePullPolicy getImagePullPolicy() {
        return imagePullPolicy;
    }

    /**
     * @return The list of configured ImagePullSecrets. Null if no secrets were configured.
     */
    public List<LocalObjectReference> getImagePullSecrets() {
        return imagePullSecrets;
    }

    /**
     * @return Returns the name of the namespace where the operator runs or null if not configured
     */
    public String getOperatorNamespace() {
        return operatorNamespace;
    }

    /**
     * @return Returns the labels of the namespace where the operator runs or null if not configured
     */
    public Labels getOperatorNamespaceLabels() {
        return operatorNamespaceLabels;
    }

    /**
     * @return Labels used for filtering custom resources
     */
    public Labels getCustomResourceSelector() {
        return customResourceSelector;
    }

    public FeatureGates featureGates()  {
        return featureGates;
    }

    /**
     * @return Thread Pool size to be used by the operator to do operations like reconciliation
     */
    public int getOperationsThreadPoolSize() {
        return operationsThreadPoolSize;
    }

    /**
     * @return Number of seconds to cache a successful DNS name lookup
     */
    public int getDnsCacheTtlSec() {
        return dnsCacheTtlSec;
    }

    /**
     * @return Indicates whether this Cluster Operator instance should reconcile only the StrimziPodSet resources or not
     */
    public boolean isPodSetReconciliationOnly() {
        return podSetReconciliationOnly;
    }

    /**
     * @return Returns the size of the StrimziPodSetController work queue
     */
    public int getPodSetControllerWorkQueueSize() {
        return podSetControllerWorkQueueSize;
    }


    public String getOperatorName() {
        return operatorName;
    }

    /**
     * @return Returns the Pdo Security Provider class
     */
    public String getPodSecurityProviderClass() {
        return podSecurityProviderClass;
    }

    @Override
    public String toString() {
        return "ClusterOperatorConfig(" +
                "namespaces=" + namespaces +
                ",reconciliationIntervalMs=" + reconciliationIntervalMs +
                ",operationTimeoutMs=" + operationTimeoutMs +
                ",connectBuildTimeoutMs=" + connectBuildTimeoutMs +
                ",createClusterRoles=" + createClusterRoles +
                ",networkPolicyGeneration=" + networkPolicyGeneration +
                ",versions=" + versions +
                ",imagePullPolicy=" + imagePullPolicy +
                ",imagePullSecrets=" + imagePullSecrets +
                ",operatorNamespace=" + operatorNamespace +
                ",operatorNamespaceLabels=" + operatorNamespaceLabels +
                ",customResourceSelector=" + customResourceSelector +
                ",featureGates=" + featureGates +
                ",zkAdminSessionTimeoutMs=" + zkAdminSessionTimeoutMs +
                ",dnsCacheTtlSec=" + dnsCacheTtlSec +
                ",podSetReconciliationOnly=" + podSetReconciliationOnly +
                ",podSetControllerWorkQueueSize=" + podSetControllerWorkQueueSize +
                ",operatorName=" + operatorName +
                ",podSecurityProviderClass=" + podSecurityProviderClass +
                ")";
    }
}
