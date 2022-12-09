/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user;

import io.strimzi.api.kafka.model.CertificateAuthority;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.model.Labels;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Cluster Operator configuration
 */
public class UserOperatorConfig {
    // Environment variable names
    static final String STRIMZI_NAMESPACE = "STRIMZI_NAMESPACE";
    static final String STRIMZI_FULL_RECONCILIATION_INTERVAL_MS = "STRIMZI_FULL_RECONCILIATION_INTERVAL_MS";
    static final String STRIMZI_LABELS = "STRIMZI_LABELS";
    static final String STRIMZI_CA_CERT_SECRET_NAME = "STRIMZI_CA_CERT_NAME";
    static final String STRIMZI_CA_KEY_SECRET_NAME = "STRIMZI_CA_KEY_NAME";
    static final String STRIMZI_CLUSTER_CA_CERT_SECRET_NAME = "STRIMZI_CLUSTER_CA_CERT_SECRET_NAME";
    static final String STRIMZI_EO_KEY_SECRET_NAME = "STRIMZI_EO_KEY_SECRET_NAME";
    static final String STRIMZI_CA_NAMESPACE = "STRIMZI_CA_NAMESPACE";
    static final String STRIMZI_KAFKA_BOOTSTRAP_SERVERS = "STRIMZI_KAFKA_BOOTSTRAP_SERVERS";
    static final String STRIMZI_CLIENTS_CA_VALIDITY = "STRIMZI_CA_VALIDITY";
    static final String STRIMZI_CLIENTS_CA_RENEWAL = "STRIMZI_CA_RENEWAL";
    static final String STRIMZI_SECRET_PREFIX = "STRIMZI_SECRET_PREFIX";
    static final String STRIMZI_ACLS_ADMIN_API_SUPPORTED = "STRIMZI_ACLS_ADMIN_API_SUPPORTED";
    static final String STRIMZI_KRAFT_ENABLED = "STRIMZI_KRAFT_ENABLED";
    static final String STRIMZI_SCRAM_SHA_PASSWORD_LENGTH = "STRIMZI_SCRAM_SHA_PASSWORD_LENGTH";
    static final String STRIMZI_MAINTENANCE_TIME_WINDOWS = "STRIMZI_MAINTENANCE_TIME_WINDOWS";
    static final String STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION = "STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION";
    static final String STRIMZI_OPERATION_TIMEOUT_MS = "STRIMZI_OPERATION_TIMEOUT_MS";
    static final String STRIMZI_WORK_QUEUE_SIZE = "STRIMZI_WORK_QUEUE_SIZE";
    static final String STRIMZI_CONTROLLER_THREAD_POOL_SIZE = "STRIMZI_CONTROLLER_THREAD_POOL_SIZE";
    static final String STRIMZI_CACHE_REFRESH_INTERVAL_MS = "STRIMZI_CACHE_REFRESH_INTERVAL_MS";
    static final String STRIMZI_BATCH_QUEUE_SIZE = "STRIMZI_BATCH_QUEUE_SIZE";
    static final String STRIMZI_BATCH_MAXIMUM_BLOCK_SIZE = "STRIMZI_BATCH_MAXIMUM_BLOCK_SIZE";
    static final String STRIMZI_BATCH_MAXIMUM_BLOCK_TIME_MS = "STRIMZI_BATCH_MAXIMUM_BLOCK_TIME_MS";
    static final String STRIMZI_USER_OPERATIONS_THREAD_POOL_SIZE = "STRIMZI_USER_OPERATIONS_THREAD_POOL_SIZE";

    // Default values
    static final long DEFAULT_FULL_RECONCILIATION_INTERVAL_MS = 120_000;
    static final String DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "localhost:9091";
    /**
     * Configures the default prefix of user secrets created by the operator
     */
    public static final String DEFAULT_SECRET_PREFIX = "";
    static final int DEFAULT_SCRAM_SHA_PASSWORD_LENGTH = 32;
    /**
     * Indicates whether the Admin APi can be used to manage ACLs. Defaults to true for backwards compatibility reasons.
     */
    public static final boolean DEFAULT_STRIMZI_ACLS_ADMIN_API_SUPPORTED = true;
    // Defaults to false for backwards compatibility in standalone UO deployments
    static final boolean DEFAULT_STRIMZI_KRAFT_ENABLED = false;
    static final long DEFAULT_OPERATION_TIMEOUT_MS = 300_000;
    static final int DEFAULT_WORK_QUEUE_SIZE = 1024;
    static final int DEFAULT_CONTROLLER_THREAD_POOL_SIZE = 50;
    static final long DEFAULT_CACHE_REFRESH_INTERVAL_MS = 15_000L;
    static final int DEFAULT_BATCH_QUEUE_SIZE = 1024;
    static final int DEFAULT_BATCH_MAXIMUM_BLOCK_SIZE = 100;
    static final int DEFAULT_BATCH_MAXIMUM_BLOCK_TIME_MS = 100;
    static final int DEFAULT_USER_OPERATIONS_THREAD_POOL_SIZE = 4;

    private final String namespace;
    private final long reconciliationIntervalMs;
    private final String kafkaBootstrapServers;
    private final Labels labels;
    private final String caCertSecretName;
    private final String caKeySecretName;
    private final String clusterCaCertSecretName;
    private final String euoKeySecretName;
    private final String caNamespace;
    private final String secretPrefix;
    private final int clientsCaValidityDays;
    private final int clientsCaRenewalDays;
    private final boolean aclsAdminApiSupported;
    private final boolean kraftEnabled;
    private final int scramPasswordLength;
    private final List<String> maintenanceWindows;
    private final Properties kafkaAdminClientConfiguration;
    private final long operationTimeoutMs;
    private final int workQueueSize;
    private final int controllerThreadPoolSize;
    private final long cacheRefresh;
    private final int batchQueueSize;
    private final int batchMaxBlockSize;
    private final int batchMaxBlockTime;
    private final int userOperationsThreadPoolSize;

    /**
     * Constructor
     *
     * @param namespace namespace in which the operator will run and create resources.
     * @param reconciliationIntervalMs How many milliseconds between reconciliation runs.
     * @param kafkaBootstrapServers Kafka bootstrap servers list
     * @param labels Map with labels which should be used to find the KafkaUser resources.
     * @param caCertSecretName Name of the secret containing the clients Certification Authority certificate.
     * @param caKeySecretName The name of the secret containing the clients Certification Authority key.
     * @param clusterCaCertSecretName Name of the secret containing the cluster Certification Authority certificate.
     * @param euoKeySecretName The name of the secret containing the Entity User Operator key and certificate
     * @param caNamespace Namespace with the CA secret.
     * @param secretPrefix Prefix used for the Secret names
     * @param aclsAdminApiSupported Indicates whether Kafka Admin API can be used to manage ACL rights
     * @param kraftEnabled Indicates whether KRaft is used in the Kafka cluster
     * @param clientsCaValidityDays Number of days for which the certificate should be valid
     * @param clientsCaRenewalDays How long before the certificate expiration should the user certificate be renewed
     * @param scramPasswordLength Length used for the Scram-Sha Password
     * @param maintenanceWindows Lit of maintenance windows
     * @param kafkaAdminClientConfiguration Additional configuration for the Kafka Admin Client
     * @param operationTimeoutMs Timeout for internal operations specified in milliseconds
     * @param workQueueSize Indicates the size of the StrimziPodSetController work queue
     * @param controllerThreadPoolSize Size of the pool of the controller threads used to reconcile the users
     * @param cacheRefresh Refresh interval for the cache storing the resources from the Kafka Admin API
     * @param batchQueueSize Maximal queue for requests when micro-batching the Kafka Admin API requests
     * @param batchMaxBlockSize Maximal batch size for micro-batching the Kafka Admin API requests
     * @param batchMaxBlockTime Maximal batch time for micro-batching the Kafka Admin API requests
     * @param userOperationsThreadPoolSize Size of the thread pool for user operations done by KafkaUserOperator and
     *                                     the classes used by it
     */
    @SuppressWarnings({"checkstyle:ParameterNumber"})
    public UserOperatorConfig(String namespace,
                              long reconciliationIntervalMs,
                              String kafkaBootstrapServers,
                              Labels labels,
                              String caCertSecretName,
                              String caKeySecretName,
                              String clusterCaCertSecretName,
                              String euoKeySecretName,
                              String caNamespace,
                              String secretPrefix,
                              boolean aclsAdminApiSupported,
                              boolean kraftEnabled,
                              int clientsCaValidityDays,
                              int clientsCaRenewalDays,
                              int scramPasswordLength,
                              List<String> maintenanceWindows,
                              Properties kafkaAdminClientConfiguration,
                              long operationTimeoutMs,
                              int workQueueSize,
                              int controllerThreadPoolSize,
                              long cacheRefresh,
                              int batchQueueSize,
                              int batchMaxBlockSize,
                              int batchMaxBlockTime,
                              int userOperationsThreadPoolSize
    ) {
        this.namespace = namespace;
        this.reconciliationIntervalMs = reconciliationIntervalMs;
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.labels = labels;
        this.caCertSecretName = caCertSecretName;
        this.caKeySecretName = caKeySecretName;
        this.clusterCaCertSecretName = clusterCaCertSecretName;
        this.euoKeySecretName = euoKeySecretName;
        this.caNamespace = caNamespace;
        this.secretPrefix = secretPrefix;
        this.aclsAdminApiSupported = aclsAdminApiSupported;
        this.kraftEnabled = kraftEnabled;
        this.clientsCaValidityDays = clientsCaValidityDays;
        this.clientsCaRenewalDays = clientsCaRenewalDays;
        this.scramPasswordLength = scramPasswordLength;
        this.maintenanceWindows = maintenanceWindows;
        this.kafkaAdminClientConfiguration = kafkaAdminClientConfiguration;
        this.operationTimeoutMs = operationTimeoutMs;
        this.workQueueSize = workQueueSize;
        this.controllerThreadPoolSize = controllerThreadPoolSize;
        this.cacheRefresh = cacheRefresh;
        this.batchQueueSize = batchQueueSize;
        this.batchMaxBlockSize = batchMaxBlockSize;
        this.batchMaxBlockTime = batchMaxBlockTime;
        this.userOperationsThreadPoolSize = userOperationsThreadPoolSize;
    }

    /**
     * Loads configuration parameters from a related map
     *
     * @param map   map from which loading configuration parameters
     * @return  Cluster Operator configuration instance
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    public static UserOperatorConfig fromMap(Map<String, String> map) {
        String namespace = map.get(UserOperatorConfig.STRIMZI_NAMESPACE);
        if (namespace == null || namespace.isEmpty()) {
            throw new InvalidConfigurationException(UserOperatorConfig.STRIMZI_NAMESPACE + " cannot be null");
        }

        long reconciliationInterval = getLongProperty(map, STRIMZI_FULL_RECONCILIATION_INTERVAL_MS, DEFAULT_FULL_RECONCILIATION_INTERVAL_MS);
        int scramPasswordLength = getIntProperty(map, STRIMZI_SCRAM_SHA_PASSWORD_LENGTH, DEFAULT_SCRAM_SHA_PASSWORD_LENGTH);
        boolean aclsAdminApiSupported = getBooleanProperty(map, UserOperatorConfig.STRIMZI_ACLS_ADMIN_API_SUPPORTED, UserOperatorConfig.DEFAULT_STRIMZI_ACLS_ADMIN_API_SUPPORTED);
        boolean kraftEnabled = getBooleanProperty(map, UserOperatorConfig.STRIMZI_KRAFT_ENABLED, UserOperatorConfig.DEFAULT_STRIMZI_KRAFT_ENABLED);
        int clientsCaValidityDays = getIntProperty(map, UserOperatorConfig.STRIMZI_CLIENTS_CA_VALIDITY, CertificateAuthority.DEFAULT_CERTS_VALIDITY_DAYS);
        int clientsCaRenewalDays = getIntProperty(map, UserOperatorConfig.STRIMZI_CLIENTS_CA_RENEWAL, CertificateAuthority.DEFAULT_CERTS_RENEWAL_DAYS);
        long operationTimeout = getLongProperty(map, STRIMZI_OPERATION_TIMEOUT_MS, DEFAULT_OPERATION_TIMEOUT_MS);
        int workQueueSize = getIntProperty(map, STRIMZI_WORK_QUEUE_SIZE, DEFAULT_WORK_QUEUE_SIZE);
        int controllerThreadPoolSize = getIntProperty(map, STRIMZI_CONTROLLER_THREAD_POOL_SIZE, DEFAULT_CONTROLLER_THREAD_POOL_SIZE);
        long cacheRefresh = getLongProperty(map, STRIMZI_CACHE_REFRESH_INTERVAL_MS, DEFAULT_CACHE_REFRESH_INTERVAL_MS);
        int batchQueueSize = getIntProperty(map, STRIMZI_BATCH_QUEUE_SIZE, DEFAULT_BATCH_QUEUE_SIZE);
        int batchMaxBlockSize = getIntProperty(map, STRIMZI_BATCH_MAXIMUM_BLOCK_SIZE, DEFAULT_BATCH_MAXIMUM_BLOCK_SIZE);
        int batchMaxBlockTime = getIntProperty(map, STRIMZI_BATCH_MAXIMUM_BLOCK_TIME_MS, DEFAULT_BATCH_MAXIMUM_BLOCK_TIME_MS);
        int userOperationsThreadPoolSize = getIntProperty(map, STRIMZI_USER_OPERATIONS_THREAD_POOL_SIZE, DEFAULT_USER_OPERATIONS_THREAD_POOL_SIZE);

        String kafkaBootstrapServers = DEFAULT_KAFKA_BOOTSTRAP_SERVERS;
        String kafkaBootstrapServersEnvVar = map.get(UserOperatorConfig.STRIMZI_KAFKA_BOOTSTRAP_SERVERS);
        if (kafkaBootstrapServersEnvVar != null && !kafkaBootstrapServersEnvVar.isEmpty()) {
            kafkaBootstrapServers = kafkaBootstrapServersEnvVar;
        }

        Labels labels;
        try {
            labels = Labels.fromString(map.get(STRIMZI_LABELS));
        } catch (Exception e)   {
            throw new InvalidConfigurationException("Failed to parse labels from " + STRIMZI_LABELS, e);
        }

        String caCertSecretName = map.get(UserOperatorConfig.STRIMZI_CA_CERT_SECRET_NAME);
        if (caCertSecretName == null || caCertSecretName.isEmpty()) {
            throw new InvalidConfigurationException(UserOperatorConfig.STRIMZI_CA_CERT_SECRET_NAME + " cannot be null");
        }

        String caKeySecretName = map.get(UserOperatorConfig.STRIMZI_CA_KEY_SECRET_NAME);
        if (caKeySecretName == null || caKeySecretName.isEmpty()) {
            throw new InvalidConfigurationException(UserOperatorConfig.STRIMZI_CA_KEY_SECRET_NAME + " cannot be null");
        }

        String clusterCaCertSecretName = map.get(UserOperatorConfig.STRIMZI_CLUSTER_CA_CERT_SECRET_NAME);

        String euoKeySecretName = map.get(UserOperatorConfig.STRIMZI_EO_KEY_SECRET_NAME);

        String caNamespace = map.get(UserOperatorConfig.STRIMZI_CA_NAMESPACE);
        if (caNamespace == null || caNamespace.isEmpty()) {
            caNamespace = namespace;
        }

        String secretPrefix = map.get(UserOperatorConfig.STRIMZI_SECRET_PREFIX);
        if (secretPrefix == null || secretPrefix.isEmpty()) {
            secretPrefix = DEFAULT_SECRET_PREFIX;
        }

        List<String> maintenanceWindows = parseMaintenanceTimeWindows(map.get(UserOperatorConfig.STRIMZI_MAINTENANCE_TIME_WINDOWS));

        Properties kafkaAdminClientConfiguration = parseKafkaAdminClientConfiguration(map.get(UserOperatorConfig.STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION));

        return new UserOperatorConfig(namespace, reconciliationInterval, kafkaBootstrapServers, labels,
                caCertSecretName, caKeySecretName, clusterCaCertSecretName, euoKeySecretName, caNamespace, secretPrefix,
                aclsAdminApiSupported, kraftEnabled, clientsCaValidityDays, clientsCaRenewalDays,
                scramPasswordLength, maintenanceWindows, kafkaAdminClientConfiguration, operationTimeout, workQueueSize,
                controllerThreadPoolSize, cacheRefresh, batchQueueSize, batchMaxBlockSize, batchMaxBlockTime,
                userOperationsThreadPoolSize);
    }

    /**
     * Parse the Kafka Admin Client configuration from the environment variable
     *
     * @param configuration The configuration from the environment variable. Null if no configuration is set.
     *
     * @return  The properties object with the configuration
     */
    /* test */ static Properties parseKafkaAdminClientConfiguration(String configuration) {
        Properties kafkaAdminClientConfiguration = new Properties();

        if (configuration != null)   {
            try {
                kafkaAdminClientConfiguration.load(new StringReader(configuration));
            } catch (IOException | IllegalArgumentException e)   {
                throw new InvalidConfigurationException("Failed to parse " + UserOperatorConfig.STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION + " configuration", e);
            }
        }

        return kafkaAdminClientConfiguration;
    }

    /**
     * @return  Clients CA validity in days
     */
    public int getClientsCaValidityDays() {
        return clientsCaValidityDays;
    }

    /**
     * @return  Clients CA renewal period in days
     */
    public int getClientsCaRenewalDays() {
        return clientsCaRenewalDays;
    }

    /**
     * Extracts the int type environment variable from the Map.
     *
     * @param map           Map with environment variables
     * @param name          Name of the environment variable which should be extracted
     * @param defaultVal    Default value which should be used when the environment variable is not set
     *
     * @return              The int value for the environment variable
     */
    private static int getIntProperty(Map<String, String> map, String name, int defaultVal) {
        String value = map.get(name);
        if (value != null) {
            return Integer.parseInt(value);
        } else {
            return defaultVal;
        }
    }

    /**
     * Extracts the long type environment variable from the Map.
     *
     * @param map           Map with environment variables
     * @param name          Name of the environment variable which should be extracted
     * @param defaultVal    Default value which should be used when the environment variable is not set
     *
     * @return              The int value for the environment variable
     */
    private static long getLongProperty(Map<String, String> map, String name, long defaultVal) {
        String value = map.get(name);
        if (value != null) {
            return Long.parseLong(value);
        } else {
            return defaultVal;
        }
    }

    /**
     * Extracts the boolean type environment variable from the Map.
     *
     * @param map           Map with environment variables
     * @param name          Name of the environment variable which should be extracted
     * @param defaultVal    Default value which should be used when the environment variable is not set
     *
     * @return              The boolean value for the environment variable
     */
    private static boolean getBooleanProperty(Map<String, String> map, String name, boolean defaultVal) {
        String value = map.get(name);
        if (value != null) {
            return Boolean.parseBoolean(value);
        } else {
            return defaultVal;
        }
    }

    /**
     * Parses the maintenance time windows from string containing zero or more Cron expressions into a list of individual
     * Cron expressions.
     *
     * @param maintenanceTimeWindows    String with semi-colon separate maintenance time windows (Cron expressions)
     *
     * @return  List of maintenance windows or null if there are no windows configured.
     */
    /* test */ static List<String> parseMaintenanceTimeWindows(String maintenanceTimeWindows) {
        List<String> windows = null;

        if (maintenanceTimeWindows != null && !maintenanceTimeWindows.isEmpty()) {
            windows = Arrays.asList(maintenanceTimeWindows.split(";"));
        }

        return windows;
    }

    /**
     * @return  namespace in which the operator runs and creates resources
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * @return  how many milliseconds the reconciliation runs
     */
    public long getReconciliationIntervalMs() {
        return reconciliationIntervalMs;
    }

    /**
     * @return  The labels which should be used as selector
     */
    public Labels getLabels() {
        return labels;
    }

    /**
     * @return  The name of the secret with the Client CA
     */
    public String getCaCertSecretName() {
        return caCertSecretName;
    }

    /**
     * @return  The name of the secret with the Client CA
     */
    public String getCaKeySecretName() {
        return caKeySecretName;
    }

    /**
     * @return  The name of the secret with the Cluster CA
     */
    public String getClusterCaCertSecretName() {
        return clusterCaCertSecretName;
    }

    /**
     * @return  The name of the secret with Entity User Operator key and certificate
     */
    public String getEuoKeySecretName() {
        return euoKeySecretName;
    }

    /**
     * @return  The namespace of the Client CA
     */
    public String getCaNamespace() {
        return caNamespace;
    }

    /**
     * @return  Kafka bootstrap servers list
     */
    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    /**
     * @return  The prefix that will be prepended to the name of the created kafka user secrets.
     */
    public String getSecretPrefix() {
        return secretPrefix;
    }

    /**
     * @return  The length used for Scram-Sha Password
     */
    public int getScramPasswordLength() {
        return scramPasswordLength;
    }

    /**
     * @return  Indicates whether the Kafka Admin API for managing ACLs is supported by the Kafka cluster or not
     */
    public boolean isAclsAdminApiSupported() {
        return aclsAdminApiSupported;
    }

    /**
     * @return  Indicates whether KRaft is used in the Kafka cluster or not. When it is used, some APIs might need to be
     * disabled or used differently.
     */
    public boolean isKraftEnabled() {
        return kraftEnabled;
    }

    /**
     * @return List of maintenance windows. Null if no maintenance windows were specified.
     */
    public List<String> getMaintenanceWindows() {
        return maintenanceWindows;
    }

    /**
     * @return Properties object with the user-supplied configuration for the Kafka Admin Client
     */
    public Properties getKafkaAdminClientConfiguration() {
        return kafkaAdminClientConfiguration;
    }

    /**
     * @return  The timeout after which operations are considered as failed
     */
    public long getOperationTimeoutMs() {
        return operationTimeoutMs;
    }

    /**
     * @return  The size of the User Controller work queue
     */
    public int getWorkQueueSize() {
        return workQueueSize;
    }

    /**
     * @return  Size of the pool of the controller threads used to reconcile the users
     */
    public int getControllerThreadPoolSize() {
        return controllerThreadPoolSize;
    }

    /**
     * @return  Refresh interval for the cache storing the resources from the Kafka Admin API
     */
    public long getCacheRefresh() {
        return cacheRefresh;
    }

    /**
     * @return  Maximal queue for requests when micro-batching the Kafka Admin API requests
     */
    public int getBatchQueueSize() {
        return batchQueueSize;
    }

    /**
     * @return  Maximal batch size for micro-batching the Kafka Admin API requests
     */
    public int getBatchMaxBlockSize() {
        return batchMaxBlockSize;
    }

    /**
     * @return  Maximal batch time for micro-batching the Kafka Admin API requests
     */
    public int getBatchMaxBlockTime() {
        return batchMaxBlockTime;
    }

    /**
     * @return Size of the thread pool for user operations done by KafkaUserOperator and the classes used by it
     */
    public int getUserOperationsThreadPoolSize() {
        return userOperationsThreadPoolSize;
    }

    @Override
    public String toString() {
        return "ClusterOperatorConfig(" +
                "namespace=" + namespace +
                ", reconciliationIntervalMs=" + reconciliationIntervalMs +
                ", kafkaBootstrapServers=" + kafkaBootstrapServers +
                ", labels=" + labels +
                ", caName=" + caCertSecretName +
                ", clusterCaCertSecretName=" + clusterCaCertSecretName +
                ", euoKeySecretName=" + euoKeySecretName +
                ", caNamespace=" + caNamespace +
                ", secretPrefix=" + secretPrefix +
                ", aclsAdminApiSupported=" + aclsAdminApiSupported +
                ", kraftEnabled=" + kraftEnabled +
                ", clientsCaValidityDays=" + clientsCaValidityDays +
                ", clientsCaRenewalDays=" + clientsCaRenewalDays +
                ", scramPasswordLength=" + scramPasswordLength +
                ", maintenanceWindows=" + maintenanceWindows +
                ", kafkaAdminClientConfiguration=" + kafkaAdminClientConfiguration +
                ", operationTimeoutMs=" + operationTimeoutMs +
                ", workQueueSize=" + workQueueSize +
                ", controllerThreadPoolSize=" + controllerThreadPoolSize +
                ", cacheRefresh=" + cacheRefresh +
                ", batchQueueSize=" + batchQueueSize +
                ", batchMaxBlockSize=" + batchMaxBlockSize +
                ", batchMaxBlockTime=" + batchMaxBlockTime +
                ", userOperationsThreadPoolSize=" + userOperationsThreadPoolSize +
                ")";
    }
}
