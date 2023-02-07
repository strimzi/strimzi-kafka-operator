/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user;

import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.AbstractConfig;


import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.HashMap;
import java.util.Objects;
import java.util.Set;
import java.util.Collections;

import static io.strimzi.operator.common.operator.resource.AbstractConfig.BOOLEAN;
import static io.strimzi.operator.common.operator.resource.AbstractConfig.LABEL_PREDICATE;
import static io.strimzi.operator.common.operator.resource.AbstractConfig.STRING;
import static io.strimzi.operator.common.operator.resource.AbstractConfig.INTEGER;
import static io.strimzi.operator.common.operator.resource.AbstractConfig.LONG;

/**
 * Cluster Operator configuration
 */
public class UserOperatorConfig {

    private static final Map<String, ConfigParameter<?>> CONFIG_VALUES = new HashMap<>();

    /**
     * Models a configuration parameter, identified by a unique key, which may be required, and if not may have a default value.
     * Optional parameters without a default value implicitly have a null default.
     * The key is also the name of the environment variable from which the value may be read, when it is read from the environment.
     *
     * @param key Configuration parameter name/key
     * @param <T> Type of object
     * @param type type of the default value
     * @param defaultValue default value of the configuration parameter
     * @param required  If the value is required or not
     */
    public record ConfigParameter<T>(String key, AbstractConfig<? extends T> type, String defaultValue, boolean required) {
        /**
         * Contructor
         * @param key Configuration parameter name/key
         * @param type type of the default value
         * @param defaultValue default value of the configuration parameter
         * @param required  If the value is required or not
         */
        public ConfigParameter {
            CONFIG_VALUES.put(key, this);
        }
    }

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

    /**
     * Namespace in which the operator will run and create resources
     */
    public static final ConfigParameter<String> NAMESPACE = new ConfigParameter<>(STRIMZI_NAMESPACE, STRING, "", true);
    /**
     * Name of the secret containing the clients Certification Authority certificate.
     */
    public static final ConfigParameter<String> CA_CERT_SECRET_NAME = new ConfigParameter<>(STRIMZI_CA_CERT_SECRET_NAME, STRING, "", true);
    /**
     * Name of the secret containing the cluster Certification Authority certificate.
     */
    public static final ConfigParameter<String> CLUSTER_CA_CERT_SECRET_NAME = new ConfigParameter<>(STRIMZI_CLUSTER_CA_CERT_SECRET_NAME, STRING, "", true);
    /**
     * Namespace with the CA secret.
     */
    public static final ConfigParameter<String> CA_NAMESPACE = new ConfigParameter<>(STRIMZI_CA_NAMESPACE, STRING, "", true);
    /**
     * The name of the secret containing the Entity User Operator key and certificate
     */
    public static final ConfigParameter<String> EO_KEY_SECRET_NAME = new ConfigParameter<>(STRIMZI_EO_KEY_SECRET_NAME, STRING, "", true);
    /**
     * The name of the secret containing the clients Certification Authority key.
     */
    public static final ConfigParameter<String> CA_KEY_SECRET_NAME = new ConfigParameter<>(STRIMZI_CA_KEY_SECRET_NAME, STRING, "", true);
    /**
     * Map with labels which should be used to find the KafkaUser resources.
     */
    public static final ConfigParameter<Labels> LABELS = new ConfigParameter<>(STRIMZI_LABELS, LABEL_PREDICATE, "", true);
    /**
     * How many milliseconds between reconciliation runs.
     */
    public static final ConfigParameter<Long> RECONCILIATION_INTERVAL_MS = new ConfigParameter<>(STRIMZI_FULL_RECONCILIATION_INTERVAL_MS, LONG, "120000", true);
    /**
     * Kafka bootstrap servers list
     */
    public static final ConfigParameter<String> KAFKA_BOOTSTRAP_SERVERS = new ConfigParameter<>(STRIMZI_KAFKA_BOOTSTRAP_SERVERS, STRING, "localhost:9091", true);
    /**
     * Configures the default prefix of user secrets created by the operator
     */
    public static final ConfigParameter<String> SECRET_PREFIX = new ConfigParameter<>(STRIMZI_SECRET_PREFIX, STRING, "", true);
    /**
     * Number of days for which the certificate should be valid
     */
    public static final ConfigParameter<Integer> CERTS_VALIDITY_DAYS = new ConfigParameter<>(STRIMZI_CLIENTS_CA_VALIDITY, INTEGER, "30", true);
    /**
     * How long before the certificate expiration should the user certificate be renewed
     */
    public static final ConfigParameter<Integer> CERTS_RENEWAL_DAYS = new ConfigParameter<>(STRIMZI_CLIENTS_CA_RENEWAL, INTEGER, "365", true);
    /**
     * Length used for the Scram-Sha Password
     */
    public static final ConfigParameter<Integer> SCRAM_SHA_PASSWORD_LENGTH = new ConfigParameter<>(STRIMZI_SCRAM_SHA_PASSWORD_LENGTH, INTEGER, "32", true);
    /**
     * Indicates whether the Admin APi can be used to manage ACLs. Defaults to true for backwards compatibility reasons.
     */
    public static final ConfigParameter<Boolean> ACLS_ADMIN_API_SUPPORTED = new ConfigParameter<>(STRIMZI_ACLS_ADMIN_API_SUPPORTED, BOOLEAN, "true", true);
    /**
     * Indicates whether KRaft is used in the Kafka cluster
     */
    public static final ConfigParameter<Boolean> KRAFT_ENABLED = new ConfigParameter<>(STRIMZI_KRAFT_ENABLED, BOOLEAN, "false", true);
    /**
     * Timeout for internal operations specified in milliseconds
     */
    public static final ConfigParameter<Long> OPERATION_TIMEOUT_MS = new ConfigParameter<>(STRIMZI_OPERATION_TIMEOUT_MS, LONG, "300000", true);
    /**
     * Indicates the size of the StrimziPodSetController work queue
     */
    public static final ConfigParameter<Integer> WORK_QUEUE_SIZE = new ConfigParameter<>(STRIMZI_WORK_QUEUE_SIZE, INTEGER, "1024", true);
    /**
     * Size of the pool of the controller threads used to reconcile the users
     */
    public static final ConfigParameter<Integer> CONTROLLER_THREAD_POOL_SIZE = new ConfigParameter<>(STRIMZI_CONTROLLER_THREAD_POOL_SIZE, INTEGER, "50", true);
    /**
     * Refresh interval for the cache storing the resources from the Kafka Admin API
     */
    public static final ConfigParameter<Long> CACHE_REFRESH_INTERVAL_MS = new ConfigParameter<>(STRIMZI_CACHE_REFRESH_INTERVAL_MS, LONG, "15000", true);
    /**
     * Maximal queue for requests when micro-batching the Kafka Admin API requests
     */
    public static final ConfigParameter<Integer> BATCH_QUEUE_SIZE = new ConfigParameter<>(STRIMZI_BATCH_QUEUE_SIZE, INTEGER, "1024", true);
    /**
     * Maximal batch size for micro-batching the Kafka Admin API requests
     */
    public static final ConfigParameter<Integer> BATCH_MAXIMUM_BLOCK_SIZE = new ConfigParameter<>(STRIMZI_BATCH_MAXIMUM_BLOCK_SIZE, INTEGER, "100", true);
    /**
     * Maximal batch time for micro-batching the Kafka Admin API requests
     */
    public static final ConfigParameter<Integer> BATCH_MAXIMUM_BLOCK_TIME_MS = new ConfigParameter<>(STRIMZI_BATCH_MAXIMUM_BLOCK_TIME_MS, INTEGER, "100", true);
    /**
     * Size of the thread pool for user operations done by KafkaUserOperator and the classes used by it
     */
    public static final ConfigParameter<Integer> USER_OPERATIONS_THREAD_POOL_SIZE = new ConfigParameter<>(STRIMZI_USER_OPERATIONS_THREAD_POOL_SIZE, INTEGER, "4", true);
    /**
     * Additional configuration for the Kafka Admin Client
     */
    public static final ConfigParameter<String> KAFKA_ADMIN_CLIENT_CONFIGURATION = new ConfigParameter<>(STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION, STRING, "", false);
    /**
     * Lit of maintenance windows
     */
    public static final ConfigParameter<String> MAINTENANCE_TIME_WINDOWS = new ConfigParameter<>(STRIMZI_MAINTENANCE_TIME_WINDOWS, STRING, null, false);

    protected Map<String, Object> map;

    /**
     * Constructor
     *
     * @param map Map containing configurations and their respective values
     */
    public UserOperatorConfig(Map<String, String> map) {
        this.map = new HashMap<>(map.size());
        for (Map.Entry<String, String> entry : map.entrySet()) {
            final ConfigParameter<?> configValue = CONFIG_VALUES.get(entry.getKey());
            if (configValue == null) {
                throw new IllegalArgumentException("Unknown config key " + entry.getKey());
            }
            this.map.put(configValue.key, get(map, configValue));
        }

        // now add all those config (with default value) that weren't in the given map
        Map<String, ConfigParameter<?>> x = new HashMap<>(CONFIG_VALUES);
        x.keySet().removeAll(map.keySet());
        for (ConfigParameter<?> value : x.values()) {
            this.map.put(value.key, get(map, value));
        }

        if (this.map.get(STRIMZI_CA_NAMESPACE) == null || this.map.get(STRIMZI_CA_NAMESPACE).equals("")) {
            this.map.put(STRIMZI_CA_NAMESPACE, this.map.get(STRIMZI_NAMESPACE));
        }
    }

    /**
     * Parse the Kafka Admin Client configuration from the environment variable
     *
     * @param configuration The configuration from the environment variable. Null if no configuration is set.
     * @return The properties object with the configuration
     */
    /* test */
    public static Properties parseKafkaAdminClientConfiguration(String configuration) {
        Properties kafkaAdminClientConfiguration = new Properties();

        if (configuration != null) {
            try {
                kafkaAdminClientConfiguration.load(new StringReader(configuration));
            } catch (IOException | IllegalArgumentException e) {
                throw new InvalidConfigurationException("Failed to parse " + UserOperatorConfig.STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION + " configuration", e);
            }
        }

        return kafkaAdminClientConfiguration;
    }

    /**
     * Parses the maintenance time windows from string containing zero or more Cron expressions into a list of individual
     * Cron expressions.
     *
     * @param maintenanceTimeWindows String with semi-colon separate maintenance time windows (Cron expressions)
     * @return List of maintenance windows or null if there are no windows configured.
     */
    /* test */
    public static List<String> parseMaintenanceTimeWindows(String maintenanceTimeWindows) {
        List<String> windows = null;

        if (maintenanceTimeWindows != null && !maintenanceTimeWindows.isEmpty()) {
            windows = Arrays.asList(maintenanceTimeWindows.split(";"));
        }

        return windows;
    }

    /**
     * @return Set of configuration key/names
     */
    public static Set<String> keyNames() {
        return Collections.unmodifiableSet(CONFIG_VALUES.keySet());
    }

    /**
     * Checks if the configuration values are known or not.
     *
     * @param map   The map containing configuration values
     * @param value The configuration value that need to be checked
     * @return The configuration value
     */
    private <T> T get(Map<String, String> map, ConfigParameter<T> value) {
        if (!CONFIG_VALUES.containsKey(value.key)) {
            throw new InvalidConfigurationException("Unknown config value: " + value.key + " probably needs to be added to Config.CONFIG_VALUES");
        }

        final String s = map.getOrDefault(value.key, value.defaultValue);
        if (s != null) {
            if ((value.key.equals(STRIMZI_NAMESPACE) || value.key.equals(STRIMZI_CA_CERT_SECRET_NAME) || value.key.equals(STRIMZI_CA_KEY_SECRET_NAME)) && s.equals("")) {
                System.out.println(s);
                throw new InvalidConfigurationException("Config value: " + value.key + " is mandatory");
            }
            return value.type.parse(s);
        } else {
            if (value.required) {
                throw new InvalidConfigurationException("Config value: " + value.key + " is mandatory");
            }
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    protected <T> T get(ConfigParameter<T> value, T defaultValue) {
        return (T) this.map.getOrDefault(value.key, defaultValue);
    }

    /**
     * Gets the configuration value corresponding to the key
     * @param <T> type of value
     * @param value instance of Value class
     * @return configuration value w.r.t to the key
     */
    @SuppressWarnings("unchecked")
    public  <T> T get(ConfigParameter<T> value) {
        return (T) this.map.get(value.key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserOperatorConfig config = (UserOperatorConfig) o;
        return Objects.equals(map, config.map);
    }

    @Override
    public int hashCode() {
        return Objects.hash(map);
    }

    /**
     * @return  namespace in which the operator runs and creates resources
     */
    public String getNamespace() {
        return (String) this.map.get(NAMESPACE.key());
    }

    /**
     * @return how many milliseconds the reconciliation runs
     */

    public long getReconciliationIntervalMs() {
        return get(RECONCILIATION_INTERVAL_MS);
    }

    /**
     * @return The labels which should be used as selector
     */
    public Labels getLabels() {
        return get(LABELS);
    }

    /**
     * @return The name of the secret with the Client CA
     */
    public String getCaCertSecretName() {
        return get(CA_CERT_SECRET_NAME);
    }

    /**
     * @return The name of the secret with the Client CA
     */
    public String getCaKeySecretName() {
        return get(CA_KEY_SECRET_NAME);
    }

    /**
     * @return The name of the secret with the Cluster CA
     */
    public String getClusterCaCertSecretName() {
        return get(CLUSTER_CA_CERT_SECRET_NAME);
    }

    /**
     * @return The name of the secret with Entity User Operator key and certificate
     */
    public String getEuoKeySecretName() {
        return get(EO_KEY_SECRET_NAME);
    }

    /**
     * @return The namespace of the Client CA
     */
    public String getCaNamespace() {
        return get(CA_NAMESPACE);
    }

    /**
     * @return Kafka bootstrap servers list
     */
    public String getKafkaBootstrapServers() {
        return get(KAFKA_BOOTSTRAP_SERVERS);
    }

    /**
     * @return The prefix that will be prepended to the name of the created kafka user secrets.
     */
    public String getSecretPrefix() {
        return get(SECRET_PREFIX);
    }

    /**
     * @return The length used for Scram-Sha Password
     */
    public int getScramPasswordLength() {
        return get(SCRAM_SHA_PASSWORD_LENGTH);
    }

    /**
     * @return Indicates whether the Kafka Admin API for managing ACLs is supported by the Kafka cluster or not
     */
    public boolean isAclsAdminApiSupported() {
        return get(ACLS_ADMIN_API_SUPPORTED);
    }

    /**
     * @return Indicates whether KRaft is used in the Kafka cluster or not. When it is used, some APIs might need to be
     * disabled or used differently.
     */
    public boolean isKraftEnabled() {
        return get(KRAFT_ENABLED);
    }

    /**
     * @return List of maintenance windows. Null if no maintenance windows were specified.
     */
    public List<String> getMaintenanceWindows() {
        return parseMaintenanceTimeWindows(get(MAINTENANCE_TIME_WINDOWS));
    }

    /**
     * @return Properties object with the user-supplied configuration for the Kafka Admin Client
     */
    public Properties getKafkaAdminClientConfiguration() {
        return parseKafkaAdminClientConfiguration(get(KAFKA_ADMIN_CLIENT_CONFIGURATION));
    }

    /**
     * @return The timeout after which operations are considered as failed
     */
    public long getOperationTimeoutMs() {
        return get(OPERATION_TIMEOUT_MS);
    }

    /**
     * @return  The size of the User Controller work queue
     */
    public int getWorkQueueSize() {
        return get(WORK_QUEUE_SIZE);
    }

    /**
     * @return  Size of the pool of the controller threads used to reconcile the users
     */
    public int getControllerThreadPoolSize() {
        return get(CONTROLLER_THREAD_POOL_SIZE);
    }

    /**
     * @return  Refresh interval for the cache storing the resources from the Kafka Admin API
     */
    public long getCacheRefresh() {
        return get(CACHE_REFRESH_INTERVAL_MS);
    }

    /**
     * @return  Maximal queue for requests when micro-batching the Kafka Admin API requests
     */
    public int getBatchQueueSize() {
        return get(BATCH_QUEUE_SIZE);
    }

    /**
     * @return  Maximal batch size for micro-batching the Kafka Admin API requests
     */
    public int getBatchMaxBlockSize() {
        return get(BATCH_MAXIMUM_BLOCK_SIZE);
    }

    /**
     * @return  Maximal batch time for micro-batching the Kafka Admin API requests
     */
    public int getBatchMaxBlockTime() {
        return get(BATCH_MAXIMUM_BLOCK_TIME_MS);
    }

    /**
     * @return Size of the thread pool for user operations done by KafkaUserOperator and the classes used by it
     */
    public int getUserOperationsThreadPoolSize() {
        return get(USER_OPERATIONS_THREAD_POOL_SIZE);
    }

    /**
     * @return The number of certificates validity days.
     */
    public int getClientsCaValidityDays() {
        return get(CERTS_VALIDITY_DAYS);
    }

    /**
     * @return The number of certificates renewal days.
     */
    public int getClientsCaRenewalDays() {
        return get(CERTS_RENEWAL_DAYS);
    }


    @Override
    public String toString() {
        return "UserOperatorBuilderConfig{" +
                "namespace='" + getNamespace() + '\'' +
                ", reconciliationIntervalMs=" + getReconciliationIntervalMs() +
                ", kafkaBootstrapServers='" + getKafkaBootstrapServers() + '\'' +
                ", labels=" + getLabels() +
                ", caCertSecretName='" + getCaCertSecretName() + '\'' +
                ", caKeySecretName='" + getCaKeySecretName() + '\'' +
                ", clusterCaCertSecretName='" + getClusterCaCertSecretName() + '\'' +
                ", euoKeySecretName='" + getEuoKeySecretName() + '\'' +
                ", caNamespace='" + getCaNamespace() + '\'' +
                ", secretPrefix='" + getSecretPrefix() + '\'' +
                ", clientsCaValidityDays=" + getClientsCaValidityDays() +
                ", clientsCaRenewalDays=" + getClientsCaRenewalDays() +
                ", aclsAdminApiSupported=" + isAclsAdminApiSupported() +
                ", kraftEnabled=" + isKraftEnabled() +
                ", scramPasswordLength=" + getScramPasswordLength() +
                ", maintenanceWindows=" + getMaintenanceWindows() +
                ", kafkaAdminClientConfiguration=" + getKafkaAdminClientConfiguration() +
                ", operationTimeoutMs=" + getOperationTimeoutMs() +
                ", workQueueSize=" + getWorkQueueSize() +
                ", controllerThreadPoolSize=" + getControllerThreadPoolSize() +
                ", cacheRefresh=" + getCacheRefresh() +
                ", batchQueueSize=" + getBatchQueueSize() +
                ", batchMaxBlockSize=" + getBatchMaxBlockSize() +
                ", batchMaxBlockTime=" + getBatchMaxBlockTime() +
                ", userOperationsThreadPoolSize=" + getUserOperationsThreadPoolSize() +
                '}';
    }
}
