/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user;

import io.strimzi.operator.common.config.ConfigParameter;
import io.strimzi.operator.common.model.Labels;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static io.strimzi.operator.common.config.ConfigParameterParser.BOOLEAN;
import static io.strimzi.operator.common.config.ConfigParameterParser.INTEGER;
import static io.strimzi.operator.common.config.ConfigParameterParser.LABEL_PREDICATE;
import static io.strimzi.operator.common.config.ConfigParameterParser.LONG;
import static io.strimzi.operator.common.config.ConfigParameterParser.NON_EMPTY_STRING;
import static io.strimzi.operator.common.config.ConfigParameterParser.PROPERTIES;
import static io.strimzi.operator.common.config.ConfigParameterParser.SEMICOLON_SEPARATED_LIST;
import static io.strimzi.operator.common.config.ConfigParameterParser.STRING;
import static io.strimzi.operator.common.config.ConfigParameterParser.strictlyPositive;

/**
 * Cluster Operator configuration
 */
public class UserOperatorConfig {

    private static final Map<String, ConfigParameter<?>> CONFIG_VALUES = new HashMap<>();

    /**
     * Namespace in which the operator will run and create resources
     */
    public static final ConfigParameter<String> NAMESPACE = new ConfigParameter<>("STRIMZI_NAMESPACE", NON_EMPTY_STRING, CONFIG_VALUES);
    /**
     * Name of the secret containing the clients Certification Authority certificate.
     */
    public static final ConfigParameter<String> CA_CERT_SECRET_NAME = new ConfigParameter<>("STRIMZI_CA_CERT_NAME", NON_EMPTY_STRING, CONFIG_VALUES);
    /**
     * Name of the secret containing the cluster Certification Authority certificate.
     */
    public static final ConfigParameter<String> CLUSTER_CA_CERT_SECRET_NAME = new ConfigParameter<>("STRIMZI_CLUSTER_CA_CERT_SECRET_NAME", STRING, null, CONFIG_VALUES);
    /**
     * Namespace with the CA secret.
     */
    public static final ConfigParameter<String> CA_NAMESPACE = new ConfigParameter<>("STRIMZI_CA_NAMESPACE", STRING, null, CONFIG_VALUES);
    /**
     * The name of the secret containing the Entity User Operator key and certificate
     */
    public static final ConfigParameter<String> EO_KEY_SECRET_NAME = new ConfigParameter<>("STRIMZI_EO_KEY_SECRET_NAME", STRING, null, CONFIG_VALUES);
    /**
     * The name of the secret containing the clients Certification Authority key.
     */
    public static final ConfigParameter<String> CA_KEY_SECRET_NAME = new ConfigParameter<>("STRIMZI_CA_KEY_NAME", NON_EMPTY_STRING, CONFIG_VALUES);
    /**
     * Map with labels which should be used to find the KafkaUser resources.
     */
    public static final ConfigParameter<Labels> LABELS = new ConfigParameter<>("STRIMZI_LABELS", LABEL_PREDICATE, "", CONFIG_VALUES);
    /**
     * How many milliseconds between reconciliation runs.
     */
    public static final ConfigParameter<Long> RECONCILIATION_INTERVAL_MS = new ConfigParameter<>("STRIMZI_FULL_RECONCILIATION_INTERVAL_MS", LONG, "120000", CONFIG_VALUES);
    /**
     * Kafka bootstrap servers list
     */
    public static final ConfigParameter<String> KAFKA_BOOTSTRAP_SERVERS = new ConfigParameter<>("STRIMZI_KAFKA_BOOTSTRAP_SERVERS", STRING, "localhost:9091", CONFIG_VALUES);
    /**
     * Configures the default prefix of user secrets created by the operator
     */
    public static final ConfigParameter<String> SECRET_PREFIX = new ConfigParameter<>("STRIMZI_SECRET_PREFIX", STRING, "", CONFIG_VALUES);
    /**
     * Number of days for which the certificate should be valid
     */
    public static final ConfigParameter<Integer> CERTS_VALIDITY_DAYS = new ConfigParameter<>("STRIMZI_CA_VALIDITY", strictlyPositive(INTEGER), "365", CONFIG_VALUES);
    /**
     * How long before the certificate expiration should the user certificate be renewed
     */
    public static final ConfigParameter<Integer> CERTS_RENEWAL_DAYS = new ConfigParameter<>("STRIMZI_CA_RENEWAL", strictlyPositive(INTEGER), "30", CONFIG_VALUES);
    /**
     * Length used for the Scram-Sha Password
     */
    public static final ConfigParameter<Integer> SCRAM_SHA_PASSWORD_LENGTH = new ConfigParameter<>("STRIMZI_SCRAM_SHA_PASSWORD_LENGTH", strictlyPositive(INTEGER), "32",  CONFIG_VALUES);
    /**
     * Indicates whether the Admin APi can be used to manage ACLs. Defaults to true for backwards compatibility reasons.
     */
    public static final ConfigParameter<Boolean> ACLS_ADMIN_API_SUPPORTED = new ConfigParameter<>("STRIMZI_ACLS_ADMIN_API_SUPPORTED", BOOLEAN, "true", CONFIG_VALUES);
    /**
     * Timeout for internal operations specified in milliseconds
     */
    public static final ConfigParameter<Long> OPERATION_TIMEOUT_MS = new ConfigParameter<>("STRIMZI_OPERATION_TIMEOUT_MS", LONG, "300000", CONFIG_VALUES);
    /**
     * Indicates the size of the StrimziPodSetController work queue
     */
    public static final ConfigParameter<Integer> WORK_QUEUE_SIZE = new ConfigParameter<>("STRIMZI_WORK_QUEUE_SIZE", INTEGER, "1024", CONFIG_VALUES);
    /**
     * Size of the pool of the controller threads used to reconcile the users
     */
    public static final ConfigParameter<Integer> CONTROLLER_THREAD_POOL_SIZE = new ConfigParameter<>("STRIMZI_CONTROLLER_THREAD_POOL_SIZE", INTEGER, "50", CONFIG_VALUES);
    /**
     * Refresh interval for the cache storing the resources from the Kafka Admin API
     */
    public static final ConfigParameter<Long> CACHE_REFRESH_INTERVAL_MS = new ConfigParameter<>("STRIMZI_CACHE_REFRESH_INTERVAL_MS", LONG, "15000", CONFIG_VALUES);
    /**
     * Maximal queue for requests when micro-batching the Kafka Admin API requests
     */
    public static final ConfigParameter<Integer> BATCH_QUEUE_SIZE = new ConfigParameter<>("STRIMZI_BATCH_QUEUE_SIZE", INTEGER, "1024", CONFIG_VALUES);
    /**
     * Maximal batch size for micro-batching the Kafka Admin API requests
     */
    public static final ConfigParameter<Integer> BATCH_MAXIMUM_BLOCK_SIZE = new ConfigParameter<>("STRIMZI_BATCH_MAXIMUM_BLOCK_SIZE", INTEGER, "100", CONFIG_VALUES);
    /**
     * Maximal batch time for micro-batching the Kafka Admin API requests
     */
    public static final ConfigParameter<Integer> BATCH_MAXIMUM_BLOCK_TIME_MS = new ConfigParameter<>("STRIMZI_BATCH_MAXIMUM_BLOCK_TIME_MS", strictlyPositive(INTEGER), "100", CONFIG_VALUES);
    /**
     * Size of the thread pool for user operations done by KafkaUserOperator and the classes used by it
     */
    public static final ConfigParameter<Integer> USER_OPERATIONS_THREAD_POOL_SIZE = new ConfigParameter<>("STRIMZI_USER_OPERATIONS_THREAD_POOL_SIZE", INTEGER, "4", CONFIG_VALUES);
    /**
     * Additional configuration for the Kafka Admin Client
     */
    public static final ConfigParameter<Properties> KAFKA_ADMIN_CLIENT_CONFIGURATION = new ConfigParameter<>("STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION", PROPERTIES, "", CONFIG_VALUES);
    /**
     * Lit of maintenance windows
     */
    public static final ConfigParameter<List<String>> MAINTENANCE_TIME_WINDOWS = new ConfigParameter<>("STRIMZI_MAINTENANCE_TIME_WINDOWS", SEMICOLON_SEPARATED_LIST, "", CONFIG_VALUES);

    private final Map<String, Object> map;

    /**
     * Constructor
     *
     * @param map Map containing configurations and their respective values
     */
    private UserOperatorConfig(Map<String, Object> map) {
        this.map = map;
    }

    /**
     * Creates the `UserOperatorConfig` object by calling the constructor.
     *
     * @param map Map containing config parameters entered by user
     * @return UserOperatorConfig object
     */
    public static UserOperatorConfig buildFromMap(Map<String, String> map) {
        Map<String, String> envMap = new HashMap<>(map);
        envMap.keySet().retainAll(UserOperatorConfig.keyNames());

        Map<String, Object> generatedMap = ConfigParameter.define(envMap, CONFIG_VALUES);

        return new UserOperatorConfig(generatedMap);
    }

    /**
     * @return Set of configuration key/names
     */
    public static Set<String> keyNames() {
        return Collections.unmodifiableSet(CONFIG_VALUES.keySet());
    }

    /**
     * Gets the configuration value corresponding to the key
     * @param <T>      Type of value
     * @param value    Instance of Config Parameter class
     * @return         Configuration value w.r.t to the key
     */
    @SuppressWarnings("unchecked")
    public <T> T get(ConfigParameter<T> value) {
        return (T) this.map.get(value.key());
    }

    /**
     * User Operator Configuration Builder class
     */
    protected static class UserOperatorConfigBuilder {

        private final Map<String, Object> map;

        protected  UserOperatorConfigBuilder(UserOperatorConfig config) {
            this.map = config.map;
        }

        protected UserOperatorConfigBuilder with(String key, String value) {
            this.map.put(key, CONFIG_VALUES.get(key).type().parse(value));
            return this;
        }

        protected UserOperatorConfig build() {
            return new UserOperatorConfig(this.map);
        }
    }

    /**
     * @return  namespace in which the operator runs and creates resources
     */
    public String getNamespace() {
        return get(NAMESPACE);
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
     * @return The namespace of the Client CA if not null or empty, else it will return namespace
     */
    public String getCaNamespaceOrNamespace() {
        if (get(CA_NAMESPACE) == null || get(CA_NAMESPACE).isEmpty()) {
            return getNamespace();
        } else {
            return get(CA_NAMESPACE);
        }
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
     * @return List of maintenance windows. Null if no maintenance windows were specified.
     */
    public List<String> getMaintenanceWindows() {
        return get(MAINTENANCE_TIME_WINDOWS);
    }

    /**
     * @return Properties object with the user-supplied configuration for the Kafka Admin Client
     */
    public Properties getKafkaAdminClientConfiguration() {
        return get(KAFKA_ADMIN_CLIENT_CONFIGURATION);
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
                "\n\tnamespace='" + getNamespace() + '\'' +
                "\n\treconciliationIntervalMs=" + getReconciliationIntervalMs() +
                "\n\tkafkaBootstrapServers='" + getKafkaBootstrapServers() + '\'' +
                "\n\tlabels=`" + getLabels() + '\'' +
                "\n\tcaCertSecretName='" + getCaCertSecretName() + '\'' +
                "\n\tcaKeySecretName='" + getCaKeySecretName() + '\'' +
                "\n\tclusterCaCertSecretName='" + getClusterCaCertSecretName() + '\'' +
                "\n\teuoKeySecretName='" + getEuoKeySecretName() + '\'' +
                "\n\tcaNamespace='" + getCaNamespaceOrNamespace() + '\'' +
                "\n\tsecretPrefix='" + getSecretPrefix() + '\'' +
                "\n\tclientsCaValidityDays=" + getClientsCaValidityDays() +
                "\n\tclientsCaRenewalDays=" + getClientsCaRenewalDays() +
                "\n\taclsAdminApiSupported=" + isAclsAdminApiSupported() +
                "\n\tscramPasswordLength=" + getScramPasswordLength() +
                "\n\tmaintenanceWindows=`" + getMaintenanceWindows() + '\'' +
                "\n\tkafkaAdminClientConfiguration=`" + getKafkaAdminClientConfiguration() + '\'' +
                "\n\toperationTimeoutMs=" + getOperationTimeoutMs() +
                "\n\tworkQueueSize=" + getWorkQueueSize() +
                "\n\tcontrollerThreadPoolSize=" + getControllerThreadPoolSize() +
                "\n\tcacheRefresh=" + getCacheRefresh() +
                "\n\tbatchQueueSize=" + getBatchQueueSize() +
                "\n\tbatchMaxBlockSize=" + getBatchMaxBlockSize() +
                "\n\tbatchMaxBlockTime=" + getBatchMaxBlockTime() +
                "\n\tuserOperationsThreadPoolSize=" + getUserOperationsThreadPoolSize() +
                '}';
    }
}
