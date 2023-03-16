/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user;

import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.model.Labels;

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

/**
 * Cluster Operator configuration
 */
public class UserOperatorConfig {


    private static abstract class Type<T> {
        abstract T parse(String s);
    }

    /**
     * A java string
     */
    private static final Type<? extends String> STRING = new Type<>() {
        @Override
        public String parse(String s) {
            return s;
        }
    };

    /**
     * A java Long
     */
    private static final Type<? extends Long> LONG = new Type<>() {
        @Override
        public Long parse(String s) {
            return Long.parseLong(s);
        }
    };

    /**
     * A Java Integer
     */
    private static final Type<? extends Integer> INTEGER = new Type<>() {
        @Override
        Integer parse(String s) {
            return Integer.parseInt(s);
        }
    };

    /**
     * A Java Boolean
     */
    private static final Type<? extends Boolean> BOOLEAN = new Type<>() {
        @Override
        Boolean parse(String s) {
            return Boolean.parseBoolean(s);
        }
    };

    /**
     * A kubernetes selector.
     */
    private static final Type<? extends Labels> LABEL_PREDICATE = new Type<>() {
        @Override
        public Labels parse(String s) {
            return Labels.fromString(s);
        }
    };

    /** Wrapper class for all the configuration parameters */
    public static class ConfigParameter<T> {
        private final String key;
        private final String defaultValue;
        private final boolean required;

        /**
         * Getter
         * @return default value
         */
        public String getDefaultValue() {
            return defaultValue;
        }

        private final Type<? extends T> type;

        private ConfigParameter(String key, Type<? extends T> type, String defaultValue) {
            this.key = key;
            this.type = type;
            if (defaultValue != null) {
                type.parse(defaultValue);
            }
            this.defaultValue = defaultValue;
            this.required = false;
        }

        private ConfigParameter(String key, Type<? extends T> type, boolean required) {
            this.key = key;
            this.type = type;
            this.defaultValue = null;
            this.required = required;
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

    /** Namespace in which the operator will run and create resources */
    public static final ConfigParameter<String> NAMESPACE = new ConfigParameter<>(STRIMZI_NAMESPACE, STRING, true);
    /** Name of the secret containing the clients Certification Authority certificate. */
    public static final ConfigParameter<String> CA_CERT_SECRET_NAME = new ConfigParameter<>(STRIMZI_CA_CERT_SECRET_NAME, STRING, true);
    /** Name of the secret containing the cluster Certification Authority certificate. */
    public static final ConfigParameter<String> CLUSTER_CA_CERT_SECRET_NAME = new ConfigParameter<>(STRIMZI_CLUSTER_CA_CERT_SECRET_NAME, STRING, null);
    /** Namespace with the CA secret. */
    public static final ConfigParameter<String> CA_NAMESPACE = new ConfigParameter<>(STRIMZI_CA_NAMESPACE, STRING, null);
    /** The name of the secret containing the Entity User Operator key and certificate */
    public static final ConfigParameter<String> EO_KEY_SECRET_NAME = new ConfigParameter<>(STRIMZI_EO_KEY_SECRET_NAME, STRING, null);
    /** The name of the secret containing the clients Certification Authority key. */
    public static final ConfigParameter<String> CA_KEY_SECRET_NAME = new ConfigParameter<>(STRIMZI_CA_KEY_SECRET_NAME, STRING, true);
    /** Map with labels which should be used to find the KafkaUser resources. */
    public static final ConfigParameter<Labels> LABELS = new ConfigParameter<>(STRIMZI_LABELS, LABEL_PREDICATE, "");
    /** How many milliseconds between reconciliation runs. */
    public static final ConfigParameter<Long> RECONCILIATION_INTERVAL_MS = new ConfigParameter<>(STRIMZI_FULL_RECONCILIATION_INTERVAL_MS, LONG, "120000");
    /** Kafka bootstrap servers list */
    public static final ConfigParameter<String> KAFKA_BOOTSTRAP_SERVERS = new ConfigParameter<>(STRIMZI_KAFKA_BOOTSTRAP_SERVERS, STRING, "localhost:9091");
    /**
     * Configures the default prefix of user secrets created by the operator
     */
    public static final ConfigParameter<String> SECRET_PREFIX = new ConfigParameter<>(STRIMZI_SECRET_PREFIX, STRING, "");
    /** Number of days for which the certificate should be valid */
    public static  final ConfigParameter<Integer> CERTS_VALIDITY_DAYS = new ConfigParameter<>(STRIMZI_CLIENTS_CA_VALIDITY, INTEGER, "30");
    /** How long before the certificate expiration should the user certificate be renewed */
    public static  final ConfigParameter<Integer> CERTS_RENEWAL_DAYS = new ConfigParameter<>(STRIMZI_CLIENTS_CA_RENEWAL, INTEGER, "365");
    /** Length used for the Scram-Sha Password */
    public static final ConfigParameter<Integer> SCRAM_SHA_PASSWORD_LENGTH = new ConfigParameter<>(STRIMZI_SCRAM_SHA_PASSWORD_LENGTH, INTEGER, "32");
    /**
     * Indicates whether the Admin APi can be used to manage ACLs. Defaults to true for backwards compatibility reasons.
     */
    public static final ConfigParameter<Boolean> ACLS_ADMIN_API_SUPPORTED = new ConfigParameter<>(STRIMZI_ACLS_ADMIN_API_SUPPORTED, BOOLEAN, "true");
    /** Indicates whether KRaft is used in the Kafka cluster */
    public static final ConfigParameter<Boolean> KRAFT_ENABLED = new ConfigParameter<>(STRIMZI_KRAFT_ENABLED, BOOLEAN, "false");
    /** Timeout for internal operations specified in milliseconds */
    public static final ConfigParameter<Long> OPERATION_TIMEOUT_MS = new ConfigParameter<>(STRIMZI_OPERATION_TIMEOUT_MS, LONG, "300000");
    /** Indicates the size of the StrimziPodSetController work queue */
    public static final ConfigParameter<Integer> WORK_QUEUE_SIZE = new ConfigParameter<>(STRIMZI_WORK_QUEUE_SIZE, INTEGER, "1024");
    /** Size of the pool of the controller threads used to reconcile the users */
    public static final ConfigParameter<Integer> CONTROLLER_THREAD_POOL_SIZE = new ConfigParameter<>(STRIMZI_CONTROLLER_THREAD_POOL_SIZE, INTEGER, "50");
    /** Refresh interval for the cache storing the resources from the Kafka Admin API */
    public static final ConfigParameter<Long> CACHE_REFRESH_INTERVAL_MS = new ConfigParameter<>(STRIMZI_CACHE_REFRESH_INTERVAL_MS, LONG, "15000");
    /** Maximal queue for requests when micro-batching the Kafka Admin API requests */
    public static final ConfigParameter<Integer> BATCH_QUEUE_SIZE = new ConfigParameter<>(STRIMZI_BATCH_QUEUE_SIZE, INTEGER, "1024");
    /** Maximal batch size for micro-batching the Kafka Admin API requests */
    public  static final ConfigParameter<Integer> BATCH_MAXIMUM_BLOCK_SIZE = new ConfigParameter<>(STRIMZI_BATCH_MAXIMUM_BLOCK_SIZE, INTEGER, "100");
    /** Maximal batch time for micro-batching the Kafka Admin API requests */
    public static final ConfigParameter<Integer> BATCH_MAXIMUM_BLOCK_TIME_MS = new ConfigParameter<>(STRIMZI_BATCH_MAXIMUM_BLOCK_TIME_MS, INTEGER, "100");
    /** Size of the thread pool for user operations done by KafkaUserOperator and the classes used by it */
    public static final ConfigParameter<Integer> USER_OPERATIONS_THREAD_POOL_SIZE = new ConfigParameter<>(STRIMZI_USER_OPERATIONS_THREAD_POOL_SIZE, INTEGER, "4");
    /** Additional configuration for the Kafka Admin Client */
    public static final ConfigParameter<String> KAFKA_ADMIN_CLIENT_CONFIGURATION = new ConfigParameter<>(STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION, STRING, false);
    /** Lit of maintenance windows */
    public static final ConfigParameter<String> MAINTENANCE_TIME_WINDOWS = new ConfigParameter<>(STRIMZI_MAINTENANCE_TIME_WINDOWS, STRING, false);

    private static final Map<String, ConfigParameter<?>> CONFIG_VALUES = Map.ofEntries(
            Map.entry(STRIMZI_NAMESPACE, NAMESPACE),
            Map.entry(STRIMZI_CA_CERT_SECRET_NAME, CA_CERT_SECRET_NAME),
            Map.entry(STRIMZI_CA_KEY_SECRET_NAME, CA_KEY_SECRET_NAME),
            Map.entry(STRIMZI_CA_NAMESPACE, CA_NAMESPACE),
            Map.entry(STRIMZI_EO_KEY_SECRET_NAME, EO_KEY_SECRET_NAME),
            Map.entry(STRIMZI_CLUSTER_CA_CERT_SECRET_NAME, CLUSTER_CA_CERT_SECRET_NAME),
            Map.entry(STRIMZI_LABELS, LABELS),
            Map.entry(STRIMZI_FULL_RECONCILIATION_INTERVAL_MS, RECONCILIATION_INTERVAL_MS),
            Map.entry(STRIMZI_KAFKA_BOOTSTRAP_SERVERS, KAFKA_BOOTSTRAP_SERVERS),
            Map.entry(STRIMZI_CLIENTS_CA_RENEWAL, CERTS_RENEWAL_DAYS),
            Map.entry(STRIMZI_CLIENTS_CA_VALIDITY, CERTS_VALIDITY_DAYS),
            Map.entry(STRIMZI_SECRET_PREFIX, SECRET_PREFIX),
            Map.entry(STRIMZI_SCRAM_SHA_PASSWORD_LENGTH, SCRAM_SHA_PASSWORD_LENGTH),
            Map.entry(STRIMZI_ACLS_ADMIN_API_SUPPORTED, ACLS_ADMIN_API_SUPPORTED),
            Map.entry(STRIMZI_KRAFT_ENABLED, KRAFT_ENABLED),
            Map.entry(STRIMZI_OPERATION_TIMEOUT_MS, OPERATION_TIMEOUT_MS),
            Map.entry(STRIMZI_WORK_QUEUE_SIZE, WORK_QUEUE_SIZE),
            Map.entry(STRIMZI_CONTROLLER_THREAD_POOL_SIZE, CONTROLLER_THREAD_POOL_SIZE),
            Map.entry(STRIMZI_CACHE_REFRESH_INTERVAL_MS, CACHE_REFRESH_INTERVAL_MS),
            Map.entry(STRIMZI_BATCH_QUEUE_SIZE, BATCH_QUEUE_SIZE),
            Map.entry(STRIMZI_BATCH_MAXIMUM_BLOCK_SIZE, BATCH_MAXIMUM_BLOCK_SIZE),
            Map.entry(STRIMZI_BATCH_MAXIMUM_BLOCK_TIME_MS, BATCH_MAXIMUM_BLOCK_TIME_MS),
            Map.entry(STRIMZI_USER_OPERATIONS_THREAD_POOL_SIZE, USER_OPERATIONS_THREAD_POOL_SIZE),
            Map.entry(STRIMZI_MAINTENANCE_TIME_WINDOWS, MAINTENANCE_TIME_WINDOWS),
            Map.entry(STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION, KAFKA_ADMIN_CLIENT_CONFIGURATION)
    );

    private final Map<String, Object> map;

    /**
     * Constructor
     *
     * @param map  Map containing configurations and their respective values
     */
    @SuppressWarnings({"checkstyle:ParameterNumber"})
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
}
