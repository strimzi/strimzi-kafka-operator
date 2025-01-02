/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.config.ConfigParameter;
import io.strimzi.operator.common.config.ConfigParameterParser;
import io.strimzi.operator.common.featuregates.FeatureGates;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlApiProperties;
import io.strimzi.operator.topic.cruisecontrol.CruiseControlClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Topic Operator configuration.
 */
public class TopicOperatorConfig {
    private final static ReconciliationLogger LOGGER = ReconciliationLogger.create(TopicOperatorConfig.class);
    private static final Map<String, ConfigParameter<?>> CONFIG_VALUES = new HashMap<>();
    private static final TypeReference<HashMap<String, String>> STRING_HASH_MAP_TYPE_REFERENCE = new TypeReference<>() { };

    /** Namespace in which the operator will run and create resources. */
    public static final ConfigParameter<String> NAMESPACE = new ConfigParameter<>("STRIMZI_NAMESPACE", ConfigParameterParser.NON_EMPTY_STRING, CONFIG_VALUES);
    /** Labels used to filter the custom resources seen by the cluster operator. */
    public static final ConfigParameter<Labels> RESOURCE_LABELS = new ConfigParameter<>("STRIMZI_RESOURCE_LABELS", ConfigParameterParser.LABEL_PREDICATE, "", CONFIG_VALUES);
    /** Kafka bootstrap address for the target Kafka cluster used by the internal admin client. */
    public static final ConfigParameter<String> BOOTSTRAP_SERVERS = new ConfigParameter<>("STRIMZI_KAFKA_BOOTSTRAP_SERVERS", ConfigParameterParser.NON_EMPTY_STRING, CONFIG_VALUES);
    /** Kafka client ID used by the internal admin client. */
    public static final ConfigParameter<String> CLIENT_ID = new ConfigParameter<>("STRIMZI_CLIENT_ID", ConfigParameterParser.NON_EMPTY_STRING, "strimzi-topic-operator-" + UUID.randomUUID(), CONFIG_VALUES);
    /** Periodic reconciliation interval in milliseconds. */
    public static final ConfigParameter<Long> FULL_RECONCILIATION_INTERVAL_MS = new ConfigParameter<>("STRIMZI_FULL_RECONCILIATION_INTERVAL_MS", ConfigParameterParser.strictlyPositive(ConfigParameterParser.LONG), "120000", CONFIG_VALUES);
    /** TLS: whether to enable configuration. */
    public static final ConfigParameter<Boolean> TLS_ENABLED = new ConfigParameter<>("STRIMZI_TLS_ENABLED", ConfigParameterParser.BOOLEAN, "false", CONFIG_VALUES);
    /** TLS: truststore location. */
    public static final ConfigParameter<String> TRUSTSTORE_LOCATION = new ConfigParameter<>("STRIMZI_TRUSTSTORE_LOCATION", ConfigParameterParser.STRING, "", CONFIG_VALUES);
    /** TLS: truststore password. */
    public static final ConfigParameter<String> TRUSTSTORE_PASSWORD = new ConfigParameter<>("STRIMZI_TRUSTSTORE_PASSWORD", ConfigParameterParser.STRING, "", CONFIG_VALUES);
    /** TLS: keystore location. */
    public static final ConfigParameter<String> KEYSTORE_LOCATION = new ConfigParameter<>("STRIMZI_KEYSTORE_LOCATION", ConfigParameterParser.STRING, "", CONFIG_VALUES);
    /** TLS: keystore location. */
    public static final ConfigParameter<String> KEYSTORE_PASSWORD = new ConfigParameter<>("STRIMZI_KEYSTORE_PASSWORD", ConfigParameterParser.STRING, "", CONFIG_VALUES);
    /** TLS: endpoint identification algorithm. */
    public static final ConfigParameter<String> SSL_ENDPOINT_IDENTIFICATION_ALGORITHM = new ConfigParameter<>("STRIMZI_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM", ConfigParameterParser.STRING, "HTTPS", CONFIG_VALUES);
    /** SASL: whether to enable configuration. */
    public static final ConfigParameter<Boolean> SASL_ENABLED = new ConfigParameter<>("STRIMZI_SASL_ENABLED", ConfigParameterParser.BOOLEAN, "false", CONFIG_VALUES);
    /** SASL: mechanism. */
    public static final ConfigParameter<String> SASL_MECHANISM = new ConfigParameter<>("STRIMZI_SASL_MECHANISM", ConfigParameterParser.STRING, "", CONFIG_VALUES);
    /** SASL: custom configuration. */
    public static final ConfigParameter<String> SASL_CUSTOM_CONFIG_JSON = new ConfigParameter<>("STRIMZI_SASL_CUSTOM_CONFIG_JSON", ConfigParameterParser.STRING, "", CONFIG_VALUES);
    /** SASL: username. */
    public static final ConfigParameter<String> SASL_USERNAME = new ConfigParameter<>("STRIMZI_SASL_USERNAME", ConfigParameterParser.STRING, "", CONFIG_VALUES);
    /** SASL: password. */
    public static final ConfigParameter<String> SASL_PASSWORD = new ConfigParameter<>("STRIMZI_SASL_PASSWORD", ConfigParameterParser.STRING, "", CONFIG_VALUES);
    /** SASL: security protocol. */
    public static final ConfigParameter<String> SECURITY_PROTOCOL = new ConfigParameter<>("STRIMZI_SECURITY_PROTOCOL", ConfigParameterParser.STRING, "", CONFIG_VALUES);
    /** Whether to use finalizers for KafkaTopic resources. */
    public static final ConfigParameter<Boolean> USE_FINALIZERS = new ConfigParameter<>("STRIMZI_USE_FINALIZERS", ConfigParameterParser.BOOLEAN, "true", CONFIG_VALUES);
    /** Max topic event queue size. */
    public static final ConfigParameter<Integer> MAX_QUEUE_SIZE = new ConfigParameter<>("STRIMZI_MAX_QUEUE_SIZE", ConfigParameterParser.strictlyPositive(ConfigParameterParser.INTEGER), "1024", CONFIG_VALUES);
    /** Max size of a topic event batch. */
    public static final ConfigParameter<Integer> MAX_BATCH_SIZE = new ConfigParameter<>("STRIMZI_MAX_BATCH_SIZE", ConfigParameterParser.strictlyPositive(ConfigParameterParser.INTEGER), "100", CONFIG_VALUES);
    /** Max linger time in milliseconds before creating a new topic event batch. */
    public static final ConfigParameter<Long> MAX_BATCH_LINGER_MS = new ConfigParameter<>("STRIMZI_MAX_BATCH_LINGER_MS", ConfigParameterParser.strictlyPositive(ConfigParameterParser.LONG), "100", CONFIG_VALUES);
    /** Whether to enable additional metrics related to requests to external services (Kafka, Kubernetes, Cruise Control). */
    public static final ConfigParameter<Boolean> ENABLE_ADDITIONAL_METRICS = new ConfigParameter<>("STRIMZI_ENABLE_ADDITIONAL_METRICS", ConfigParameterParser.BOOLEAN, "false", CONFIG_VALUES);
    /** An allow list of topic configurations that are reconciles, everything else is ignored. */
    public static final ConfigParameter<String> ALTERABLE_TOPIC_CONFIG = new ConfigParameter<>("STRIMZI_ALTERABLE_TOPIC_CONFIG", ConfigParameterParser.STRING, "ALL", CONFIG_VALUES);
    /** Skip cluster level configuration checks. */
    public static final ConfigParameter<Boolean> SKIP_CLUSTER_CONFIG_REVIEW = new ConfigParameter<>("STRIMZI_SKIP_CLUSTER_CONFIG_REVIEW", ConfigParameterParser.BOOLEAN, "false", CONFIG_VALUES);
    /** List of enabled and disabled feature gates. */
    public static final ConfigParameter<FeatureGates> FEATURE_GATES = new ConfigParameter<>("STRIMZI_FEATURE_GATES", ConfigParameterParser.parseFeatureGates(), "", CONFIG_VALUES);
    /** Cruise Control: whether to enable configuration. */
    public static final ConfigParameter<Boolean> CRUISE_CONTROL_ENABLED = new ConfigParameter<>("STRIMZI_CRUISE_CONTROL_ENABLED", ConfigParameterParser.BOOLEAN, "false", CONFIG_VALUES);
    /** Cruise Control: whether rack awareness is enabled. */
    public static final ConfigParameter<Boolean> CRUISE_CONTROL_RACK_ENABLED = new ConfigParameter<>("STRIMZI_CRUISE_CONTROL_RACK_ENABLED", ConfigParameterParser.BOOLEAN, "false", CONFIG_VALUES);
    /** Cruise Control: server hostname. */
    public static final ConfigParameter<String> CRUISE_CONTROL_HOSTNAME = new ConfigParameter<>("STRIMZI_CRUISE_CONTROL_HOSTNAME", ConfigParameterParser.STRING, "localhost", CONFIG_VALUES);
    /** Cruise Control: server port. */
    public static final ConfigParameter<Integer> CRUISE_CONTROL_PORT = new ConfigParameter<>("STRIMZI_CRUISE_CONTROL_PORT", ConfigParameterParser.strictlyPositive(ConfigParameterParser.INTEGER), "9090", CONFIG_VALUES);
    /** Cruise Control: whether rack awareness is enabled. */
    public static final ConfigParameter<Boolean> CRUISE_CONTROL_SSL_ENABLED = new ConfigParameter<>("STRIMZI_CRUISE_CONTROL_SSL_ENABLED", ConfigParameterParser.BOOLEAN, "false", CONFIG_VALUES);
    /** Cruise Control: whether authentication is enabled. */
    public static final ConfigParameter<Boolean> CRUISE_CONTROL_AUTH_ENABLED = new ConfigParameter<>("STRIMZI_CRUISE_CONTROL_AUTH_ENABLED", ConfigParameterParser.BOOLEAN, "false", CONFIG_VALUES);
    /** Cruise Control: CA certificate file location. */
    public static final ConfigParameter<String> CRUISE_CONTROL_CRT_FILE_PATH = new ConfigParameter<>("STRIMZI_CRUISE_CONTROL_CRT_FILE_PATH", ConfigParameterParser.STRING, "/etc/tls-sidecar/cluster-ca-certs/ca.crt", CONFIG_VALUES);
    /** Cruise Control: username file location. */
    public static final ConfigParameter<String> CRUISE_CONTROL_API_USER_PATH = new ConfigParameter<>("STRIMZI_CRUISE_CONTROL_API_USER_PATH", ConfigParameterParser.STRING, "/etc/eto-cc-api/" + CruiseControlApiProperties.TOPIC_OPERATOR_USERNAME_KEY, CONFIG_VALUES);
    /** Cruise Control: password file location. */
    public static final ConfigParameter<String> CRUISE_CONTROL_API_PASS_PATH = new ConfigParameter<>("STRIMZI_CRUISE_CONTROL_API_PASS_PATH", ConfigParameterParser.STRING, "/etc/eto-cc-api/" + CruiseControlApiProperties.TOPIC_OPERATOR_PASSWORD_KEY, CONFIG_VALUES);

    private final Map<String, Object> map;

    /**
     * Constructor.
     *
     * @param map Map containing configurations and their respective values.
     */
    private TopicOperatorConfig(Map<String, Object> map) {
        this.map = map;
    }

    /**
     * Creates the Topic Operator configuration from a map.
     *
     * @param map Configuration map.
     * @return Topic Operator configuration.
     */
    public static TopicOperatorConfig buildFromMap(Map<String, String> map) {
        Map<String, String> envMap = new HashMap<>(map);
        envMap.keySet().retainAll(TopicOperatorConfig.keyNames());
        Map<String, Object> generatedMap = ConfigParameter.define(envMap, CONFIG_VALUES);
        TopicOperatorConfig topicOperatorConfig = new TopicOperatorConfig(generatedMap);
        LOGGER.infoOp("TopicOperator configuration is {}", topicOperatorConfig);
        return topicOperatorConfig;
    }

<<<<<<< HEAD
    private static Set<String> keyNames() {
        return Collections.unmodifiableSet(CONFIG_VALUES.keySet());
    }

    @SuppressWarnings("unchecked")
    private <T> T get(ConfigParameter<T> value) {
        return (T) map.get(value.key());
    }

    /** @return Value of {@link #NAMESPACE} configuration. */
    public String namespace() {
        return get(NAMESPACE);
    }

    /** @return Value of {@link #RESOURCE_LABELS} configuration. */
    public Labels resourceLabels() {
        return get(RESOURCE_LABELS);
    }

    /** @return Value of {@link #BOOTSTRAP_SERVERS} configuration. */
    public String bootstrapServers() {
        return get(BOOTSTRAP_SERVERS);
    }

    /** @return Value of {@link #CLIENT_ID} configuration. */
    public String clientId() {
        return get(CLIENT_ID);
    }

    /** @return Value of {@link #FULL_RECONCILIATION_INTERVAL_MS} configuration. */
    public long fullReconciliationIntervalMs() {
        return get(FULL_RECONCILIATION_INTERVAL_MS);
    }

    /** @return Value of {@link #TLS_ENABLED} configuration. */
    public boolean tlsEnabled() {
        return get(TLS_ENABLED);
    }

    /** @return Value of {@link #TRUSTSTORE_LOCATION} configuration. */
    public String truststoreLocation() {
        return get(TRUSTSTORE_LOCATION);
    }

    /** @return Value of {@link #TRUSTSTORE_PASSWORD} configuration. */
    public String truststorePassword() {
        return get(TRUSTSTORE_PASSWORD);
    }

    /** @return Value of {@link #KEYSTORE_LOCATION} configuration. */
    public String keystoreLocation() {
        return get(KEYSTORE_LOCATION);
    }

    /** @return Value of {@link #KEYSTORE_PASSWORD} configuration. */
    public String keystorePassword() {
        return get(KEYSTORE_PASSWORD);
    }

    /** @return Value of {@link #SSL_ENDPOINT_IDENTIFICATION_ALGORITHM} configuration. */
    public String sslEndpointIdentificationAlgorithm() {
        return get(SSL_ENDPOINT_IDENTIFICATION_ALGORITHM);
    }

    /** @return Value of {@link #SASL_ENABLED} configuration. */
    public boolean saslEnabled() {
        return get(SASL_ENABLED);
    }

    /** @return Value of {@link #SASL_MECHANISM} configuration. */
    public String saslMechanism() {
        return get(SASL_MECHANISM);
    }

    /** @return Value of {@link #SASL_CUSTOM_CONFIG_JSON} configuration. */
    public String saslCustomConfigJson() {
        return get(SASL_CUSTOM_CONFIG_JSON);
    }

    /** @return Value of {@link #SASL_USERNAME} configuration. */
    public String saslUsername() {
        return get(SASL_USERNAME);
    }

    /** @return Value of {@link #SASL_PASSWORD} configuration. */
    public String saslPassword() {
        return get(SASL_PASSWORD);
    }

    /** @return Value of {@link #SECURITY_PROTOCOL} configuration. */
    public String securityProtocol() {
        return get(SECURITY_PROTOCOL);
    }

    /** @return Value of {@link #USE_FINALIZERS} configuration. */
    public boolean useFinalizer() {
        return get(USE_FINALIZERS);
    }

    /** @return Value of {@link #MAX_QUEUE_SIZE} configuration. */
    public int maxQueueSize() {
        return get(MAX_QUEUE_SIZE);
    }

    /** @return Value of {@link #MAX_BATCH_SIZE} configuration. */
    public int maxBatchSize() {
        return get(MAX_BATCH_SIZE);
    }

    /** @return Value of {@link #MAX_BATCH_LINGER_MS} configuration. */
    public long maxBatchLingerMs() {
        return get(MAX_BATCH_LINGER_MS);
    }

    /** @return Value of {@link #ENABLE_ADDITIONAL_METRICS} configuration. */
    public boolean enableAdditionalMetrics() {
        return get(ENABLE_ADDITIONAL_METRICS);
    }

    /** @return Value of {@link #ALTERABLE_TOPIC_CONFIG} configuration. */
    public String alterableTopicConfig() {
        return get(ALTERABLE_TOPIC_CONFIG);
    }

    /** @return Value of {@link #SKIP_CLUSTER_CONFIG_REVIEW} configuration. */
    public boolean skipClusterConfigReview() {
        return get(SKIP_CLUSTER_CONFIG_REVIEW);
    }

    /** @return Value of {@link #FEATURE_GATES} configuration. */
    public FeatureGates featureGates() {
        return get(FEATURE_GATES);
    }

    /** @return Value of {@link #CRUISE_CONTROL_ENABLED} configuration. */
    public boolean cruiseControlEnabled() {
        return get(CRUISE_CONTROL_ENABLED);
    }

    /** @return Value of {@link #CRUISE_CONTROL_RACK_ENABLED} configuration. */
    public boolean cruiseControlRackEnabled() {
        return get(CRUISE_CONTROL_RACK_ENABLED);
    }

    /** @return Value of {@link #CRUISE_CONTROL_HOSTNAME} configuration. */
    public String cruiseControlHostname() {
        return get(CRUISE_CONTROL_HOSTNAME);
    }

    /** @return Value of {@link #CRUISE_CONTROL_PORT} configuration. */
    public int cruiseControlPort() {
        return get(CRUISE_CONTROL_PORT);
    }

    /** @return Value of {@link #CRUISE_CONTROL_SSL_ENABLED} configuration. */
    public boolean cruiseControlSslEnabled() {
        return get(CRUISE_CONTROL_SSL_ENABLED);
    }

    /** @return Value of {@link #CRUISE_CONTROL_AUTH_ENABLED} configuration. */
    public boolean cruiseControlAuthEnabled() {
        return get(CRUISE_CONTROL_AUTH_ENABLED);
    }

    /** @return Value of {@link #CRUISE_CONTROL_CRT_FILE_PATH} configuration. */
    public String cruiseControlCrtFilePath() {
        return get(CRUISE_CONTROL_CRT_FILE_PATH);
    }

    /** @return Value of {@link #CRUISE_CONTROL_API_USER_PATH} configuration. */
    public String cruiseControlApiUserPath() {
        return get(CRUISE_CONTROL_API_USER_PATH);
    }

    /** @return Value of {@link #CRUISE_CONTROL_API_PASS_PATH} configuration. */
    public String cruiseControlApiPassPath() {
        return get(CRUISE_CONTROL_API_PASS_PATH);
    }

    Map<String, Object> adminClientConfig() {
        var kafkaClientProps = new HashMap<String, Object>();
        kafkaClientProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        kafkaClientProps.put(AdminClientConfig.CLIENT_ID_CONFIG, clientId());

        if (tlsEnabled() && !securityProtocol().isEmpty()) {
            if (!securityProtocol().equals("SSL") && !securityProtocol().equals("SASL_SSL")) {
                throw new InvalidConfigurationException("TLS is enabled but the security protocol does not match SSL or SASL_SSL");
            }
        }

        if (!securityProtocol().isEmpty()) {
            kafkaClientProps.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, securityProtocol());
        } else if (tlsEnabled()) {
            kafkaClientProps.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SSL");
        } else {
            kafkaClientProps.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
        }

        if (securityProtocol().equals("SASL_SSL") || securityProtocol().equals("SSL") || tlsEnabled()) {
            kafkaClientProps.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, sslEndpointIdentificationAlgorithm());

            if (!truststoreLocation().isEmpty()) {
                kafkaClientProps.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststoreLocation());
            }

            if (!truststorePassword().isEmpty()) {
                if (truststoreLocation().isEmpty()) {
                    throw new InvalidConfigurationException("TLS_TRUSTSTORE_PASSWORD was supplied but TLS_TRUSTSTORE_LOCATION was not supplied");
                }
                kafkaClientProps.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePassword());
            }

            if (!keystoreLocation().isEmpty() && !keystorePassword().isEmpty()) {
                kafkaClientProps.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystoreLocation());
                kafkaClientProps.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keystorePassword());
            }
        }

        if (saslEnabled()) {
            putSaslConfigs(kafkaClientProps);
        }

        return kafkaClientProps;
    }

    private void putSaslConfigs(Map<String, Object> kafkaClientProps) {
        if (saslCustomConfigJson().isBlank()) {
            setStandardSaslConfigs(kafkaClientProps);
        } else {
            setCustomSaslConfigs(kafkaClientProps);
        }
    }

    private void setCustomSaslConfigs(Map<String, Object> kafkaClientProps) {
        if (saslCustomConfigJson().isEmpty()) {
            throw new InvalidConfigurationException("Custom SASL config properties are not set");
        }

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);

        try {
            Map<String, String> customProperties = objectMapper.readValue(saslCustomConfigJson(), STRING_HASH_MAP_TYPE_REFERENCE);

            if (customProperties.isEmpty()) {
                throw new InvalidConfigurationException("SASL custom config properties empty");
            }

            for (var entry : customProperties.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();

                if (key == null || key.isBlank() || !key.startsWith("sasl.")) {
                    throw new InvalidConfigurationException("SASL custom config properties not SASL properties. customProperty: '" + key + "' = '" + value + "'");
                }

                kafkaClientProps.put(key, value);
            }
        } catch (JsonProcessingException e) {
            throw new InvalidConfigurationException("SASL custom config properties deserialize failed. customProperties: '" + saslCustomConfigJson() + "'");
        }
    }

    private void setStandardSaslConfigs(Map<String, Object> kafkaClientProps) {
        String saslMechanism;
        String jaasConfig;

        if (saslUsername().isEmpty() || saslPassword().isEmpty()) {
            throw new InvalidConfigurationException("SASL credentials are not set");
        }

        if ("plain".equals(saslMechanism())) {
            saslMechanism = "PLAIN";
            jaasConfig = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + saslUsername() + "\" password=\"" + saslPassword() + "\";";
        } else if ("scram-sha-256".equals(saslMechanism()) || "scram-sha-512".equals(saslMechanism())) {
            jaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + saslUsername() + "\" password=\"" + saslPassword() + "\";";

            if ("scram-sha-256".equals(saslMechanism())) {
                saslMechanism = "SCRAM-SHA-256";
            } else {
                saslMechanism = "SCRAM-SHA-512";
            }
        } else {
            throw new IllegalArgumentException("Invalid SASL_MECHANISM type: " + saslMechanism());
        }

        kafkaClientProps.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
        kafkaClientProps.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);
    }

    /**
     * @return Cruise Control client configuration.
     */
    public CruiseControlClient.Config cruiseControlClientConfig() {
        var sslCertificate = cruiseControlSslEnabled() ? getFileContent(cruiseControlCrtFilePath()) : null;
        var apiUsername = cruiseControlAuthEnabled() ? new String(getFileContent(cruiseControlApiUserPath()), StandardCharsets.UTF_8) : null;
        var apiPassword = cruiseControlAuthEnabled() ? new String(getFileContent(cruiseControlApiPassPath()), StandardCharsets.UTF_8) : null;
        
        return new CruiseControlClient.Config(
            cruiseControlHostname(),
            cruiseControlPort(),
            cruiseControlRackEnabled(),
            cruiseControlSslEnabled(),
            sslCertificate,
            cruiseControlAuthEnabled(),
            apiUsername,
            apiPassword
        );
    }

    private static byte[] getFileContent(String filePath) {
        try {
            return Files.readAllBytes(Path.of(filePath));
        } catch (IOException ioe) {
            throw new IllegalArgumentException(String.format("File not found: %s", filePath), ioe);
        }
    }

    @Override
    public String toString() {
        String mask = "********";
        return "TopicOperatorConfig{" +
            "\n\tnamespace='" + namespace() + '\'' +
            "\n\tresourceLabels=" + resourceLabels() +
            "\n\tbootstrapServers='" + bootstrapServers() + '\'' +
            "\n\tclientId='" + clientId() + '\'' +
            "\n\tfullReconciliationIntervalMs=" + fullReconciliationIntervalMs() +
            "\n\ttlsEnabled=" + tlsEnabled() +
            "\n\ttruststoreLocation='" + truststoreLocation() + '\'' +
            "\n\ttruststorePassword='" + mask + '\'' +
            "\n\tkeystoreLocation='" + keystoreLocation() + '\'' +
            "\n\tkeystorePassword='" + mask + '\'' +
            "\n\tsslEndpointIdentificationAlgorithm='" + sslEndpointIdentificationAlgorithm() + '\'' +
            "\n\tsaslEnabled=" + saslEnabled() +
            "\n\tsaslMechanism='" + saslMechanism() + '\'' +
            "\n\tsaslCustomConfigJson='" + (saslCustomConfigJson() == null ? null : mask) + '\'' +
            "\n\talterableTopicConfig='" + alterableTopicConfig() + '\'' +
            "\n\tskipClusterConfigReview='" + skipClusterConfigReview() + '\'' +
            "\n\tsaslUsername='" + saslUsername() + '\'' +
            "\n\tsaslPassword='" + mask + '\'' +
            "\n\tsecurityProtocol='" + securityProtocol() + '\'' +
            "\n\tuseFinalizer=" + useFinalizer() +
            "\n\tmaxQueueSize=" + maxQueueSize() +
            "\n\tmaxBatchSize=" + maxBatchSize() +
            "\n\tmaxBatchLingerMs=" + maxBatchLingerMs() +
            "\n\tenableAdditionalMetrics=" + enableAdditionalMetrics() +
            "\n\tfeatureGates='" + featureGates() + "'" +
            "\n\tcruiseControlEnabled=" + cruiseControlEnabled() +
            "\n\tcruiseControlRackEnabled=" + cruiseControlRackEnabled() +
            "\n\tcruiseControlHostname=" + cruiseControlHostname() +
            "\n\tcruiseControlPort=" + cruiseControlPort() +
            "\n\tcruiseControlSslEnabled=" + cruiseControlSslEnabled() +
            "\n\tcruiseControlAuthEnabled=" + cruiseControlAuthEnabled() +
            "\n\tcruiseControlCrtFilePath=" + cruiseControlCrtFilePath() +
            "\n\tcruiseControlApiUserPath=" + cruiseControlApiUserPath() +
            "\n\tcruiseControlApiPassPath=" + cruiseControlApiPassPath() +
            '}';
    }
}
