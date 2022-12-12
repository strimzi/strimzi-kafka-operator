/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.strimzi.api.kafka.model.KafkaClusterSpec;
import io.strimzi.kafka.config.model.ConfigModel;
import io.strimzi.kafka.config.model.ConfigModels;
import io.strimzi.operator.common.Reconciliation;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyList;

/**
 * Class for handling Kafka configuration passed by the user
 */
public class KafkaConfiguration extends AbstractConfiguration {
    /**
     * Configuration key of the inter-broker protocol version option
     */
    public static final String INTERBROKER_PROTOCOL_VERSION = "inter.broker.protocol.version";

    /**
     * Configuration key of the message format version option
     */
    public static final String LOG_MESSAGE_FORMAT_VERSION = "log.message.format.version";

    /**
     * Configuration key of the default replication factor option
     */
    public static final String DEFAULT_REPLICATION_FACTOR = "default.replication.factor";

    /**
     * Configuration key of the min-insync replicas version option
     */
    public static final String MIN_INSYNC_REPLICAS = "min.insync.replicas";

    private static final List<String> FORBIDDEN_PREFIXES;
    private static final List<String> FORBIDDEN_PREFIX_EXCEPTIONS;

    static {
        FORBIDDEN_PREFIXES = AbstractConfiguration.splitPrefixesToList(KafkaClusterSpec.FORBIDDEN_PREFIXES);
        FORBIDDEN_PREFIX_EXCEPTIONS = AbstractConfiguration.splitPrefixesToList(KafkaClusterSpec.FORBIDDEN_PREFIX_EXCEPTIONS);
    }

    /**
     * Constructor used to instantiate this class from JsonObject. Should be used to create configuration from
     * ConfigMap / CRD.
     *
     * @param reconciliation  The reconciliation
     * @param jsonOptions     Json object with configuration options as key ad value pairs.
     */
    public KafkaConfiguration(Reconciliation reconciliation, Iterable<Map.Entry<String, Object>> jsonOptions) {
        super(reconciliation, jsonOptions, FORBIDDEN_PREFIXES, FORBIDDEN_PREFIX_EXCEPTIONS);
    }

    private KafkaConfiguration(Reconciliation reconciliation, String configuration, List<String> forbiddenPrefixes) {
        super(reconciliation, configuration, forbiddenPrefixes);
    }


    /**
     * Returns a KafkaConfiguration created without forbidden option filtering.
     *
     * @param reconciliation The reconciliation
     * @param string A string representation of the Properties
     * @return The KafkaConfiguration
     */
    public static KafkaConfiguration unvalidated(Reconciliation reconciliation, String string) {
        return new KafkaConfiguration(reconciliation, string, emptyList());
    }

    /**
     * Validate the configs in this KafkaConfiguration returning a list of errors.
     * @param kafkaVersion The broker version.
     * @return A list of error messages.
     */
    public List<String> validate(KafkaVersion kafkaVersion) {
        List<String> errors = new ArrayList<>();
        Map<String, ConfigModel> models = readConfigModel(kafkaVersion);
        for (Map.Entry<String, String> entry: asOrderedProperties().asMap().entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            ConfigModel config = models.get(key);
            if (config != null) {
                // It's not an error if config _is_ null because extra configs
                // might be intended for plugins
                errors.addAll(config.validate(key, value));
            }
        }
        return errors;
    }

    /**
     * Gets the config model for the given version of the Kafka broker.
     * @param kafkaVersion The broker version.
     * @return The config model for that broker version.
     */
    @SuppressFBWarnings({"RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE"})
    public static Map<String, ConfigModel> readConfigModel(KafkaVersion kafkaVersion) {
        String name = "/kafka-" + kafkaVersion.version() + "-config-model.json";
        try {
            try (InputStream in = KafkaConfiguration.class.getResourceAsStream(name)) {
                if (in != null) {
                    ConfigModels configModels = new ObjectMapper().readValue(in, ConfigModels.class);
                    if (!kafkaVersion.version().equals(configModels.getVersion())) {
                        throw new RuntimeException("Incorrect version");
                    }
                    return configModels.getConfigs();
                } else {
                    // The configuration model does not exist
                    throw new RuntimeException("Configuration model " + name + " was not found");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error reading from classpath resource " + name, e);
        }
    }

    /**
     * Return the config properties with their values in this KafkaConfiguration which are not known broker configs.
     * These might be consumed by broker plugins.
     * @param kafkaVersion The broker version.
     * @return The unknown configs.
     */
    public Set<String> unknownConfigsWithValues(KafkaVersion kafkaVersion) {
        Map<String, ConfigModel> configModel = readConfigModel(kafkaVersion);
        Set<String> result = new HashSet<>();
        for (Map.Entry<String, String> e :this.asOrderedProperties().asMap().entrySet()) {
            if (!configModel.containsKey(e.getKey())) {
                result.add(e.getKey() + "=" + e.getValue());
            }
        }
        return result;
    }

    /**
     * @return  True if the configuration is empty. False otherwise.
     */
    public boolean isEmpty() {
        return this.asOrderedProperties().asMap().size() == 0;
    }
}
