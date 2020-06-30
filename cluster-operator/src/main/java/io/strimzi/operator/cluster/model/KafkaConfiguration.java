/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.strimzi.api.kafka.model.KafkaClusterSpec;
import io.strimzi.kafka.config.model.ConfigModel;
import io.strimzi.kafka.config.model.ConfigModels;
import io.strimzi.kafka.config.model.Scope;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;

/**
 * Class for handling Kafka configuration passed by the user
 */
public class KafkaConfiguration extends AbstractConfiguration {

    public static final String INTERBROKER_PROTOCOL_VERSION = "inter.broker.protocol.version";
    public static final String LOG_MESSAGE_FORMAT_VERSION = "log.message.format.version";

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
     * @param jsonOptions     Json object with configuration options as key ad value pairs.
     */
    public KafkaConfiguration(Iterable<Map.Entry<String, Object>> jsonOptions) {
        super(jsonOptions, FORBIDDEN_PREFIXES, FORBIDDEN_PREFIX_EXCEPTIONS);
    }

    private KafkaConfiguration(String configuration, List<String> forbiddenPrefixes) {
        super(configuration, forbiddenPrefixes);
    }


    /**
     * Returns a KafkaConfiguration created without forbidden option filtering.
     * @param string A string representation of the Properties
     * @return The KafkaConfiguration
     */
    public static KafkaConfiguration unvalidated(String string) {
        return new KafkaConfiguration(string, emptyList());
    }

    /**
     * Returns a KafkaConfiguration created without forbidden option filtering.
     * @param map A map representation of the Properties
     * @return The KafkaConfiguration
     */
    public static KafkaConfiguration unvalidated(Map<String, String> map) {
        StringBuilder string = new StringBuilder();
        map.entrySet().forEach(entry -> string.append(entry.getKey() + "=" + entry.getValue() + "\n"));
        return new KafkaConfiguration(string.toString(), emptyList());
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
    public static Map<String, ConfigModel> readConfigModel(KafkaVersion kafkaVersion) {
        String name = "/kafka-" + kafkaVersion.version() + "-config-model.json";
        try {
            try (InputStream in = KafkaConfiguration.class.getResourceAsStream(name)) {
                ConfigModels configModels = new ObjectMapper().readValue(in, ConfigModels.class);
                if (!kafkaVersion.version().equals(configModels.getVersion())) {
                    throw new RuntimeException("Incorrect version");
                }
                return configModels.getConfigs();
            }
        } catch (IOException e) {
            throw new RuntimeException("Error reading from classpath resource " + name, e);
        }
    }

    /**
     * Return true if the configs in this KafkaConfiguration include any which are read-only.
     * @param kafkaVersion The broker version.
     * @return true if the configs in this KafkaConfiguration include any which are read-only.
     */
    public boolean anyReadOnly(KafkaVersion kafkaVersion) {
        Set<String> strings = readOnlyConfigs(kafkaVersion);
        return !strings.isEmpty();
    }

    /**
     * Return the configs in this KafkaConfiguration which are read-only.
     * @param kafkaVersion The broker version.
     * @return The read-only configs.
     */
    public Set<String> readOnlyConfigs(KafkaVersion kafkaVersion) {
        return withScope(kafkaVersion, Scope.READ_ONLY);
    }

    /**
     * Return the configs in this KafkaConfiguration which are cluster-wide.
     * @param kafkaVersion The broker version.
     * @return The cluster-wide configs.
     */
    public Set<String> clusterWideConfigs(KafkaVersion kafkaVersion) {
        return withScope(kafkaVersion, Scope.CLUSTER_WIDE);
    }

    /**
     * Return the configs in this KafkaConfiguration which are per-broker.
     * @param kafkaVersion The broker version.
     * @return The per-broker configs.
     */
    public Set<String> perBrokerConfigs(KafkaVersion kafkaVersion) {
        return withScope(kafkaVersion, Scope.PER_BROKER);
    }

    private Set<String> withScope(KafkaVersion kafkaVersion, Scope scope) {
        Map<String, ConfigModel> c = readConfigModel(kafkaVersion);
        List<String> configsOfScope = c.entrySet().stream()
                .filter(config -> scope.equals(config.getValue().getScope()))
                .map(config -> config.getKey())
                .collect(Collectors.toList());
        Set<String> result = new HashSet<>(asOrderedProperties().asMap().keySet());
        result.retainAll(configsOfScope);
        return Collections.unmodifiableSet(result);
    }

    /**
     * Return the configs in this KafkaConfiguration which are not known broker configs.
     * These might be consumed by broker plugins.
     * @param kafkaVersion The broker version.
     * @return The unknown configs.
     */
    public Set<String> unknownConfigs(KafkaVersion kafkaVersion) {
        Map<String, ConfigModel> c = readConfigModel(kafkaVersion);
        Set<String> result = new HashSet<>(asOrderedProperties().asMap().keySet());
        result.removeAll(c.keySet());
        return result;
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

    public boolean isEmpty() {
        return this.asOrderedProperties().asMap().size() == 0;
    }
}
