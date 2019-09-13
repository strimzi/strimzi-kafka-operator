/*
 * Copyright 2017-2018, Strimzi authors.
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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

/**
 * Class for handling Kafka configuration passed by the user
 */
public class KafkaConfiguration extends AbstractConfiguration {
    public static final String INTERBROKER_PROTOCOL_VERSION = "inter.broker.protocol.version";
    public static final String LOG_MESSAGE_FORMAT_VERSION = "log.message.format.version";

    private static final List<String> FORBIDDEN_OPTIONS;
    private static final List<String> EXCEPTIONS;

    static {
        FORBIDDEN_OPTIONS = asList(KafkaClusterSpec.FORBIDDEN_PREFIXES.split(", "));
        EXCEPTIONS = asList(KafkaClusterSpec.FORBIDDEN_PREFIX_EXCEPTIONS.split(", "));
    }

    /**
     * Constructor used to instantiate this class from JsonObject. Should be used to create configuration from
     * ConfigMap / CRD.
     *
     * @param jsonOptions     Json object with configuration options as key ad value pairs.
     */
    public KafkaConfiguration(Iterable<Map.Entry<String, Object>> jsonOptions) {
        super(jsonOptions, FORBIDDEN_OPTIONS, EXCEPTIONS);
    }

    private KafkaConfiguration(String configuration, List<String> forbiddenOptions) {
        super(configuration, forbiddenOptions);
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
     * Validate the configs in this KafkaConfiguration returning a list of errors.
     * @param kafkaVersion The broker version.
     * @return A list of error messages.
     */
    public List<String> validate(KafkaVersion kafkaVersion) {
        ConfigModels configs = readConfigModel(kafkaVersion);

        if (!kafkaVersion.version().equals(configs.getVersion())) {
            throw new RuntimeException("Incorrect version");
        }

        List<String> errors = new ArrayList<>();

        for (Map.Entry<String, String> entry: asOrderedProperties().asMap().entrySet()) {
            String key = entry.getKey();
            ConfigModel config = configs.getConfigs().get(key);
            if (config == null) {
                //throw new RuntimeException("Unknown config " + key);
                // TODO What about "child" configs (e.g. per-listener options)
            } else {
                switch (config.getType()) {
                    case INT:
                        validateInt(errors, entry, key, config);
                        break;
                    case LONG:
                        validateLong(errors, entry, key, config);
                        break;
                    case DOUBLE:
                        validateDouble(errors, entry, key, config);
                        break;
                    case BOOLEAN:
                        if (!entry.getValue().matches("true|false")) {
                            errors.add(key + " has value '" + entry.getValue() + "' which is not a boolean");
                        }
                        break;
                    case CLASS:
                        break;
                    case PASSWORD:
                        break;
                    case STRING:
                        if (config.getValues() != null
                                && !config.getValues().contains(entry.getValue())) {
                            errors.add(key + " has value '" + entry.getValue() + "' which is not one of the allowed values: " + config.getValues());
                        }
                        if (config.getPattern() != null
                                && !entry.getValue().matches(config.getPattern())) {
                            errors.add(key + " has value '" + entry.getValue() + "' which does not match the required pattern: " + config.getPattern());
                        }
                        break;
                    case LIST:
                        validateList(errors, entry, key, config);
                        break;
                    default:
                        throw new IllegalStateException("Unsupported type " + config.getType());
                }
            }
        }
        return errors;
    }

    private void validateList(List<String> errors, Map.Entry<String, String> entry, String key, ConfigModel config) {
        // TODO be more careful about whitespace
        List<String> l = asList(entry.getValue().trim().split(" *, *"));
        if (config.getItems() != null) {
            HashSet<String> items = new HashSet<>(l);
            items.removeAll(config.getItems());
            if (!items.isEmpty()) {
                errors.add(key + " contains values " + items + " which are not in the allowed items " + config.getItems());
            }
        }
    }

    private void validateDouble(List<String> errors, Map.Entry<String, String> entry, String key, ConfigModel config) {
        try {
            double i = Double.parseDouble(entry.getValue());
            if (config.getMinimum() != null
                    && i < config.getMinimum().doubleValue()) {
                errors.add(key + " has value " + entry.getValue() + " which less than the minimum value " + config.getMinimum());
            }
            if (config.getMaximum() != null
                    && i > config.getMaximum().doubleValue()) {
                errors.add(key + " has value '" + entry.getValue() + " which greater than the maximum value " + config.getMaximum());
            }
        } catch (NumberFormatException e) {
            errors.add(key + " has value '" + entry.getValue() + "' which is not a double");
        }
    }

    private void validateLong(List<String> errors, Map.Entry<String, String> entry, String key, ConfigModel config) {
        try {
            long i = Long.parseLong(entry.getValue());
            if (config.getMinimum() != null
                    && i < config.getMinimum().longValue()) {
                errors.add(key + " has value " + entry.getValue() + " which less than the minimum value " + config.getMinimum());
            }
            if (config.getMaximum() != null
                    && i > config.getMaximum().longValue()) {
                errors.add(key + " has value " + entry.getValue() + " which greater than the maximum value " + config.getMaximum());
            }
        } catch (NumberFormatException e) {
            errors.add(key + " has value '" + entry.getValue() + "' which is not a long");
        }
    }

    private void validateInt(List<String> errors, Map.Entry<String, String> entry, String key, ConfigModel config) {
        try {
            int i = Integer.parseInt(entry.getValue());
            if (config.getMinimum() != null
                && i < config.getMinimum() .intValue()) {
                errors.add(key + " has value " + entry.getValue() + " which less than the minimum value " + config.getMinimum());
            }
            if (config.getMaximum() != null
                    && i > config.getMaximum().intValue()) {
                errors.add(key + " has value " + entry.getValue() + " which greater than the maximum value " + config.getMaximum());
            }
        } catch (NumberFormatException e) {
            errors.add(key + " has value '" + entry.getValue() + "' which is not an int");
        }
    }

    private ConfigModels readConfigModel(KafkaVersion kafkaVersion) {
        String name = "/kafka-" + kafkaVersion.version() + "-config-model.json";
        try {
            try (InputStream in = KafkaConfiguration.class.getResourceAsStream(name)) {
                return new ObjectMapper().readValue(in, ConfigModels.class);
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
        ConfigModels c = readConfigModel(kafkaVersion);
        List<String> configsOfScope = c.getConfigs().entrySet().stream()
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
        ConfigModels c = readConfigModel(kafkaVersion);
        Set<String> result = new HashSet<>(asOrderedProperties().asMap().keySet());
        result.removeAll(c.getConfigs().keySet());
        return result;
    }


}
