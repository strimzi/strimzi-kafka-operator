/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.topic;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.strimzi.kafka.config.model.ConfigModel;
import io.strimzi.kafka.config.model.ConfigModels;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.AbstractConfiguration;

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
public class KafkaTopicConfiguration extends AbstractConfiguration {

    /**
     * Constructor used to instantiate this class from JsonObject. Should be used to create configuration from
     * ConfigMap / CRD.
     *
     * @param reconciliation  The reconciliation
     * @param jsonOptions     Json object with configuration options as key ad value pairs.
     */
    public KafkaTopicConfiguration(Reconciliation reconciliation, Iterable<Map.Entry<String, Object>> jsonOptions) {
        super(reconciliation, jsonOptions, emptyList(), emptyList());
    }

    private KafkaTopicConfiguration(Reconciliation reconciliation, String configuration, List<String> forbiddenPrefixes) {
        super(reconciliation, configuration, forbiddenPrefixes);
    }

    /**
     * Returns a KafkaConfiguration created without forbidden option filtering.
     *
     * @param reconciliation The reconciliation
     * @param string A string representation of the Properties
     * @return The KafkaConfiguration
     */
    public static KafkaTopicConfiguration unvalidated(Reconciliation reconciliation, String string) {
        return new KafkaTopicConfiguration(reconciliation, string, emptyList());
    }

    /**
     * Returns a KafkaConfiguration created without forbidden option filtering.
     *
     * @param reconciliation The reconciliation
     * @param map A map representation of the Properties
     * @return The KafkaConfiguration
     */
    public static KafkaTopicConfiguration unvalidated(Reconciliation reconciliation, Map<String, String> map) {
        StringBuilder string = new StringBuilder();
        map.entrySet().forEach(entry -> string.append(entry.getKey() + "=" + entry.getValue() + "\n"));
        return new KafkaTopicConfiguration(reconciliation, string.toString(), emptyList());
    }

    /**
     * Validate the configs in this KafkaConfiguration returning a list of errors.
     * @param kafkaVersion The broker version.
     * @return A list of error messages.
     */
    public List<String> validate(String kafkaVersion) {
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
    public static Map<String, ConfigModel> readConfigModel(String kafkaVersion) {
        String name = "/kafka-" + kafkaVersion + "-topic-config-model.json";
        try {
            try (InputStream in = KafkaTopicConfiguration.class.getResourceAsStream(name)) {
                ConfigModels configModels = new ObjectMapper().readValue(in, ConfigModels.class);
                if (!kafkaVersion.equals(configModels.getVersion())) {
                    throw new RuntimeException("Incorrect version");
                }
                return configModels.getConfigs();
            }
        } catch (IOException e) {
            throw new RuntimeException("Error reading from classpath resource " + name, e);
        }
    }

    /**
     * Return the configs in this KafkaConfiguration which are not known broker configs.
     * These might be consumed by broker plugins.
     * @param kafkaVersion The broker version.
     * @return The unknown configs.
     */
    public Set<String> unknownConfigs(String kafkaVersion) {
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
    public Set<String> unknownConfigsWithValues(String kafkaVersion) {
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
