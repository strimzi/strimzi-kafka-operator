/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.strimzi.api.kafka.model.kafka.KafkaClusterSpec;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;
import io.strimzi.kafka.config.model.ConfigModel;
import io.strimzi.kafka.config.model.ConfigModels;
import io.strimzi.kafka.config.model.Type;
import io.strimzi.operator.common.Reconciliation;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
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
     * Configuration key of the default replication factor option
     */
    public static final String DEFAULT_REPLICATION_FACTOR = "default.replication.factor";

    private static final String PER_LISTENER_CONFIG_FORMAT = "listener.name.%s.%s";
    private static final List<String> FORBIDDEN_PREFIXES;
    private static final List<String> FORBIDDEN_PREFIX_EXCEPTIONS;
    private static final List<String> ALLOWED_PER_LISTENER_CONFIGURATIONS;
    private static final Map<String, String> DEFAULTS;

    static {
        FORBIDDEN_PREFIXES = AbstractConfiguration.splitPrefixesOrOptionsToList(KafkaClusterSpec.FORBIDDEN_PREFIXES);
        FORBIDDEN_PREFIX_EXCEPTIONS = AbstractConfiguration.splitPrefixesOrOptionsToList(KafkaClusterSpec.FORBIDDEN_PREFIX_EXCEPTIONS);
        ALLOWED_PER_LISTENER_CONFIGURATIONS = AbstractConfiguration.splitPrefixesOrOptionsToList(KafkaClusterSpec.ALLOWED_PER_LISTENER_CONFIGS);

        DEFAULTS = new HashMap<>(1);
        // when users remove "min.insync.replicas" from the Kafka custom resource, the operator is going to force the
        // default value (1) regardless of whether ELR (Eligible Leader Replicas) is enabled or disabled
        DEFAULTS.put("min.insync.replicas", "1");
    }

    /**
     * Copy constructor which creates new instance of the Kafka Configuration from existing configuration. It is
     * useful when you need to modify an instance of the configuration without permanently changing the original.
     *
     * @param configuration     Existing configuration
     */
    public KafkaConfiguration(KafkaConfiguration configuration)   {
        super(configuration);
    }

    /**
     * Constructor used to instantiate this class from JsonObject. Should be used to create configuration from
     * ConfigMap / CRD.
     *
     * @param reconciliation  The reconciliation
     * @param jsonOptions     Json object with configuration options as key ad value pairs.
     */
    public KafkaConfiguration(Reconciliation reconciliation, Iterable<Map.Entry<String, Object>> jsonOptions) {
        super(reconciliation, jsonOptions, FORBIDDEN_PREFIXES, FORBIDDEN_PREFIX_EXCEPTIONS, List.of(), DEFAULTS);
    }

    /**
     * Constructor used to instantiate this class from JsonObject. Should be used to create configuration from
     * ConfigMap / CRD. It also modifies the FORBIDDEN_PREFIX_EXCEPTIONS with the given listener names.
     *
     * @param reconciliation        The reconciliation
     * @param jsonOptions           Json object with configuration options as key ad value pairs.
     * @param listeners             Listener names to add to the exception list
     */
    public KafkaConfiguration(Reconciliation reconciliation, Iterable<Map.Entry<String, Object>> jsonOptions, List<GenericKafkaListener> listeners) {
        super(reconciliation, jsonOptions, FORBIDDEN_PREFIXES, modifyWithListeners(listeners), List.of(), DEFAULTS);
    }

    private KafkaConfiguration(Reconciliation reconciliation, String configuration, List<String> forbiddenPrefixes) {
        super(reconciliation, configuration, forbiddenPrefixes, List.of(), List.of(), DEFAULTS);
    }


    /* test */ static List<String> modifyWithListeners(List<GenericKafkaListener> listeners) {
        List<String> exceptions = new ArrayList<>();
        listeners.stream().map(ListenersUtils::identifier).forEach(l -> ALLOWED_PER_LISTENER_CONFIGURATIONS.forEach(c -> exceptions.add(String.format(PER_LISTENER_CONFIG_FORMAT, l, c))));
        exceptions.addAll(FORBIDDEN_PREFIX_EXCEPTIONS);
        return exceptions;
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
            if (isCustomConfigurationOption(e.getKey(), configModel)) {
                result.add(e.getKey() + "=" + e.getValue());
            }
        }
        return result;
    }

    /**
     * Checks if the configuration is empty.
     *
     * @return  True if the configuration is empty. False otherwise.
     */
    public boolean isEmpty() {
        return this.asOrderedProperties().asMap().isEmpty();
    }

    /**
     * Checks if the Kafka configuration option is part of the Kafka configuration or is a custom option not recognized
     * by Kafka broker configuration APIs. Custom options can be for example options used by plugins etc. But right now,
     * also the options prefixed with the listener prefix as considered custom by this method as well (which is correct
     * for the time being as we anyway do a rolling update when they change).
     *
     * @param optionName    Name of the option to check
     * @param configModel   Configuration model for given Kafka version
     *
     * @return  True if entry is custom (not default). False otherwise.
     */
    public static boolean isCustomConfigurationOption(String optionName, Map<String, ConfigModel> configModel) {
        return !configModel.containsKey(optionName);
    }

    /**
     * Checks if the property uses the Double type
     *
     * @param optionName    Name of the property to check
     * @param configModel   Configuration model for the given Kafka version
     *
     * @return  True if the property uses the Double type. False otherwise.
     */
    public static boolean isDouble(String optionName, Map<String, ConfigModel> configModel) {
        return Type.DOUBLE.equals(configModel.get(optionName).getType());
    }
}
