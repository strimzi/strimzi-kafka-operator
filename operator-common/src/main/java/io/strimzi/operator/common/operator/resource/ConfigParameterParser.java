/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;


import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.model.Labels;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Abstraction for things which convert a single configuration parameter value from a String to some specific type.
 */
public interface ConfigParameterParser<T> {

    /**
     * Parses the string based on its type
     *
     * @param configValue config value in String format
     * @throws InvalidConfigurationException if the given configuration value is not supported
     * @return the value based on its type
     */
    T parse(String configValue) throws InvalidConfigurationException;

    /**
     * A java string
     */
    ConfigParameterParser<String> STRING = configValue -> configValue;

    /**
     * A non empty java string
     */
    ConfigParameterParser<String> NON_EMPTY_STRING = configValue -> {
        if (configValue == null || configValue.isEmpty()) {
            throw new InvalidConfigurationException("Failed to parse. Value cannot be empty or null");
        } else {
            return configValue;
        }
    };

    /**
     * A semicolon-delimited list of strings.
     */
    ConfigParameterParser<List<String>> SEMICOLON_SEPARATED_LIST = configValue -> {
        List<String> windows = null;
        if (configValue != null && !configValue.isEmpty()) {
            windows = Arrays.asList(configValue.split(";"));
        }
        return windows;
    };

    /**
     * Returns Properties based on its String format
     */
    ConfigParameterParser<Properties> PROPERTIES = configValue -> {

        Properties kafkaAdminClientConfiguration = new Properties();

        if (configValue != null) {
            try {
                kafkaAdminClientConfiguration.load(new StringReader(configValue));
            } catch (IOException | IllegalArgumentException e) {
                throw new InvalidConfigurationException("Failed to parse the configuration string", e);
            }
        }

        return kafkaAdminClientConfiguration;
    };

    /**
     * A java Long
     */
    ConfigParameterParser<Long> LONG = s -> {
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            throw new InvalidConfigurationException("Failed to parse. Value is not valid", e);
        }
    };

    /**
     * A Java Integer
     */
    ConfigParameterParser<Integer> INTEGER = s -> {
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            throw new InvalidConfigurationException("Failed to parse. Value is not valid", e);
        }
    };

    /**
     * Strictly Positive Number
     * @param parser ConfigParameterParser object
     * @param <T>    Type of parameter
     * @return Positive number
     */
    static <T extends Number> ConfigParameterParser<T> strictlyPositive(ConfigParameterParser<T> parser) {
        return s -> {
            var value = parser.parse(s);
            if (value.longValue() <= 0) {
                throw new InvalidConfigurationException("Failed to parse. Negative value is not supported for this configuration");
            }
            return value;
        };
    }

    /**
     * A Java Boolean
     */
    ConfigParameterParser<Boolean> BOOLEAN = s -> {
        if (s.equals("true") || s.equals("false")) {
            return Boolean.parseBoolean(s);
        } else {
            throw new InvalidConfigurationException("Failed to parse. Value is not valid");
        }
    };

    /**
     * A kubernetes selector.
     */
    ConfigParameterParser<Labels> LABEL_PREDICATE = stringLabels -> {
        try {
            return Labels.fromString(stringLabels);
        } catch (IllegalArgumentException e) {
            throw new InvalidConfigurationException("Failed to parse. Value is not valid", e);
        }
    };
}

