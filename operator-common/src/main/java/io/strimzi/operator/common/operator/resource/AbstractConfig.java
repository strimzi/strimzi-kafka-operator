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
 * Abstract class which contains different type of parameters
 */
public interface AbstractConfig<T> {

    /**
     * Parses the string based on its type
     *
     * @param s String value
     * @return the value based on its type
     */
    T parse(String s) throws InvalidConfigurationException;

    /**
     * A java string
     */

    AbstractConfig<String> STRING = (AbstractConfig<String>) s -> s;

    /**
     * A non empty java string
     */
    AbstractConfig<String> NON_EMPTY = (AbstractConfig<String>) s -> {
        if (s == null || s.isEmpty()) {
            throw new InvalidConfigurationException("Failed to parse. Value cannot be empty or null");
        } else {
            return s;
        }
    };

    /**
     * Returns List based on the String
     */
    AbstractConfig<List<String>> LIST = s -> {

            List<String> windows = null;
            if (s != null && !s.isEmpty()) {
                windows = Arrays.asList(s.split(";"));
            }
            return windows;
        };

    /**
     * Returns Kafka admin client configuration properties
     */
    AbstractConfig<Properties> KAFKA_ADMIN_CLIENT_CONFIGURATION_PROPERTIES = (AbstractConfig<Properties>) s -> {

        Properties kafkaAdminClientConfiguration = new Properties();

        if (s != null) {
            try {
                kafkaAdminClientConfiguration.load(new StringReader(s));
            } catch (IOException | IllegalArgumentException e) {
                throw new InvalidConfigurationException("Failed to parse " + "STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION" + " configuration", e);
            }
        }

        return kafkaAdminClientConfiguration;
    };

    /**
     * A java Long
     */
    AbstractConfig<Long> LONG = (AbstractConfig<Long>) Long::parseLong;

    /**
     * A Java Integer
     */
    AbstractConfig<Integer> INTEGER = (AbstractConfig<Integer>) Integer::parseInt;

    /**
     * A Java Boolean
     */
    AbstractConfig<Boolean> BOOLEAN = (AbstractConfig<Boolean>) Boolean::parseBoolean;

    /**
     * A kubernetes selector.
     */
    AbstractConfig<Labels> LABEL_PREDICATE = (AbstractConfig<Labels>) Labels::fromString;
}

