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
public abstract class AbstractConfig<T> {

    /**
     * Parses the string based on its type
     *
     * @param s String value
     * @return the value based on its type
     */
    public abstract T parse(String s) throws InvalidConfigurationException;

    /**
     * A java string
     */

    public static final AbstractConfig<? extends String> STRING = new AbstractConfig<>() {
        @Override
        public String parse(String s) {
            return s;
        }
    };

    public static final AbstractConfig<? extends String> NON_EMPTY = new AbstractConfig<>() {
        @Override
        public String parse(String s) throws InvalidConfigurationException {
            if (s == null || s.isEmpty()) {
                throw new InvalidConfigurationException("Failed to parse. Value cannot be empty or null");
            } else {
                return s;
            }
        }
    };


    /**
     * Returns List based on the String
     */
    public static final AbstractConfig<? extends List> LIST =  new AbstractConfig<>() {
        @Override
        public List<?> parse(String s) throws InvalidConfigurationException {

                List<String> windows = null;
                if (s != null && !s.isEmpty()) {
                    windows = Arrays.asList(s.split(";"));
                }

                return windows;
            }
        };

    public static final AbstractConfig<? extends Properties> KAFKA_ADMIN_CLIENT_CONFIGURATION_PROPERTIES =  new AbstractConfig<>() {
        @Override
        public Properties parse(String s) throws InvalidConfigurationException {

            Properties kafkaAdminClientConfiguration = new Properties();

            if (s != null) {
                try {
                    kafkaAdminClientConfiguration.load(new StringReader(s));
                } catch (IOException | IllegalArgumentException e) {
                    throw new InvalidConfigurationException("Failed to parse " + "STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION" + " configuration", e);
                }
            }

            return kafkaAdminClientConfiguration;
        }
    };

    /**
     * A java Long
     */
    public static final AbstractConfig<? extends Long> LONG = new AbstractConfig<>() {
        @Override
        public Long parse(String s) {
            return Long.parseLong(s);
        }
    };

    /**
     * A Java Integer
     */
    public static final AbstractConfig<? extends Integer> INTEGER = new AbstractConfig<>() {
        @Override
        public Integer parse(String s) {
            return Integer.parseInt(s);
        }
    };

    /**
     * A Java Boolean
     */
    public static final AbstractConfig<? extends Boolean> BOOLEAN = new AbstractConfig<>() {
        @Override
        public Boolean parse(String s) {
            return Boolean.parseBoolean(s);
        }
    };

    /**
     * A kubernetes selector.
     */
    public static final AbstractConfig<? extends Labels> LABEL_PREDICATE = new AbstractConfig<>() {
        @Override
        public Labels parse(String s) {
            return Labels.fromString(s);
        }
    };
}

