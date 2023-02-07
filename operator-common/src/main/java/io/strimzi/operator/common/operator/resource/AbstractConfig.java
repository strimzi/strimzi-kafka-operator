/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;


import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.model.Labels;

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

