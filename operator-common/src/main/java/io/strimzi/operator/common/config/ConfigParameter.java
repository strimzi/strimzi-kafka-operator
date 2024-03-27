/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.config;

import io.strimzi.operator.common.InvalidConfigurationException;

import java.util.HashMap;
import java.util.Map;

/**
 * Models a configuration parameter, identified by a unique key, which may be required, and if not may have a default value.
 * Optional parameters without a default value implicitly have a null default.
 * The key is also the name of the environment variable from which the value may be read, when it is read from the environment.
 *
 * @param key           Configuration parameter name/key
 * @param <T>           Type of object
 * @param type          type of the default value
 * @param defaultValue  Default value of the configuration parameter
 * @param required      If the value is required or not
 * @param map           Map that will contain all the configuration values
 */
public record ConfigParameter<T>(String key, ConfigParameterParser<T> type, String defaultValue, boolean required, Map<String, ConfigParameter<?>> map) {
    /**
     * Marker for indication "all namespaces" => this is used for example when creating watches to create a cluster
     * wide watch.
     */
    public final static String ANY_NAMESPACE = "*";

    /**
     * Constructor
     * @param key           Configuration parameter name/key
     * @param type          Type of the default value
     * @param map           Configuration map
     */
    public ConfigParameter(String key, ConfigParameterParser<T> type, Map<String, ConfigParameter<?>> map) {
        this(key, type, null, true, map);
        map.put(key(), this);
    }

    /**
     * Constructor
     * @param key           Configuration parameter name/key
     * @param type          Type of the default value
     * @param defaultValue  Default value of the configuration parameter
     * @param map           Configuration map
     */
    public ConfigParameter(String key, ConfigParameterParser<T> type, String defaultValue, Map<String, ConfigParameter<?>> map) {
        this(key, type, defaultValue, false, map);
        map.put(key(), this);
    }

    /**
     * Generates the configuration map
     * @param envVarMap          Map containing values entered by user.
     * @param configParameterMap Map containing all the configuration keys with default values
     * @return                   Generated configuration map
     */
    public static Map<String, Object> define(Map<String, String> envVarMap, Map<String, ConfigParameter<?>> configParameterMap) {

        Map<String, Object> generatedMap = new HashMap<>(envVarMap.size());
        for (Map.Entry<String, String> entry : envVarMap.entrySet()) {
            final ConfigParameter<?> configValue = configParameterMap.get(entry.getKey());
            if (configValue == null) {
                throw new InvalidConfigurationException("Unknown or null config value.");
            }
            // This check makes sure that if a user enters a null or empty value then the default values will be used.
            if (envVarMap.get(configValue.key()) == null || envVarMap.get(configValue.key()).isEmpty()) {
                generatedMap.put(configValue.key(), configValue.type().parse(configValue.defaultValue()));
            } else {
                generatedMap.put(configValue.key(), get(envVarMap, configValue));
            }
        }

        // now add all those config (with default value) that weren't in the given map
        Map<String, ConfigParameter<?>> x = new HashMap<>(configParameterMap);
        x.keySet().removeAll(envVarMap.keySet());
        for (ConfigParameter<?> value : x.values()) {
            generatedMap.put(value.key(), get(envVarMap, value));
        }
        return generatedMap;
    }

    private static <T> T get(Map<String, String> map, ConfigParameter<T> value) {

        final String s = map.getOrDefault(value.key(), value.defaultValue());
        if (s != null) {
            return value.type().parse(s);
        } else {
            if (value.required()) {
                throw new InvalidConfigurationException("Config value: " + value.key() + " is mandatory");
            }
            return null;
        }
    }
}