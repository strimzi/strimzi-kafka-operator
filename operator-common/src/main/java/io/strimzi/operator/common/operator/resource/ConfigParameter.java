package io.strimzi.operator.common.operator.resource;

import io.strimzi.operator.common.InvalidConfigurationException;

import java.util.HashMap;
import java.util.Map;

/**
 * Models a configuration parameter, identified by a unique key, which may be required, and if not may have a default value.
 * Optional parameters without a default value implicitly have a null default.
 * The key is also the name of the environment variable from which the value may be read, when it is read from the environment.
 *
 * @param key Configuration parameter name/key
 * @param <T> Type of object
 * @param type type of the default value
 * @param defaultValue default value of the configuration parameter
 * @param required  If the value is required or not
 */
public record ConfigParameter<T>(String key, AbstractConfig<? extends T> type, String defaultValue, boolean required, Map<String, ConfigParameter<?>> map) {
    /**
     * Contructor
     * @param key Configuration parameter name/key
     * @param type type of the default value
     * @param defaultValue default value of the configuration parameter
     * @param required  If the value is required or not
     */
    public ConfigParameter {
        map.put(key, this);
    }


    public static Map<String,Object> define(Map<String, String> envVarMap, Map<String, ConfigParameter<?>> CONFIG_VALUES) {

        Map<String, Object> generatedMap = new HashMap<>(envVarMap.size());
        for (Map.Entry<String, String> entry : envVarMap.entrySet()) {
            final ConfigParameter<?> configValue = CONFIG_VALUES.get(entry.getKey());
            generatedMap.put(configValue.key(), get(envVarMap, CONFIG_VALUES, configValue));
        }

        // now add all those config (with default value) that weren't in the given map
        Map<String, ConfigParameter<?>> x = new HashMap<>(CONFIG_VALUES);
        x.keySet().removeAll(envVarMap.keySet());
        for (ConfigParameter<?> value : x.values()) {
           generatedMap.put(value.key(), get(envVarMap, CONFIG_VALUES, value));
        }

        return generatedMap;
    }

    public static <T> T get(Map<String, String> map, Map<String, ConfigParameter<?>> CONFIG_VALUES, ConfigParameter<T> value) {
        if (!CONFIG_VALUES.containsKey(value.key())) {
            throw new InvalidConfigurationException("Unknown config value: " + value.key() + " probably needs to be added to Config.CONFIG_VALUES");
        }

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
    @SuppressWarnings("unchecked")
    public static <T> T get(Map<String, Object> generatedMap, ConfigParameter<T> value, T defaultValue) {
        return (T) generatedMap.getOrDefault(value.key(), defaultValue);
    }

    @SuppressWarnings("unchecked")
    public static <T> T get(Map<String, Object> generatedMap, String key) {
        return (T) generatedMap.get(key);
    }

    /**
     * Gets the configuration value corresponding to the key
     * @param <T> type of value
     * @param value instance of Value class
     * @return configuration value w.r.t to the key
     */
    @SuppressWarnings("unchecked")
    public static <T> T get(Map<String, Object> generatedMap, ConfigParameter<T> value) {
        return (T) generatedMap.get(value.key());
    }
}