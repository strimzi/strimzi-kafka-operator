/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.model;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Abstract class for processing and generating configuration passed by the user.
 */
public abstract class AbstractConfiguration {
    private static final Logger log = LogManager.getLogger(AbstractConfiguration.class.getName());

    private final OrderedProperties options = new OrderedProperties();

    /**
     * Constructor used to instantiate this class from String configuration. Should be used to create configuration
     * from the Assembly.
     *
     * @param configuration     Configuration in String format. Should contain zero or more lines with with key=value
     *                          pairs.
     * @param forbiddenOptions   List with configuration keys which are not allowed. All keys which start with one of
     *                           these keys will be ignored.
     */
    public AbstractConfiguration(String configuration, List<String> forbiddenOptions) {
        options.addStringPairs(configuration);
        filterForbidden(forbiddenOptions);
    }

    /**
     * Constructor used to instantiate this class from String configuration. Should be used to create configuration
     * from the Assembly.
     *
     * @param configuration     Configuration in String format. Should contain zero or more lines with with key=value
     *                          pairs.
     * @param forbiddenOptions  List with configuration keys which are not allowed. All keys which start with one of
     *                          these keys will be ignored.
     * @param defaults          Properties object with default options
     */
    public AbstractConfiguration(String configuration, List<String> forbiddenOptions, Map<String, String> defaults) {
        options.addMapPairs(defaults);
        options.addStringPairs(configuration);
        filterForbidden(forbiddenOptions);
    }

    /**
     * Constructor used to instantiate this class from JsonObject. Should be used to create configuration from
     * ConfigMap / CRD.
     *
     * @param jsonOptions     Json object with configuration options as key ad value pairs.
     * @param forbiddenOptions   List with configuration keys which are not allowed. All keys which start with one of
     *                           these keys will be ignored.
     */
    public AbstractConfiguration(Iterable<Map.Entry<String, Object>> jsonOptions, List<String> forbiddenOptions) {
        options.addIterablePairs(jsonOptions);
        filterForbidden(forbiddenOptions);
    }

    /**
     * Constructor used to instantiate this class from JsonObject. Should be used to create configuration from
     * ConfigMap / CRD.
     *
     * @param jsonOptions     Json object with configuration options as key ad value pairs.
     * @param forbiddenOptions   List with configuration keys which are not allowed. All keys which start with one of
     *                           these keys will be ignored.
     * @param exceptions        Exceptions excluded from forbidden options checking
     */
    public AbstractConfiguration(Iterable<Map.Entry<String, Object>> jsonOptions, List<String> forbiddenOptions, List<String> exceptions) {
        options.addIterablePairs(jsonOptions);
        filterForbidden(forbiddenOptions, exceptions);
    }

    /**
     * Constructor used to instantiate this class from JsonObject. Should be used to create configuration from
     * ConfigMap / CRD.
     *
     * @param jsonOptions     Json object with configuration options as key ad value pairs.
     * @param forbiddenOptions   List with configuration keys which are not allowed. All keys which start with one of
     *                           these keys will be ignored.
     * @param defaults          Properties object with default options
     */
    public AbstractConfiguration(Iterable<Map.Entry<String, Object>> jsonOptions, List<String> forbiddenOptions, Map<String, String> defaults) {
        options.addMapPairs(defaults);
        options.addIterablePairs(jsonOptions);
        filterForbidden(forbiddenOptions);
    }

    /**
     * Filters forbidden values from the configuration.
     *
     * @param forbiddenOptions  List with configuration keys which are not allowed. All keys which start with one of
     *                          these keys will be ignored.
     */
    private void filterForbidden(List<String> forbiddenOptions, List<String> exceptions)   {
        options.filter(k -> forbiddenOptions.stream().anyMatch(s -> {
            boolean forbidden = k.toLowerCase(Locale.ENGLISH).startsWith(s);
            if (forbidden) {
                if (exceptions.contains(k))
                    forbidden = false;
            }
            if (forbidden) {
                log.warn("Configuration option \"{}\" is forbidden and will be ignored", k);
            } else {
                log.trace("Configuration option \"{}\" is allowed and will be passed to the assembly", k);
            }
            return forbidden;
        }));
    }

    private void filterForbidden(List<String> forbiddenOptions)   {
        this.filterForbidden(forbiddenOptions, Collections.emptyList());
    }

    public String getConfigOption(String configOption) {
        return options.asMap().get(configOption);
    }

    public String getConfigOption(String configOption, String defaultValue) {
        return options.asMap().getOrDefault(configOption, defaultValue);
    }

    public void setConfigOption(String configOption, String value) {
        options.asMap().put(configOption, value);
    }

    public void removeConfigOption(String configOption) {
        options.asMap().remove(configOption);
    }

    /**
     * Generate configuration file in String format.
     *
     * @return  String with one or more lines containing key=value pairs with the configuration options.
     */
    public String getConfiguration() {
        return options.asPairs();
    }

    /**
     * Get access to underlying key-value pairs.  Any changes to OrderedProperties will be reflected in
     * in value returned by subsequent calls to getConfiguration()
     * @return A map of keys to values.
     */
    OrderedProperties asOrderedProperties() {
        return options;
    }
}
