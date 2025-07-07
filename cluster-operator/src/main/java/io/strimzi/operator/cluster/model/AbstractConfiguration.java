/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.model;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.OrderedProperties;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;

/**
 * Abstract class for processing and generating configuration passed by the user.
 */
public abstract class AbstractConfiguration {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(AbstractConfiguration.class.getName());

    private final OrderedProperties options = new OrderedProperties();

    /**
     * Copy constructor which creates new instance of the Abstract Configuration from existing configuration. It is
     * useful when you need to modify an instance of the configuration without permanently changing the original.
     *
     * @param configuration     Existing configuration
     */
    public AbstractConfiguration(AbstractConfiguration configuration)   {
        options.addMapPairs(configuration.asOrderedProperties().asMap());
    }

    /**
     * Constructor used to instantiate this class from JsonObject. Should be used to create configuration from
     * ConfigMap / CRD.
     *
     * @param reconciliation             The reconciliation
     * @param jsonOptions                Json object with configuration options as key ad value pairs.
     * @param forbiddenPrefixes          List with configuration key prefixes which are not allowed. All keys which
     *                                   start with one of these prefixes will be ignored.
     * @param forbiddenPrefixExceptions  Exceptions excluded from forbidden prefix options checking
     * @param forbiddenOptions           List with the exact configuration options (not prefixes) that should not be used
     * @param defaults                   Properties object with default options
     */
    public AbstractConfiguration(Reconciliation reconciliation, Iterable<Map.Entry<String, Object>> jsonOptions, List<String> forbiddenPrefixes, List<String> forbiddenPrefixExceptions, List<String> forbiddenOptions, Map<String, String> defaults) {
        options.addMapPairs(defaults);
        options.addIterablePairs(jsonOptions);
        filterForbidden(reconciliation, forbiddenPrefixes, forbiddenPrefixExceptions, forbiddenOptions);
    }

    /**
     * Constructor used to instantiate this class from JsonObject. Should be used to create configuration from
     * ConfigMap / CRD.
     *
     * @param reconciliation             The reconciliation
     * @param configuration              Configuration in String format. Should contain zero or more lines with
     *                                   key=value pairs.
     * @param forbiddenPrefixes          List with configuration key prefixes which are not allowed. All keys which
     *                                   start with one of these prefixes will be ignored.
     * @param forbiddenPrefixExceptions  Exceptions excluded from forbidden prefix options checking
     * @param forbiddenOptions           List with the exact configuration options (not prefixes) that should not be used
     * @param defaults                   Properties object with default options
     */
    public AbstractConfiguration(Reconciliation reconciliation, String configuration, List<String> forbiddenPrefixes, List<String> forbiddenPrefixExceptions, List<String> forbiddenOptions, Map<String, String> defaults) {
        options.addMapPairs(defaults);
        options.addStringPairs(configuration);
        filterForbidden(reconciliation, forbiddenPrefixes, forbiddenPrefixExceptions, forbiddenOptions);
    }

    /**
     * Filters forbidden values from the configuration.
     *
     * @param reconciliation            The reconciliation
     * @param forbiddenPrefixes         List with configuration key prefixes which are not allowed. All keys which start
     *                                  with one of these prefixes will be ignored.
     * @param forbiddenPrefixExceptions Exceptions excluded from forbidden prefix options checking
     * @param forbiddenOptions          List with the exact configuration options (not prefixes) that should not be used
     */
    private void filterForbidden(Reconciliation reconciliation, List<String> forbiddenPrefixes, List<String> forbiddenPrefixExceptions, List<String> forbiddenOptions)   {
        // We filter the prefixes first
        options.filter(k -> forbiddenPrefixes.stream().anyMatch(s -> {
            boolean forbidden = k.toLowerCase(Locale.ENGLISH).startsWith(s);

            if (forbidden) {
                if (forbiddenPrefixExceptions.contains(k))
                    forbidden = false;
            }

            if (forbidden) {
                LOGGER.warnCr(reconciliation, "Configuration option \"{}\" is forbidden and will be ignored", k);
            } else {
                LOGGER.traceCr(reconciliation, "Configuration option \"{}\" is allowed and will be passed to the assembly", k);
            }

            return forbidden;
        }));

        // Then we filter the forbidden options
        if (!forbiddenOptions.isEmpty()) {
            options.filter(k -> forbiddenOptions.stream().anyMatch(s -> {
                boolean forbidden = k.toLowerCase(Locale.ENGLISH).equals(s);

                if (forbidden) {
                    LOGGER.warnCr(reconciliation, "Configuration option \"{}\" is forbidden and will be ignored", k);
                } else {
                    LOGGER.traceCr(reconciliation, "Configuration option \"{}\" is allowed and will be passed to the assembly", k);
                }

                return forbidden;
            }));
        }
    }

    /**
     * Returns a value for a specific config option
     *
     * @param configOption  Config option which should be looked-up
     *
     * @return  The configured value for this option
     */
    public String getConfigOption(String configOption) {
        return options.asMap().get(configOption);
    }

    /**
     * Returns a value for a specific config option or a default value
     *
     * @param configOption  Config option which should be looked-up
     * @param defaultValue  Default value
     *
     * @return  The configured value for this option or the default value if given option is not configured
     */
    public String getConfigOption(String configOption, String defaultValue) {
        return options.asMap().getOrDefault(configOption, defaultValue);
    }

    /**
     * Sets config option
     *
     * @param configOption  Configuration option which should be set
     * @param value         The value to which it should be set
     */
    public void setConfigOption(String configOption, String value) {
        options.asMap().put(configOption, value);
    }

    /**
     * Removes a configuration option from the configuration
     *
     * @param configOption  Configuration option which should be removed
     */
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
     * Get access to underlying key-value pairs. Any changes to OrderedProperties will be reflected in value returned
     * by subsequent calls to getConfiguration()
     *
     * @return A map of keys to values.
     */
    public OrderedProperties asOrderedProperties() {
        return options;
    }

    /**
     * Splits string with comma-separated values into a List
     *
     * @param prefixes  String with comma-separated items
     * @return          List with the values as separate items
     */
    protected static List<String> splitPrefixesOrOptionsToList(String prefixes) {
        return asList(prefixes.split("\\s*,+\\s*"));
    }

    /**
     * Append list property values or create a new list property if missing.
     * A list property can contain a comma separated list of values.
     * Duplicated values are removed.
     *
     * @param props Ordered properties.
     * @param key List property key.
     * @param values List property values.
     */
    public static void createOrAddListProperty(OrderedProperties props, String key, String values) {
        if (props == null) {
            throw new IllegalArgumentException("Configuration is required");
        }
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Configuration key is required");
        }
        if (values == null || values.isBlank()) {
            throw new IllegalArgumentException("Configuration values are required");
        }

        String existingConfig = props.asMap().get(key);
        // using an ordered set to preserve ordering of the existing props value
        Set<String> existingSet = existingConfig == null ? new LinkedHashSet<>() :
                Arrays.stream(existingConfig.split(","))
                        .map(String::trim)
                        .filter(s -> !s.isEmpty())
                        .collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new));
        // we also preserve ordering for new values in case they are user-provided
        Set<String> newValues = Arrays.stream(values.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new));
        // add only new values
        boolean updated = existingSet.addAll(newValues);
        if (updated) {
            String updatedConfig = String.join(",", existingSet);
            props.addPair(key, updatedConfig);
        }
    }
}
