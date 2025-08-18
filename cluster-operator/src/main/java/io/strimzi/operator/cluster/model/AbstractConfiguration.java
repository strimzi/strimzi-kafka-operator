/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.model;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.OrderedProperties;

import java.util.List;
import java.util.Locale;
import java.util.Map;

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
     * @param allowedPrefixes            List with configuration key prefixes which are allowed. All other keys will be ignored.
     */
    public AbstractConfiguration(Reconciliation reconciliation, Iterable<Map.Entry<String, Object>> jsonOptions, List<String> allowedPrefixes) {
        options.addIterablePairs(jsonOptions);
        filterAllowed(reconciliation, allowedPrefixes);
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
     * Filters forbidden values from the configuration based on a list of forbidden prefixes.
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
     * Filters forbidden values from the configuration based on a list of allowed prefixes.
     *
     * @param reconciliation            The reconciliation
     * @param allowedPrefixes           List with configuration key prefixes which are allowed. All other keys will be ignored.
     */
    private void filterAllowed(Reconciliation reconciliation, List<String> allowedPrefixes)   {
        options.filter(k -> {
            boolean allowed = allowedPrefixes.stream().anyMatch(p -> k.toLowerCase(Locale.ENGLISH).startsWith(p));

            if (allowed) {
                LOGGER.traceCr(reconciliation, "Configuration option \"{}\" is allowed and will be passed to the assembly", k);
            } else {
                LOGGER.warnCr(reconciliation, "Configuration option \"{}\" is not allowed and will be ignored", k);
            }

            // We have to return the reverted value to filter out the not allowed options
            return !allowed;
        });
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
}
