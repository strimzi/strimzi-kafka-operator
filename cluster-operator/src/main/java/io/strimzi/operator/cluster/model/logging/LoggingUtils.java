/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.logging;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.strimzi.api.kafka.model.common.ExternalLogging;
import io.strimzi.api.kafka.model.common.InlineLogging;
import io.strimzi.api.kafka.model.common.Logging;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.OrderedProperties;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Shared methods for working with Logging configurations
 */
public class LoggingUtils {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(LoggingUtils.class.getName());

    /**
     * The refresh interval which will be set in Log4j2 logging configurations to automatically refresh the logging
     * configuration.
     */
    private static final String LOG4J2_MONITOR_INTERVAL_SECONDS = "30";

    /**
     * Generates the logging configuration as a String. The configuration is generated based on the default logging
     * configuration files from resources, the (optional) inline logging configuration from the custom resource
     * and the (optional) external logging configuration in a user-provided ConfigMap.
     *
     * @param reconciliation                Reconciliation marker
     * @param logging                       Logging configuration from the custom resource
     * @param externalCm                    User-provided ConfigMap with custom Log4j / Log4j2 file
     *
     * @return  String with the Log4j / Log4j2 properties used for configuration
     */
    protected static String loggingConfiguration(Reconciliation reconciliation, LoggingModel logging, ConfigMap externalCm) {
        if (logging.getLogging() instanceof InlineLogging inlineLogging) {
            OrderedProperties newSettings = defaultLogConfig(reconciliation, logging.getDefaultLogConfigBaseName());

            if (inlineLogging.getLoggers() != null) {
                newSettings.addMapPairs(inlineLogging.getLoggers());
            }

            return createLog4jProperties(newSettings);
        } else if (logging.getLogging() instanceof ExternalLogging externalLogging) {
            if (externalLogging.getValueFrom() != null && externalLogging.getValueFrom().getConfigMapKeyRef() != null && externalLogging.getValueFrom().getConfigMapKeyRef().getKey() != null) {
                if (externalCm != null && externalCm.getData() != null && externalCm.getData().containsKey(externalLogging.getValueFrom().getConfigMapKeyRef().getKey())) {
                    return maybeAddMonitorIntervalToExternalLogging(externalCm.getData().get(externalLogging.getValueFrom().getConfigMapKeyRef().getKey()));
                } else {
                    throw new InvalidResourceException(
                            String.format("ConfigMap %s with external logging configuration does not exist or doesn't contain the configuration under the %s key.",
                                    externalLogging.getValueFrom().getConfigMapKeyRef().getName(),
                                    externalLogging.getValueFrom().getConfigMapKeyRef().getKey())
                    );
                }
            } else {
                throw new InvalidResourceException("Property logging.valueFrom has to be specified when using external logging.");
            }
        } else {
            LOGGER.debugCr(reconciliation, "logging is not set, using default loggers");
            return createLog4jProperties(defaultLogConfig(reconciliation, logging.getDefaultLogConfigBaseName()));
        }
    }

    /**
     * Extracts root logger appender name form the logging configuration
     *
     * @param reconciliation    Reconciliation marker
     * @param newSettings       New logging settings
     *
     * @return  Name of the root logger appender
     */
    private static String getRootAppenderNamesFromDefaultLoggingConfig(Reconciliation reconciliation, OrderedProperties newSettings) {
        String logger = newSettings.asMap().get("log4j.rootLogger");
        String appenderName = "";

        if (logger != null) {
            String[] tmp = logger.trim().split(",", 2);

            if (tmp.length == 2) {
                appenderName = tmp[1].trim();
            } else {
                LOGGER.warnCr(reconciliation, "Logging configuration for root logger does not contain appender.");
            }
        } else {
            LOGGER.warnCr(reconciliation, "Logger log4j.rootLogger not set.");
        }

        return appenderName;
    }

    /**
     * Adds 'monitorInterval=30' to Log4j2 logging. If the logging configuration already has it,
     * returns the logging configuration without any change.
     *
     * @param data      String with Log4j2 properties in format key=value separated by new lines
     *
     * @return  Log4j2 configuration with monitorInterval property
     */
    private static String maybeAddMonitorIntervalToExternalLogging(String data) {
        OrderedProperties orderedProperties = new OrderedProperties();
        orderedProperties.addStringPairs(data);

        Optional<String> mi = orderedProperties.asMap().keySet().stream()
                .filter(key -> key.matches("^monitorInterval$")).findFirst();
        if (mi.isPresent()) {
            return data;
        } else {
            // do not override custom value
            return data + "\nmonitorInterval=" + LOG4J2_MONITOR_INTERVAL_SECONDS + "\n";
        }
    }

    /**
     * Transforms map to Log4j properties file format. It also injects the refresh interval if needed.
     *
     * @param properties    Map of Log4j properties
     *
     * @return  Log4j properties as a String.
     */
    /* test */ static String createLog4jProperties(OrderedProperties properties) {
        return maybeAddMonitorIntervalToExternalLogging(
                properties.asPairsWithComment("Do not change this generated file. Logging can be configured in the corresponding Kubernetes resource.")
        );
    }

    /**
     * Read a properties file with default logging configuration and returns the properties in a deterministic order.
     *
     * @param reconciliation            Reconciliation marker
     * @param defaultLogConfigBaseName  Base of the file name with the default logging configuration. If it is null or
     *                                  empty, an empty OrderProperties object will be returned.
     *
     * @return  The OrderedProperties of the inputted file.
     */
    public static OrderedProperties defaultLogConfig(Reconciliation reconciliation, String defaultLogConfigBaseName) {
        if (defaultLogConfigBaseName == null || defaultLogConfigBaseName.isEmpty()) {
            return new OrderedProperties();
        } else {
            OrderedProperties properties = new OrderedProperties();
            String logConfigFile = "/default-logging/" + defaultLogConfigBaseName + ".properties";
            InputStream is = AbstractModel.class.getResourceAsStream(logConfigFile);

            if (is == null) {
                LOGGER.warnCr(reconciliation, "Cannot find resource '{}'", logConfigFile);
            } else {
                try {
                    properties.addStringPairs(is);
                } catch (IOException e) {
                    LOGGER.warnCr(reconciliation, "Unable to read default log config from '{}'", logConfigFile);
                } finally {
                    try {
                        is.close();
                    } catch (IOException e) {
                        LOGGER.errorCr(reconciliation, "Failed to close stream. Reason: " + e.getMessage());
                    }
                }
            }

            return properties;
        }
    }

    /**
     * Validates the logging configuration
     *
     * @param logging    Logging which should be validated
     */
    protected static void validateLogging(Logging logging)   {
        List<String> errors = new ArrayList<>();

        if (logging instanceof ExternalLogging externalLogging) {
            if (externalLogging.getValueFrom() != null
                    && externalLogging.getValueFrom().getConfigMapKeyRef() != null)   {
                // The Config Map reference exists
                if (externalLogging.getValueFrom().getConfigMapKeyRef().getName() == null
                        || externalLogging.getValueFrom().getConfigMapKeyRef().getName().isEmpty())  {
                    errors.add("Name of the Config Map with logging configuration is missing");
                }

                if (externalLogging.getValueFrom().getConfigMapKeyRef().getKey() == null
                        || externalLogging.getValueFrom().getConfigMapKeyRef().getKey().isEmpty())  {
                    errors.add("The key under which the logging configuration is stored in the ConfigMap is missing");
                }
            } else {
                // The Config Map reference is missing
                errors.add("Config Map reference is missing");
            }
        }

        if (!errors.isEmpty())  {
            throw new InvalidResourceException("Logging configuration is invalid: " + errors);
        }
    }

    /**
     * Load the properties and expand any variables of format ${NAME} inside values with resolved values.
     * Variables are resolved by looking up the property names only within the loaded map.
     *
     * @param env Multiline properties file as String
     * @return Multiline properties file as String with variables resolved
     */
    public static String expandVars(String env) {
        OrderedProperties ops = new OrderedProperties();
        ops.addStringPairs(env);
        Map<String, String> map = ops.asMap();
        StringBuilder resultBuilder = new StringBuilder();
        for (Map.Entry<String, String> entry: map.entrySet()) {
            resultBuilder.append(entry.getKey() + "=" + expandVar(entry.getValue(), ops.asMap()) + "\n");
        }
        return resultBuilder.toString();
    }

    /**
     * Search for occurrences of ${NAME} in the 'value' parameter and replace them with
     * the value for the NAME key in the 'env' map.
     *
     * @param value String value possibly containing variables of format: ${NAME}
     * @param env Var map with name:value pairs
     * @return Input string with variable references resolved
     */
    public static String expandVar(String value, Map<String, String> env) {
        StringBuilder sb = new StringBuilder();
        int endIdx = -1;
        int startIdx;
        int prefixLen = "${".length();
        while ((startIdx = value.indexOf("${", endIdx + 1)) != -1) {
            sb.append(value, endIdx + 1, startIdx);
            endIdx = value.indexOf("}", startIdx + prefixLen);
            if (endIdx != -1) {
                String key = value.substring(startIdx + prefixLen, endIdx);
                String resolved = env.get(key);
                sb.append(resolved != null ? resolved : "");
            } else {
                break;
            }
        }
        sb.append(value.substring(endIdx + 1));
        return sb.toString();
    }
}
