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
                // Inline logging as specified and some loggers are configured
                if (logging.isShouldPatchLoggerAppender()) {
                    String rootAppenderName = getRootAppenderNamesFromDefaultLoggingConfig(reconciliation, newSettings);
                    String newRootLogger = inlineLogging.getLoggers().get("log4j.rootLogger");
                    newSettings.addMapPairs(inlineLogging.getLoggers());

                    if (newRootLogger != null && !rootAppenderName.isEmpty() && !newRootLogger.contains(",")) {
                        // this should never happen as appender name is added in default configuration
                        LOGGER.debugCr(reconciliation, "Newly set rootLogger does not contain appender. Setting appender to {}.", rootAppenderName);
                        String level = newSettings.asMap().get("log4j.rootLogger");
                        newSettings.addPair("log4j.rootLogger", level + ", " + rootAppenderName);
                    }
                } else {
                    newSettings.addMapPairs(inlineLogging.getLoggers());
                }
            }

            return createLog4jProperties(newSettings, logging.isLog4j2());
        } else if (logging.getLogging() instanceof ExternalLogging externalLogging) {
            if (externalLogging.getValueFrom() != null && externalLogging.getValueFrom().getConfigMapKeyRef() != null && externalLogging.getValueFrom().getConfigMapKeyRef().getKey() != null) {
                if (externalCm != null && externalCm.getData() != null && externalCm.getData().containsKey(externalLogging.getValueFrom().getConfigMapKeyRef().getKey())) {
                    return maybeAddMonitorIntervalToExternalLogging(externalCm.getData().get(externalLogging.getValueFrom().getConfigMapKeyRef().getKey()), logging.isLog4j2());
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
            return createLog4jProperties(defaultLogConfig(reconciliation, logging.getDefaultLogConfigBaseName()), logging.isLog4j2());
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
     * Adds 'monitorInterval=30' to Log4j2 logging. If the logging configuration already has it or is not Log4j2,
     * returns the logging configuration without any change.
     *
     * @param data      String with Log4j2 properties in format key=value separated by new lines
     * @param isLog4j2  Indicator whether Log4j1 or Log4j2 logging is used
     *
     * @return  Log4j2 configuration with monitorInterval property
     */
    private static String maybeAddMonitorIntervalToExternalLogging(String data, boolean isLog4j2) {
        if (isLog4j2) {
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
        } else {
            return data;
        }
    }

    /**
     * Transforms map to Log4j properties file format. If Log4j2 logging is used, it also injects the refresh interval
     * if needed.
     *
     * @param properties    Map of Log4j properties
     * @param isLog4j2  Indicator whether Log4j1 or Log4j2 logging is used
     *
     * @return  Log4j properties as a String.
     */
    /* test */ static String createLog4jProperties(OrderedProperties properties, boolean isLog4j2) {
        return maybeAddMonitorIntervalToExternalLogging(
                properties.asPairsWithComment("Do not change this generated file. Logging can be configured in the corresponding Kubernetes resource."),
                isLog4j2
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
}
