/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.logging;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.strimzi.api.kafka.model.common.HasConfigurableLogging;
import io.strimzi.api.kafka.model.common.Logging;
import io.strimzi.operator.common.Reconciliation;

/**
 * Represents a model for logging configuration
 */
public class LoggingModel {
    /**
     * Key under which the Log4j1 properties are stored in the ConfigMap
     */
    public static final String LOG4J1_CONFIG_MAP_KEY = "log4j.properties";

    /**
     * Key under which the Log4j2 properties are stored in the ConfigMap
     */
    public static final String LOG4J2_CONFIG_MAP_KEY = "log4j2.properties";

    private final Logging logging;
    private final String defaultLogConfigBaseName;
    private final boolean isLog4j2; // Indicates whether Log4j2 is used => if set to false, uses Log4j1
    private final boolean shouldPatchLoggerAppender;

    /**
     * Constructs the Logging Model for managing logging
     *
     * @param specSection                   Custom resource section configuring logging
     * @param defaultLogConfigBaseName      Base of the file name with the default logging configuration
     * @param isLog4j2                      Indicates whether this logging is based on Log4j1 or Log4j2
     * @param shouldPatchLoggerAppender     Indicates whether logger appenders should be patched or not
     */
    public LoggingModel(HasConfigurableLogging specSection, String defaultLogConfigBaseName, boolean isLog4j2, boolean shouldPatchLoggerAppender) {
        LoggingUtils.validateLogging(specSection.getLogging());

        this.logging = specSection.getLogging();
        this.defaultLogConfigBaseName = defaultLogConfigBaseName;
        this.isLog4j2 = isLog4j2;
        this.shouldPatchLoggerAppender = shouldPatchLoggerAppender;
    }

    /**
     * Generates the logging configuration as a String. The configuration is generated based on the default logging
     * configuration files from resources, the (optional) inline logging configuration from the custom resource
     * and the (optional) external logging configuration in a user-provided ConfigMap.
     *
     * @param reconciliation    Reconciliation marker
     * @param externalCm        The user-provided ConfigMap with custom Log4j / Log4j2 file
     *
     * @return String with the Log4j / Log4j2 properties used for configuration
     */
    public String loggingConfiguration(Reconciliation reconciliation, ConfigMap externalCm) {
        return LoggingUtils
                .loggingConfiguration(
                        reconciliation,
                        this,
                        externalCm
                );
    }

    /**
     * @return  The key under which the logging configuration should be stored in the Config Map
     */
    public String configMapKey()    {
        return isLog4j2 ? LOG4J2_CONFIG_MAP_KEY : LOG4J1_CONFIG_MAP_KEY;
    }

    /**
     * @return  The logging configuration from the custom resource
     */
    public Logging getLogging() {
        return logging;
    }

    /**
     * @return  Base of the file name with the default logging configuration
     */
    public String getDefaultLogConfigBaseName() {
        return defaultLogConfigBaseName;
    }

    /**
     * @return  Indicates whether this logging is based on Log4j1 or Log4j2
     */
    public boolean isLog4j2() {
        return isLog4j2;
    }

    /**
     * @return  Indicates whether logger appenders should be patched or not
     */
    public boolean isShouldPatchLoggerAppender() {
        return shouldPatchLoggerAppender;
    }
}