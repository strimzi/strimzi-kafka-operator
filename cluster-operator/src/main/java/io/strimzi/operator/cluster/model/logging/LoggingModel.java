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
     * Key under which the Log4j2 properties are stored in the ConfigMap
     */
    public static final String LOG4J2_CONFIG_MAP_KEY = "log4j2.properties";

    private final Logging logging;
    private final String defaultLogConfigBaseName;

    /**
     * Constructs the Logging Model for managing logging
     *
     * @param specSection                   Custom resource section configuring logging
     * @param defaultLogConfigBaseName      Base of the file name with the default logging configuration
     */
    public LoggingModel(HasConfigurableLogging specSection, String defaultLogConfigBaseName) {
        LoggingUtils.validateLogging(specSection.getLogging());

        this.logging = specSection.getLogging();
        this.defaultLogConfigBaseName = defaultLogConfigBaseName;
    }

    /**
     * Generates the logging configuration as a String. The configuration is generated based on the default logging
     * configuration files from resources, the (optional) inline logging configuration from the custom resource
     * and the (optional) external logging configuration in a user-provided ConfigMap.
     *
     * @param reconciliation    Reconciliation marker
     * @param externalCm        The user-provided ConfigMap with custom Log4j2 file
     *
     * @return String with the Log4j2 properties used for configuration
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
}