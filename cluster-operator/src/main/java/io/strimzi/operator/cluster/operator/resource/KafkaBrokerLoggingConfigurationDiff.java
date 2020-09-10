/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.api.kafka.model.Logging;
import io.strimzi.operator.common.model.OrderedProperties;
import io.strimzi.operator.common.operator.resource.AbstractResourceDiff;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class KafkaBrokerLoggingConfigurationDiff extends AbstractResourceDiff {

    private static final Logger log = LogManager.getLogger(KafkaBrokerLoggingConfigurationDiff.class);
    private final Collection<AlterConfigOp> diff;

    public KafkaBrokerLoggingConfigurationDiff(Config brokerConfigs, String desired, int brokerId) {
        this.diff = diff(brokerId, desired, brokerConfigs);
    }

    /**
     * Returns logging difference
     * @return Collection of AlterConfigOp containing difference between current and desired logging configuration
     */
    public Collection<AlterConfigOp> getLoggingDiff() {
        return diff;
    }

    /**
     * @return The number of broker configs which are different.
     */
    public int getDiffSize() {
        return diff.size();
    }

    /**
     * Computes logging diff
     * @param brokerId id of compared broker
     * @param desired desired logging configuration, may be null if the related ConfigMap does not exist yet or no changes are required
     * @param brokerConfigs current configuration
     * @return Collection of AlterConfigOp containing all entries which were changed from current in desired configuration
     */
    private static Collection<AlterConfigOp> diff(int brokerId, String desired,
                                                  Config brokerConfigs) {
        if (brokerConfigs == null || desired == null) {
            return Collections.emptyList();
        }

        Collection<AlterConfigOp> updatedCE = new ArrayList<>();

        OrderedProperties orderedProperties = new OrderedProperties();
        desired = desired.replaceAll("log4j\\.logger\\.", "");
        desired = desired.replaceAll("log4j\\.rootLogger", "root");
        orderedProperties.addStringPairs(desired);
        Map<String, String> desiredMap = orderedProperties.asMap();

        LoggingLevelResolver levelResolver = new LoggingLevelResolver(desiredMap);

        for (ConfigEntry entry: brokerConfigs.entries()) {
            LoggingLevel desiredLevel;
            try {
                desiredLevel = levelResolver.resolveLevel(entry.name());
            } catch (IllegalArgumentException e) {
                log.warn("Skipping {} - it is configured with an unsupported value (\"{}\")", entry.name(), e.getMessage());
                continue;
            }

            if (!desiredLevel.name().equals(entry.value())) {
                updatedCE.add(new AlterConfigOp(new ConfigEntry(entry.name(), desiredLevel.name()), AlterConfigOp.OpType.SET));
                log.trace("{} has an outdated value. Setting to {}", entry.name(), desiredLevel.name());
            }
        }

        for (Map.Entry<String, String> ent: desiredMap.entrySet()) {
            String name = ent.getKey();
            if (name.startsWith("log4j.appender")) {
                continue;
            }
            ConfigEntry configEntry = brokerConfigs.get(name);
            if (configEntry == null) {
                String level = LoggingLevel.nameOrDefault(LoggingLevel.ofLog4jConfig(ent.getValue()), LoggingLevel.WARN);
                updatedCE.add(new AlterConfigOp(new ConfigEntry(name, level), AlterConfigOp.OpType.SET));
                log.trace("{} not set. Setting to {}", name, level);
            }
        }

        return updatedCE;
    }

    /**
     * @return whether the current config and the desired config are identical (thus, no update is necessary).
     */
    @Override
    public boolean isEmpty() {
        return diff.size() == 0;
    }

    /**
     * This internal class calculates the logging level of an arbitrary category based on the logging configuration.
     *
     * It takes Log4j properties configuration in the form of a map of key:value pairs,
     * where key is the category name, and the value is whatever comes to the right of '=' sign in log4j.properties,
     * which is either a logging level, or a logging level followed by a comma, and followed by the appender name.
     */
    static class LoggingLevelResolver {

        private final Map<String, String> config;

        LoggingLevelResolver(Map<String, String> loggingConfig) {
            this.config = loggingConfig;
        }

        /**
         * The method that returns the logging level of the category
         * based on logging configuration, taking inheritance into account.
         *
         * For example, if looking for a logging level for 'io.strimzi.kafka.oauth.server.OAuthKafkaPrincipalBuilder',
         * the following configuration lookups are performed until one is found:
         * <ul>
         *     <li>io.strimzi.kafka.oauth.server.OAuthKafkaPrincipalBuilder</li>
         *     <li>io.strimzi.kafka.oauth.server</li>
         *     <li>io.strimzi.kafka.oauth</li>
         *     <li>io.strimzi.kafka</li>
         *     <li>io.strimzi</li>
         *     <li>io</li>
         *     <li>root</li>
         * </ul>
         *
         * When the configured level is ALL, the actual level returned will be TRACE.
         * Similarly when the configured level is OFF, the actual level returned will be FATAL.
         *
         * If logging level can't be parsed from the configuration (unsupported or badly formatted)
         * the method returns WARN level. The rationale is that making a configuration mistake in logging level
         * should not accidentally trigger a massive amount of logging.
         *
         * @param name The logging category name
         * @return The logging level compatible with dynamic logging update
         */
        LoggingLevel resolveLevel(String name) {
            String level = config.get(name);
            if (level != null) {
                LoggingLevel result = LoggingLevel.ofLog4jConfig(level);
                return result != null ? result : LoggingLevel.WARN;
            }

            int e = name.length();
            while (e > -1) {
                e = name.lastIndexOf('.', e);
                if (e == -1) {
                    level = config.get("root");
                } else {
                    level = config.get(name.substring(0, e));
                }
                if (level != null) {
                    LoggingLevel result = LoggingLevel.ofLog4jConfig(level);
                    return result != null ? result : LoggingLevel.WARN;
                }
                e -= 1;
            }
            // still here? Not even root logger defined?
            return LoggingLevel.WARN;
        }
    }

    enum LoggingLevel {
        OFF,
        FATAL,
        ERROR,
        WARN,
        INFO,
        DEBUG,
        TRACE,
        ALL;

        static LoggingLevel ofLog4jConfig(String value) {
            if (value != null && !"".equals(value)) {
                String v = value.split(",")[0].trim();
                if ("ALL".equals(v)) {
                    return TRACE;
                } else if ("OFF".equals(v)) {
                    return FATAL;
                } else {
                    try {
                        return valueOf(v);
                    } catch (RuntimeException e) {
                        log.warn("Invalid logging level: {}. Using WARN as a failover.", v);
                    }
                }
            }
            return null;
        }

        static String nameOrDefault(LoggingLevel level, LoggingLevel failover) {
            return level != null ? level.name() : failover.name();
        }
    }
}
