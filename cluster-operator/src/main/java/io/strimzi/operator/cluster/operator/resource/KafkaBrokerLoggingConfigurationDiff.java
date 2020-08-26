/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.operator.resource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.fabric8.zjsonpatch.JsonDiff;
import io.strimzi.operator.common.model.OrderedProperties;
import io.strimzi.operator.common.operator.resource.AbstractResourceDiff;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.fabric8.kubernetes.client.internal.PatchUtils.patchMapper;

public class KafkaBrokerLoggingConfigurationDiff extends AbstractResourceDiff {

    private static final Logger log = LogManager.getLogger(KafkaBrokerLoggingConfigurationDiff.class);
    private final Collection<AlterConfigOp> diff;
    private int brokerId;

    private static final HashSet VALID_LOGGER_LEVELS = new HashSet<>(Arrays.asList("INFO", "ERROR", "WARN", "TRACE", "DEBUG", "FATAL", "OFF"));

    public KafkaBrokerLoggingConfigurationDiff(Config brokerConfigs, String desired, int brokerId) {
        this.brokerId = brokerId;
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
        Map<String, String> currentMap;
        Collection<AlterConfigOp> updatedCE = new ArrayList<>();
        currentMap = brokerConfigs.entries().stream().collect(
            Collectors.toMap(
                ConfigEntry::name,
                configEntry -> configEntry.value() == null ? "null" : configEntry.value()));

        OrderedProperties orderedProperties = new OrderedProperties();
        desired = desired.replaceAll("log4j\\.logger\\.", "");
        desired = desired.replaceAll("log4j\\.rootLogger", "root");
        orderedProperties.addStringPairs(desired);
        Map<String, String> desiredMap = orderedProperties.asMap();

        ObjectMapper orderedMapper = patchMapper().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

        JsonNode source = orderedMapper.valueToTree(currentMap);
        JsonNode target = orderedMapper.valueToTree(desiredMap);
        JsonNode jsonDiff = JsonDiff.asJson(source, target);

        for (JsonNode d : jsonDiff) {
            String pathValue = d.get("path").asText();
            String pathValueWithoutSlash = pathValue.substring(1);

            Optional<ConfigEntry> optEntry = brokerConfigs.entries().stream()
                    .filter(configEntry -> configEntry.name().equals(pathValueWithoutSlash))
                    .findFirst();

            if (pathValueWithoutSlash.equals("log4j.rootLogger")) {
                if (!desiredMap.get(pathValueWithoutSlash).matches(".+,.+")) {
                    log.warn("Broker {} logging: Logger log4j.rootLogger should contain level and appender, e.g. \'log4j.rootLogger = INFO, CONSOLE\'", brokerId);
                }
            }
            String op = d.get("op").asText();
            if (optEntry.isPresent()) {
                ConfigEntry entry = optEntry.get();
                if ("remove".equals(op)) {
                    removeProperty(updatedCE, pathValueWithoutSlash, entry);
                } else if ("replace".equals(op)) {
                    // entry is in the current, desired is updated value
                    if (!entry.value().equals(parseLogLevelFromAppenderCouple(desiredMap.get(entry.name())))) {
                        updateOrAdd(entry.name(), desiredMap, updatedCE);
                    }
                }
            } else {
                if ("add".equals(op)) {
                    // entry is not in the current, it is added
                    updateOrAdd(pathValueWithoutSlash, desiredMap, updatedCE);
                }
            }

            log.debug("Kafka Broker {} Logging Config Differs : {}", brokerId, d);
            log.debug("Current Kafka Broker Logging Config path {} has value {}", pathValueWithoutSlash, lookupPath(source, pathValue));
            log.debug("Desired Kafka Broker Logging Config path {} has value {}", pathValueWithoutSlash, lookupPath(target, pathValue));
        }
        return updatedCE;
    }

    private static String parseLogLevelFromAppenderCouple(String level) {
        int index = level.indexOf(",");
        if (index > 0) {
            return level.substring(0, index).trim();
        } else {
            return level.trim();
        }
    }

    private static void updateOrAdd(String propertyName, Map<String, String> desiredMap, Collection<AlterConfigOp> updatedCE) {
        if (!propertyName.contains("log4j.appender") && !propertyName.equals("monitorInterval")) {
            String level = parseLogLevelFromAppenderCouple(desiredMap.get(propertyName));
            if (isValidLoggerLevel(level)) {
                updatedCE.add(new AlterConfigOp(new ConfigEntry(propertyName, level), AlterConfigOp.OpType.SET));
                log.trace("{} not set in current or has deprecated value. Setting to {}", propertyName, level);
            } else {
                log.warn("Level {} is not valid logging level", level);
            }
        }
    }

    /**
     * All loggers can be set dynamically. If the logger is not set in desire, set it to ERROR. Loggers with already set to ERROR should be skipped.
     * ERROR is set as inactive because log4j does not support OFF logger value.
     * We want to skip "root" logger as well to avoid duplicated key in alterConfigOps collection.
     * @param alterConfigOps collection of AlterConfigOp
     * @param pathValueWithoutSlash name of "removed" logger
     * @param entry entry to be removed (set to ERROR)
     */
    private static void removeProperty(Collection<AlterConfigOp> alterConfigOps, String pathValueWithoutSlash, ConfigEntry entry) {
        if (!pathValueWithoutSlash.contains("log4j.appender") && !pathValueWithoutSlash.equals("root") && !"ERROR".equals(entry.value())) {
            alterConfigOps.add(new AlterConfigOp(new ConfigEntry(pathValueWithoutSlash, "ERROR"), AlterConfigOp.OpType.SET));
            log.trace("{} not set in desired, setting to ERROR", entry.name());
        }
    }

    /**
     * @return whether the current config and the desired config are identical (thus, no update is necessary).
     */
    @Override
    public boolean isEmpty() {
        return  diff.size() == 0;
    }

    private static boolean isValidLoggerLevel(String level) {
        return VALID_LOGGER_LEVELS.contains(level);
    }

}
