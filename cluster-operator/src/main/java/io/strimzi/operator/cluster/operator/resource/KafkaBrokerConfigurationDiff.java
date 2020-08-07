/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.operator.resource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.fabric8.zjsonpatch.JsonDiff;
import io.strimzi.kafka.config.model.ConfigModel;
import io.strimzi.kafka.config.model.Scope;
import io.strimzi.operator.cluster.model.KafkaConfiguration;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.common.model.OrderedProperties;
import io.strimzi.operator.common.operator.resource.AbstractResourceDiff;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.fabric8.kubernetes.client.internal.PatchUtils.patchMapper;

/**
 The algorithm:
 *  1. Create a map from the supplied desired String
 *  2. Fill placeholders (e.g. ${BROKER_ID}) in desired map as the broker's {@code kafka_config_generator.sh} would
 *  3a. Loop over all entries. If the entry is in IGNORABLE_PROPERTIES or entry.value from desired is equal to entry.value from current, do nothing
 *      else add it to the diff
 *  3b. If entry was removed from desired, add it to the diff with null value.
 *  3c. If custom entry was removed, delete property
 */
public class KafkaBrokerConfigurationDiff extends AbstractResourceDiff {

    private static final Logger log = LogManager.getLogger(KafkaBrokerConfigurationDiff.class);
    private final Collection<AlterConfigOp> diff;
    private int brokerId;
    private Map<String, ConfigModel> configModel;

    /**
     * These options are skipped because they contain placeholders
     * 909[1-4] is for skipping all (internal, plain, secured, external) listeners properties
     */
    public static final Pattern IGNORABLE_PROPERTIES = Pattern.compile(
            "^(broker\\.id"
            + "|.*-909[1-4]\\.ssl\\.keystore\\.location"
            + "|.*-909[1-4]\\.ssl\\.keystore\\.password"
            + "|.*-909[1-4]\\.ssl\\.keystore\\.type"
            + "|.*-909[1-4]\\.ssl\\.truststore\\.location"
            + "|.*-909[1-4]\\.ssl\\.truststore\\.password"
            + "|.*-909[1-4]\\.ssl\\.truststore\\.type"
            + "|.*-909[1-4]\\.ssl\\.client\\.auth"
            + "|.*-909[1-4]\\.scram-sha-512\\.sasl\\.jaas\\.config"
            + "|.*-909[1-4]\\.sasl\\.enabled\\.mechanisms"
            + "|advertised\\.listeners"
            + "|zookeeper\\.connect"
            + "|zookeeper\\.ssl\\..*"
            + "|zookeeper\\.clientCnxnSocket"
            + "|broker\\.rack)$");

    public KafkaBrokerConfigurationDiff(Config brokerConfigs, String desired, KafkaVersion kafkaVersion, int brokerId) {
        this.configModel = KafkaConfiguration.readConfigModel(kafkaVersion);
        this.brokerId = brokerId;
        this.diff = diff(brokerId, desired, brokerConfigs, configModel);
    }

    private static void fillPlaceholderValue(Map<String, String> orderedProperties, String placeholder, String value) {
        orderedProperties.entrySet().forEach(entry -> {
            entry.setValue(entry.getValue().replaceAll("\\$\\{" + Pattern.quote(placeholder) + "\\}", value));
        });
    }

    /**
     * Returns true if property in desired map has a default value
     * @param key name of the property
     * @param config Config which contains property
     * @return true if property in desired map has a default value
     */
    boolean isDesiredPropertyDefaultValue(String key, Config config) {
        Optional<ConfigEntry> entry = config.entries().stream().filter(configEntry -> configEntry.name().equals(key)).findFirst();
        if (entry.isPresent()) {
            return entry.get().isDefault();
        }
        return false;
    }

    public boolean canBeUpdatedDynamically() {
        boolean result = true;
        for (AlterConfigOp entry : diff) {
            if (isEntryReadOnly(entry.configEntry())) {
                result = false;
            }
        }
        return result;
    }

    /**
     * @param entry tested ConfigEntry
     * @return true if the entry is READ_ONLY
     */
    private boolean isEntryReadOnly(ConfigEntry entry) {
        return configModel.get(entry.name()).getScope().equals(Scope.READ_ONLY);
    }

    /**
     * Returns configuration difference
     * @return Collection of AlterConfigOp containing difference between current and desired configuration
     */
    public Collection<AlterConfigOp> getConfigDiff() {
        return diff;
    }

    /**
     * @return The number of broker configs which are different.
     */
    public int getDiffSize() {
        return diff.size();
    }

    private static boolean isIgnorableProperty(String key) {
        return IGNORABLE_PROPERTIES.matcher(key).matches();
    }

    /**
     * Computes diff between two maps. Entries in IGNORABLE_PROPERTIES are skipped
     * @param brokerId id of compared broker
     * @param desired desired configuration, may be null if the related ConfigMap does not exist yet or no changes are required
     * @param brokerConfigs current configuration
     * @param configModel default configuration for {@code kafkaVersion} of broker
     * @return Collection of AlterConfigOp containing all entries which were changed from current in desired configuration
     */
    private static Collection<AlterConfigOp> diff(int brokerId, String desired,
                                                  Config brokerConfigs,
                                                  Map<String, ConfigModel> configModel) {
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
        orderedProperties.addStringPairs(desired);
        Map<String, String> desiredMap = orderedProperties.asMap();

        fillPlaceholderValue(desiredMap, "STRIMZI_BROKER_ID", Integer.toString(brokerId));

        JsonNode source = patchMapper().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true).valueToTree(currentMap);
        JsonNode target = patchMapper().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true).valueToTree(desiredMap);
        JsonNode jsonDiff = JsonDiff.asJson(source, target);

        for (JsonNode d : jsonDiff) {
            String pathValue = d.get("path").asText();
            String pathValueWithoutSlash = pathValue.substring(1);

            Optional<ConfigEntry> optEntry = brokerConfigs.entries().stream()
                    .filter(configEntry -> configEntry.name().equals(pathValueWithoutSlash))
                    .findFirst();

            String op = d.get("op").asText();
            if (optEntry.isPresent()) {
                ConfigEntry entry = optEntry.get();
                if ("remove".equals(op)) {
                    removeProperty(configModel, updatedCE, pathValueWithoutSlash, entry);
                } else if ("replace".equals(op)) {
                    // entry is in the current, desired is updated value
                    updateOrAdd(entry.name(), configModel, desiredMap, updatedCE);
                }
            } else {
                if ("add".equals(op)) {
                    // entry is not in the current, it is added
                    updateOrAdd(pathValueWithoutSlash, configModel, desiredMap, updatedCE);
                }
            }

            log.debug("Kafka Broker {} Config Differs : {}", brokerId, d);
            log.debug("Current Kafka Broker Config path {} has value {}", pathValueWithoutSlash, lookupPath(source, pathValue));
            log.debug("Desired Kafka Broker Config path {} has value {}", pathValueWithoutSlash, lookupPath(target, pathValue));
        }

        return updatedCE;
    }

    private static void updateOrAdd(String propertyName, Map<String, ConfigModel> configModel, Map<String, String> desiredMap, Collection<AlterConfigOp> updatedCE) {
        if (!isIgnorableProperty(propertyName)) {
            if (isCustomEntry(propertyName, configModel)) {
                log.trace("custom property {} has been updated/added {}", propertyName, desiredMap.get(propertyName));
            } else {
                log.trace("property {} has been updated/added {}", propertyName, desiredMap.get(propertyName));
                updatedCE.add(new AlterConfigOp(new ConfigEntry(propertyName, desiredMap.get(propertyName)), AlterConfigOp.OpType.SET));
            }
        } else {
            log.trace("{} is ignorable, not considering");
        }
    }

    private static void removeProperty(Map<String, ConfigModel> configModel, Collection<AlterConfigOp> updatedCE, String pathValueWithoutSlash, ConfigEntry entry) {
        if (isCustomEntry(entry.name(), configModel)) {
            // we are deleting custom option
            log.trace("removing custom property {}", entry.name());
        } else if (entry.isDefault()) {
            // entry is in current, is not in desired, is default -> it uses default value, skip.
            // Some default properties do not have set ConfigEntry.ConfigSource.DEFAULT_CONFIG and thus
            // we are removing property. That might cause redundant RU. To fix this we would have to add defaultValue
            // to the configModel
            log.trace("{} not set in desired, using default value", entry.name());
        } else {
            // entry is in current, is not in desired, is not default -> it was using non-default value and was removed
            // if the entry was custom, it should be deleted
            if (!isIgnorableProperty(pathValueWithoutSlash)) {
                updatedCE.add(new AlterConfigOp(new ConfigEntry(pathValueWithoutSlash, null), AlterConfigOp.OpType.DELETE));
                log.trace("{} not set in desired, unsetting back to default {}", entry.name(), "deleted entry");
            } else {
                log.trace("{} is ignorable, not considering as removed");
            }
        }
    }

    /**
     * @return whether the current config and the desired config are identical (thus, no update is necessary).
     */
    @Override
    public boolean isEmpty() {
        return  diff.size() == 0;
    }

    /**
     * For some reason not all default entries have set ConfigEntry.ConfigSource.DEFAULT_CONFIG so we need to compare
     * @param entryName tested ConfigEntry
     * @param configModel configModel
     * @return true if entry is custom (not default)
     */
    private static boolean isCustomEntry(String entryName, Map<String, ConfigModel> configModel) {
        return !configModel.keySet().contains(entryName);
    }

}
