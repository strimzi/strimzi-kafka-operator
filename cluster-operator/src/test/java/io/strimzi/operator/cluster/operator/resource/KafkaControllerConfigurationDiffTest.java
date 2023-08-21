/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.TestUtils;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.junit.jupiter.api.Test;

import static java.util.Collections.emptyMap;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class KafkaControllerConfigurationDiffTest {

    private static final int BROKER_ID = 0;

    @Test
    public void testChangedPresentValue() {
        Map<String, String> config = Map.of("controller.quorum.election.timeout.ms", "5000");
        KafkaControllerConfigurationDiff kcd = new KafkaControllerConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION,
                getCurrentConfiguration(emptyMap(), ""),
                getDesiredConfiguration(config),
                BROKER_ID);
        assertTrue(kcd.configsHaveChanged);
    }

    @Test
    public void testUnchangedPresentValue() {
        Map<String, String> config = Map.of("controller.quorum.election.timeout.ms", "1000");
        KafkaControllerConfigurationDiff kcd = new KafkaControllerConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION,
                getCurrentConfiguration(emptyMap(), ""),
                getDesiredConfiguration(config),
                BROKER_ID);
        assertFalse(kcd.configsHaveChanged);
    }

    @Test
    public void testChangedNonControllerValue() {
        Map<String, String> config = Map.of("min.insync.replicas", "2");
        KafkaControllerConfigurationDiff kcd = new KafkaControllerConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION,
                getCurrentConfiguration(emptyMap(), ""),
                getDesiredConfiguration(config),
                BROKER_ID);
        assertFalse(kcd.configsHaveChanged);
    }

    @Test
    public void testChangedControllerAndNonControllerValues() {
        Map<String, String> config = new HashMap<>();
        config.put("min.insync.replicas", "2");
        config.put("process.roles", "controller");
        KafkaControllerConfigurationDiff kcd = new KafkaControllerConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION,
                getCurrentConfiguration(emptyMap(), ""),
                getDesiredConfiguration(config),
                BROKER_ID);
        assertTrue(kcd.configsHaveChanged);
    }

    @Test
    public void testAddValue() {
        Map<String, String> config = Map.of("controller.quorum.election.timeout.ms", "1005");
        KafkaControllerConfigurationDiff kcd = new KafkaControllerConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION,
                getCurrentConfiguration(emptyMap(), "controller.quorum.election.timeout.ms"),
                getDesiredConfiguration(config),
                BROKER_ID);
        assertTrue(kcd.configsHaveChanged);
    }

    @Test
    public void testMoveValue() {
        Config currentConfig = new Config(List.of(new ConfigEntry("broker.session.timeout.ms", "1000")));
        String desiredConfig = "controller.quorum.election.timeout.ms=1000";
        KafkaControllerConfigurationDiff kcd = new KafkaControllerConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION,
                currentConfig, desiredConfig,
                BROKER_ID);
        assertTrue(kcd.configsHaveChanged);
    }

    @Test
    public void testRemoveValue() {
        List<ConfigEntry> currentConfigs = new ArrayList<>();
        currentConfigs.add(new ConfigEntry("broker.session.timeout.ms", "1000"));
        currentConfigs.add(new ConfigEntry("controller.quorum.election.timeout.ms", "1000"));
        String desiredConfig = "controller.quorum.election.timeout.ms=1000";
        KafkaControllerConfigurationDiff kcd = new KafkaControllerConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION,
                new Config(currentConfigs), desiredConfig,
                BROKER_ID);
        assertFalse(kcd.configsHaveChanged);
    }

    @Test
    public void testCustomPropertyAdded() {
        Map<String, String> config = Map.of("custom.property", "42");
        KafkaControllerConfigurationDiff kcd = new KafkaControllerConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION,
                getCurrentConfiguration(emptyMap(), ""),
                getDesiredConfiguration(config),
                BROKER_ID);
        assertFalse(kcd.configsHaveChanged);
    }

    @Test
    public void testCustomPropertyRemoved() {
        Map<String, String> config = Map.of("custom.property", "42");
        KafkaControllerConfigurationDiff kcd = new KafkaControllerConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION,
                getCurrentConfiguration(config, ""),
                getDesiredConfiguration(emptyMap()),
                BROKER_ID);
        assertFalse(kcd.configsHaveChanged);
    }

    @Test
    public void testCustomPropertyKept() {
        Map<String, String> config = Map.of("custom.property", "42");
        KafkaControllerConfigurationDiff kcd = new KafkaControllerConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION,
                getCurrentConfiguration(config, ""),
                getDesiredConfiguration(config),
                BROKER_ID);
        assertFalse(kcd.configsHaveChanged);
    }

    @Test
    public void testCustomPropertyChanged() {
        Map<String, String> config = Map.of("custom.property", "42");
        Map<String, String> config2 = Map.of("custom.property", "43");
        KafkaControllerConfigurationDiff kcd = new KafkaControllerConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION,
                getCurrentConfiguration(config, ""),
                getDesiredConfiguration(config2),
                BROKER_ID);
        assertFalse(kcd.configsHaveChanged);
    }

    @Test
    public void testNullCurrentConfig() {
        KafkaControllerConfigurationDiff kcd = new KafkaControllerConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION,
                null,
                getDesiredConfiguration(emptyMap()),
                BROKER_ID);
        assertFalse(kcd.configsHaveChanged);
    }

    @Test
    public void testNullDesiredConfig() {
        KafkaControllerConfigurationDiff kcd = new KafkaControllerConfigurationDiff(Reconciliation.DUMMY_RECONCILIATION,
                getCurrentConfiguration(emptyMap(), ""),
                null,
                BROKER_ID);
        assertFalse(kcd.configsHaveChanged);
    }

    private String getDesiredConfiguration(Map<String, String> additional) {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("desired-kafka-controller.conf")) {
            String desiredConfigString = TestUtils.readResource(is);
            String additionalConfigString = additional.keySet().stream().map((key) -> key + "=" + additional.get(key)).collect(Collectors.joining("\n"));
            return desiredConfigString + "\n" + additionalConfigString;
        } catch (IOException e) {
            fail(e);
        }
        return "";
    }

    private Config getCurrentConfiguration(Map<String, String> additional, String removeKey) {
        List<ConfigEntry> configEntries = new ArrayList<>();

        try (InputStream is = getClass().getClassLoader().getResourceAsStream("current-kafka-controller.conf")) {

            List<String> configList = Arrays.asList(TestUtils.readResource(is).split(System.getProperty("line.separator")));

            configList.forEach(entry -> {
                String[] split = entry.split("=");
                String value = split.length == 1 ? "" : split[1];
                String key = split[0].replace("\n", "");
                if (!key.equals(removeKey)) {
                    configEntries.add(new ConfigEntry(key, value));
                }
            });
            additional.forEach((key, value) -> configEntries.add(new ConfigEntry(key, value)));
        } catch (IOException e) {
            fail(e);
        }
        return new Config(configEntries);
    }
}
