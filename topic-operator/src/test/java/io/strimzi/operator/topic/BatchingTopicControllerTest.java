/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import org.apache.kafka.clients.admin.ConfigEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BatchingTopicControllerTest {
    @ParameterizedTest
    @CsvSource({
        "1, 1.0",
        "1.00, 1.0",
        "8e-1, 0.8",
        "8e-2, 0.08",
        "1.024E8, 102400000.0",
        "-0.0, 0.0"
    })
    void shouldTreatEquivalentDoubleRepresentationsAsEqual(String specValue, String kafkaValue) {
        assertTrue(BatchingTopicController.configValuesEqual(specValue, configEntry(ConfigEntry.ConfigType.DOUBLE, kafkaValue)));
    }

    @ParameterizedTest
    @CsvSource(value = {
        "0.8, 0.9",
        "0.5, 0.5000000000000001",
        "invalid, 0.5",
        "0.5, invalid",
        "'0,5', 0.5",
        "'', 0.5",
        "null, 0.5",
        "0.5, null"
    }, nullValues = "null")
    void shouldRetainChangedOrInvalidDoubleValues(String specValue, String kafkaValue) {
        assertFalse(BatchingTopicController.configValuesEqual(specValue, configEntry(ConfigEntry.ConfigType.DOUBLE, kafkaValue)));
    }

    @Test
    void shouldCompareNonDoubleAndUnknownValuesAsStrings() {
        assertFalse(BatchingTopicController.configValuesEqual("1", configEntry(ConfigEntry.ConfigType.STRING, "1.0")));
        assertFalse(BatchingTopicController.configValuesEqual("1", configEntry(ConfigEntry.ConfigType.UNKNOWN, "1.0")));
    }

    private static ConfigEntry configEntry(ConfigEntry.ConfigType type, String value) {
        return new ConfigEntry("config", value, ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG,
            false, false, List.of(), type, null);
    }
}
