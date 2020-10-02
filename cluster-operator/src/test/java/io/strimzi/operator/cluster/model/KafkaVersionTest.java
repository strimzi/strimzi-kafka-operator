/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KafkaVersionTest {

    @Test
    public void parsingTest() throws Exception {
        Map<String, KafkaVersion> map = new HashMap<>();
        KafkaVersion defaultVersion = KafkaVersion.parseKafkaVersions(
                new StringReader(KafkaVersionTestUtils.getKafkaVersionYaml()), map);
        assertThat(defaultVersion.version(), is(KafkaVersionTestUtils.DEFAULT_KAFKA_VERSION));
        assertThat(map.containsKey(KafkaVersionTestUtils.LATEST_KAFKA_VERSION), is(true));
        assertThat(map.get(KafkaVersionTestUtils.LATEST_KAFKA_VERSION).version(), is(KafkaVersionTestUtils.LATEST_KAFKA_VERSION));
        assertThat(map.get(KafkaVersionTestUtils.LATEST_KAFKA_VERSION).protocolVersion(), is(KafkaVersionTestUtils.LATEST_PROTOCOL_VERSION));
        assertThat(map.get(KafkaVersionTestUtils.LATEST_KAFKA_VERSION).messageVersion(), is(KafkaVersionTestUtils.LATEST_FORMAT_VERSION));
        assertThat(map.containsKey(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION), is(true));
        assertThat(map.get(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).version(), is(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION));
        assertThat(map.get(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).protocolVersion(), is(KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION));
        assertThat(map.get(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).messageVersion(), is(KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION));
    }

    @Test
    public void duplicateVersionTest() throws IOException {
        assertThrows(IllegalArgumentException.class, () -> {
            Map<String, KafkaVersion> map = new HashMap<>();
            KafkaVersion defaultVersion = KafkaVersion.parseKafkaVersions(
                    new StringReader(
                            KafkaVersionTestUtils.getPreviousVersionYaml(false) +
                                    KafkaVersionTestUtils.getPreviousVersionYaml(false) +
                                    KafkaVersionTestUtils.getLatestVersionYaml(true)
                    ), map);
        });
    }

    @Test
    public void noDefaultTest() throws IOException {
        assertThrows(RuntimeException.class, () -> {
            Map<String, KafkaVersion> map = new HashMap<>();
            KafkaVersion defaultVersion = KafkaVersion.parseKafkaVersions(
                    new StringReader(
                            KafkaVersionTestUtils.getPreviousVersionYaml(false) +
                                    KafkaVersionTestUtils.getLatestVersionYaml(false)),
                    map);
        });
    }

    @Test
    public void multipleDefaultTest() throws IOException {
        assertThrows(IllegalArgumentException.class, () -> {
            Map<String, KafkaVersion> map = new HashMap<>();
            KafkaVersion defaultVersion = KafkaVersion.parseKafkaVersions(
                    new StringReader(
                            KafkaVersionTestUtils.getPreviousVersionYaml(true) +
                                    KafkaVersionTestUtils.getLatestVersionYaml(true)
                    ), map);
        });
    }

    @Test
    public void compareEqualVersionTest() {
        assertThat(KafkaVersion.compareDottedVersions(KafkaVersionTestUtils.DEFAULT_KAFKA_VERSION, KafkaVersionTestUtils.DEFAULT_KAFKA_VERSION), is(0));
    }

    @Test
    public void compareVersionLowerTest() {
        assertThat(KafkaVersion.compareDottedVersions(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION, KafkaVersionTestUtils.LATEST_KAFKA_VERSION), lessThan(0));
    }

    @Test
    public void compareVersionHigherTest() {
        assertThat(KafkaVersion.compareDottedVersions(KafkaVersionTestUtils.LATEST_KAFKA_VERSION, KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION), greaterThan(0));
    }

}
