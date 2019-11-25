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

import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KafkaVersionTest {

    @Test
    public void loadVersionsFileTest() {
        KafkaVersion.Lookup loaded = new KafkaVersion.Lookup(emptyMap(), emptyMap(), emptyMap(), emptyMap());

        assertThat(loaded.supportedVersions().contains("2.2.0"), is(false));
        assertThat(loaded.supportedVersions().contains("2.2.1"), is(true));
        assertThat(loaded.supportedVersions().contains("2.3.0"), is(true));
        assertThat(loaded.supportedVersions().contains("2.3.1"), is(true));
        assertThat(loaded.version("2.2.1").version(), is("2.2.1"));
        assertThat(loaded.version("2.2.1").protocolVersion(), is("2.2"));
        assertThat(loaded.version("2.2.1").messageVersion(), is("2.2"));
        assertThat(loaded.version("2.3.0").version(), is("2.3.0"));
        assertThat(loaded.version("2.3.0").protocolVersion(), is("2.3"));
        assertThat(loaded.version("2.3.0").messageVersion(), is("2.3"));
        assertThat(loaded.version("2.3.1").version(), is("2.3.1"));
        assertThat(loaded.version("2.3.1").protocolVersion(), is("2.3"));
        assertThat(loaded.version("2.3.1").messageVersion(), is("2.3"));
    }

    @Test
    public void parsingTest() throws Exception {
        Map<String, KafkaVersion> map = new HashMap<>();
        KafkaVersion defaultVersion = KafkaVersion.parseKafkaVersions(
                new StringReader(KafkaVersionTestUtils.getKafkaVersionYaml()), map);
        assertThat(defaultVersion.version(), is(KafkaVersionTestUtils.DEFAULT_KAFKA_VERSION));
        assertThat(map.size(), is(3));
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
    public void compare() {
        assertThat(KafkaVersion.compareDottedVersions(KafkaVersionTestUtils.DEFAULT_KAFKA_VERSION,
                KafkaVersionTestUtils.DEFAULT_KAFKA_VERSION), is(0));
    }
}
