/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KafkaVersionTest {

    @Test
    public void loadVersionsFileTest() {
        KafkaVersion.Lookup loaded = new KafkaVersion.Lookup(emptyMap(), emptyMap(), emptyMap(), emptyMap());
        assertTrue(loaded.supportedVersions().contains("2.2.1"));
        assertTrue(loaded.supportedVersions().contains("2.3.0"));
        assertEquals("2.2.1", loaded.version("2.2.1").version());
        assertEquals("2.2", loaded.version("2.2.1").protocolVersion());
        assertEquals("2.2", loaded.version("2.2.1").messageVersion());
        assertEquals("2.3.0", loaded.version("2.3.0").version());
        assertEquals("2.3", loaded.version("2.3.0").protocolVersion());
        assertEquals("2.3", loaded.version("2.3.0").messageVersion());
    }

    @Test
    public void parsingTest() throws Exception {
        Map<String, KafkaVersion> map = new HashMap<>();
        KafkaVersion defaultVersion = KafkaVersion.parseKafkaVersions(
                new StringReader(KafkaVersionTestUtils.getKafkaVersionYaml()), map);
        assertEquals(KafkaVersionTestUtils.DEFAULT_KAFKA_VERSION, defaultVersion.version());
        assertEquals(3, map.size());
        assertTrue(map.containsKey(KafkaVersionTestUtils.LATEST_KAFKA_VERSION));
        assertEquals(KafkaVersionTestUtils.LATEST_KAFKA_VERSION,
                map.get(KafkaVersionTestUtils.LATEST_KAFKA_VERSION).version());
        assertEquals(KafkaVersionTestUtils.LATEST_PROTOCOL_VERSION,
                map.get(KafkaVersionTestUtils.LATEST_KAFKA_VERSION).protocolVersion());
        assertEquals(KafkaVersionTestUtils.LATEST_FORMAT_VERSION,
                map.get(KafkaVersionTestUtils.LATEST_KAFKA_VERSION).messageVersion());
        assertTrue(map.containsKey(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION));
        assertEquals(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION,
                map.get(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).version());
        assertEquals(KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION,
                map.get(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).protocolVersion());
        assertEquals(KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                map.get(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION).messageVersion());
    }

    @Test(expected = IllegalArgumentException.class)
    public void duplicateVersionTest() throws IOException {
        Map<String, KafkaVersion> map = new HashMap<>();
        KafkaVersion defaultVersion = KafkaVersion.parseKafkaVersions(
                new StringReader(
                        KafkaVersionTestUtils.getPreviousVersionYaml(false) +
                                KafkaVersionTestUtils.getPreviousVersionYaml(false) +
                                KafkaVersionTestUtils.getLatestVersionYaml(true)
                ), map);
    }

    @Test(expected = RuntimeException.class)
    public void noDefaultTest() throws IOException {
        Map<String, KafkaVersion> map = new HashMap<>();
        KafkaVersion defaultVersion = KafkaVersion.parseKafkaVersions(
                new StringReader(
                        KafkaVersionTestUtils.getPreviousVersionYaml(false) +
                        KafkaVersionTestUtils.getLatestVersionYaml(false)),
                map);
    }

    @Test(expected = IllegalArgumentException.class)
    public void multipleDefaultTest() throws IOException {
        Map<String, KafkaVersion> map = new HashMap<>();
        KafkaVersion defaultVersion = KafkaVersion.parseKafkaVersions(
                new StringReader(
                        KafkaVersionTestUtils.getPreviousVersionYaml(true) +
                        KafkaVersionTestUtils.getLatestVersionYaml(true)
                ), map);
    }

    @Test
    public void compare() {
        assertEquals(0, KafkaVersion.compareDottedVersions(KafkaVersionTestUtils.DEFAULT_KAFKA_VERSION,
                KafkaVersionTestUtils.DEFAULT_KAFKA_VERSION));
    }
}
