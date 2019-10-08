/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

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
                new StringReader(
                        "- version: 2.2.1\n" +
                           "  format: 2.2\n" +
                           "  protocol: 2.2\n" +
                           "  checksum: ABCDE1234\n" +
                           "  third-party-libs: 2.2.x\n" +
                           "  default: false\n" +
                           "- version: 2.3.0\n" +
                           "  format: 2.3\n" +
                           "  protocol: 2.3\n" +
                           "  checksum: ABCDE1234\n" +
                           "  third-party-libs: 2.3.x\n" +
                           "  default: true"
                ), map);
        assertEquals("2.3.0", defaultVersion.version());
        assertEquals(2, map.size());
        assertTrue(map.containsKey("2.3.0"));
        assertEquals("2.3.0", map.get("2.3.0").version());
        assertEquals("2.3", map.get("2.3.0").protocolVersion());
        assertEquals("2.3", map.get("2.3.0").messageVersion());
        assertTrue(map.containsKey("2.2.1"));
        assertEquals("2.2.1", map.get("2.2.1").version());
        assertEquals("2.2", map.get("2.2.1").protocolVersion());
        assertEquals("2.2", map.get("2.2.1").messageVersion());
    }

    @Test(expected = IllegalArgumentException.class)
    public void duplicateVersionTest() throws IOException {
        Map<String, KafkaVersion> map = new HashMap<>();
        KafkaVersion defaultVersion = KafkaVersion.parseKafkaVersions(
                new StringReader(
                        "- version: 2.2.1\n" +
                                "  format: 2.2\n" +
                                "  protocol: 2.2\n" +
                                "  checksum: ABCDE1234\n" +
                                "  third-party-libs: 2.2.x\n" +
                                "  default: false\n" +
                                "- version: 2.2.1\n" +
                                "  format: 2.2\n" +
                                "  protocol: 2.2\n" +
                                "  checksum: ABCDE1234\n" +
                                "  third-party-libs: 2.2.x\n" +
                                "  default: false\n" +
                                "- version: 2.3.0\n" +
                                "  format: 2.3\n" +
                                "  protocol: 2.3\n" +
                                "  checksum: ABCDE1234\n" +
                                "  third-party-libs: 2.3.x\n" +
                                "  default: true"
                ), map);
    }

    @Test(expected = RuntimeException.class)
    public void noDefaultTest() throws IOException {
        Map<String, KafkaVersion> map = new HashMap<>();
        KafkaVersion defaultVersion = KafkaVersion.parseKafkaVersions(
                new StringReader(
                        "- version: 2.2.1\n" +
                                "  format: 2.2\n" +
                                "  protocol: 2.2\n" +
                                "  checksum: ABCDE1234\n" +
                                "  third-party-libs: 2.2.x\n" +
                                "  default: false\n" +
                                "- version: 2.3.0\n" +
                                "  format: 2.3\n" +
                                "  protocol: 2.3\n" +
                                "  checksum: ABCDE1234\n" +
                                "  third-party-libs: 2.3.x\n" +
                                "  default: false"
                ), map);
    }

    @Test(expected = IllegalArgumentException.class)
    public void multipleDefaultTest() throws IOException {
        Map<String, KafkaVersion> map = new HashMap<>();
        KafkaVersion defaultVersion = KafkaVersion.parseKafkaVersions(
                new StringReader(
                        "- version: 2.2.1\n" +
                                "  format: 2.2\n" +
                                "  protocol: 2.2\n" +
                                "  checksum: ABCDE1234\n" +
                                "  third-party-libs: 2.2.x\n" +
                                "  default: true\n" +
                                "- version: 2.3.0\n" +
                                "  format: 2.3\n" +
                                "  protocol: 2.3\n" +
                                "  checksum: ABCDE1234\n" +
                                "  third-party-libs: 2.3.x\n" +
                                "  default: true"
                ), map);
    }

    @Test
    public void compare() {
        assertEquals(0, KafkaVersion.compareDottedVersions("2.0.0", "2.0.0"));
    }
}
