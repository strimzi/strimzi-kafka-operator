/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import org.junit.Test;

import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KafkaVersionTest {

    @Test
    public void load() {
        KafkaVersion.Lookup loaded = new KafkaVersion.Lookup();
        assertTrue(loaded.supportedVersions().contains("2.0.0"));
        assertTrue(loaded.supportedVersions().contains("2.0.1"));
        assertEquals("2.0.0", loaded.version("2.0.0").version());
        assertEquals("2.0", loaded.version("2.0.0").protocolVersion());
        assertEquals("2.0", loaded.version("2.0.0").messageVersion());
    }

    @Test
    public void parse() throws Exception {
        Map<String, KafkaVersion> map = new HashMap<>();
        KafkaVersion defaultVersion = KafkaVersion.parseKafkaVersions(new LineNumberReader(new StringReader(
                "2.0.0 default 2.0 2.0 1234567890abcdef\n" +
                        "2.0.1  2.0 2.0 1234567890abcdef")), map);
        assertEquals("2.0.0", defaultVersion.version());
        assertEquals(2, map.size());
        assertTrue(map.containsKey("2.0.0"));
        assertEquals("2.0.0", map.get("2.0.0").version());
        assertEquals("2.0", map.get("2.0.0").protocolVersion());
        assertEquals("2.0", map.get("2.0.0").messageVersion());
        assertTrue(map.containsKey("2.0.1"));
        assertEquals("2.0.1", map.get("2.0.1").version());
        assertEquals("2.0", map.get("2.0.1").protocolVersion());
        assertEquals("2.0", map.get("2.0.1").messageVersion());
    }

    @Test
    public void compare() {
        assertEquals(0, KafkaVersion.compareDottedVersions("2.0.0", "2.0.0"));
    }
}
