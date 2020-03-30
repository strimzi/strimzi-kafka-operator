/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ConfigTest {

    private static final Map<String, String> MANDATORY = new HashMap<>();

    static {
        MANDATORY.put(Config.ZOOKEEPER_CONNECT.key, "localhost:2181");
        MANDATORY.put(Config.KAFKA_BOOTSTRAP_SERVERS.key, "localhost:9092");
        MANDATORY.put(Config.NAMESPACE.key, "default");
    }

    @Test
    public void testUnknownKeyThrows() {
        assertThrows(IllegalArgumentException.class, () -> {
            new Config(Collections.singletonMap("foo", "bar"));
        });
    }

    @Test
    public void testEmptyMapThrows() {
        assertThrows(IllegalArgumentException.class, () -> {
            Config c = new Config(Collections.emptyMap());
        });
    }

    @Test
    public void testDefaultInput() {
        Map<String, String> map = new HashMap<>(MANDATORY);
        Config c = new Config(map);
        assertThat(c.get(Config.ZOOKEEPER_SESSION_TIMEOUT_MS).intValue(), is(20_000));
    }

    @Test
    public void testOverrideZookeeperSessionTimeout() {
        Map<String, String> map = new HashMap<>(MANDATORY);
        map.put(Config.ZOOKEEPER_SESSION_TIMEOUT_MS.key, "13000");

        Config c = new Config(map);
        assertThat(c.get(Config.ZOOKEEPER_SESSION_TIMEOUT_MS).intValue(), is(13_000));
    }

    @Test
    public void testNewInvalidTimeout() {
        Map<String, String> map = new HashMap<>(MANDATORY);
        map.put(Config.ZOOKEEPER_SESSION_TIMEOUT_MS.key, "13000");

        assertDoesNotThrow(() -> new Config(map));

        map.put(Config.ZOOKEEPER_SESSION_TIMEOUT_MS.key, "foos");
        assertThrows(IllegalArgumentException.class, () -> new Config(map));
    }

    @Test
    public void testTopicMetadataMaxAttemptsIsSetCorrectly() {

        Map<String, String> map = new HashMap<>(MANDATORY);
        map.put(Config.TC_TOPIC_METADATA_MAX_ATTEMPTS, "3");

        Config c = new Config(map);
        assertThat(c.get(Config.TOPIC_METADATA_MAX_ATTEMPTS).intValue(), is(3));
    }
}
