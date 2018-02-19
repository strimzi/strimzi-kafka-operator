/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.topic;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ConfigTest {

    private static final Map<String, String> MANDATORY = new HashMap<>();

    static {
        MANDATORY.put(Config.ZOOKEEPER_CONNECT.key, "localhost:2181");
        MANDATORY.put(Config.KAFKA_BOOTSTRAP_SERVERS.key, "localhost:9092");
        MANDATORY.put(Config.NAMESPACE.key, "default");
    }

    @Test(expected = IllegalArgumentException.class)
    public void unknownKey() {
        new Config(Collections.singletonMap("foo", "bar"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void empty() {
        Config c = new Config(Collections.emptyMap());
    }

    @Test
    public void defaults() {
        Map<String, String> map = new HashMap<>(MANDATORY);
        Config c = new Config(map);
        assertEquals(20_000, c.get(Config.ZOOKEEPER_SESSION_TIMEOUT_MS).intValue());
    }

    @Test
    public void override() {
        Map<String, String> map = new HashMap<>(MANDATORY);
        map.put(Config.ZOOKEEPER_SESSION_TIMEOUT_MS.key, "13 seconds");

        Config c = new Config(map);
        assertEquals(13_000, c.get(Config.ZOOKEEPER_SESSION_TIMEOUT_MS).intValue());
    }

    @Test
    public void intervals() {
        Map<String, String> map = new HashMap<>(MANDATORY);

        map.put(Config.ZOOKEEPER_SESSION_TIMEOUT_MS.key, "13 seconds");
        new Config(map);

        map.put(Config.ZOOKEEPER_SESSION_TIMEOUT_MS.key, "13seconds");
        new Config(map);

        try {
            map.put(Config.ZOOKEEPER_SESSION_TIMEOUT_MS.key, "13foos");
            new Config(map);
            fail();
        } catch (IllegalArgumentException e) {

        }
    }
}
