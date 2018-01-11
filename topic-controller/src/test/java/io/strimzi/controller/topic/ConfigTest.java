/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.strimzi.controller.topic;

import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ConfigTest {

    @Test(expected = IllegalArgumentException.class)
    public void unknownKey() {
        new Config(Collections.singletonMap("foo", "bar"));
    }

    @Test
    public void empty() {
        Config c = new Config(Collections.emptyMap());
        assertEquals(Config.ZOOKEEPER_CONNECT.defaultValue, c.get(Config.ZOOKEEPER_CONNECT));
        assertEquals(Config.KAFKA_BOOTSTRAP_SERVERS.defaultValue, c.get(Config.KAFKA_BOOTSTRAP_SERVERS));
        assertEquals(2_000, c.get(Config.ZOOKEEPER_SESSION_TIMEOUT_MS).intValue());
    }

    @Test
    public void override() {
        Config c = new Config(Collections.singletonMap(Config.ZOOKEEPER_SESSION_TIMEOUT_MS.key, "13 seconds"));
        assertEquals(Config.ZOOKEEPER_CONNECT.defaultValue, c.get(Config.ZOOKEEPER_CONNECT));
        assertEquals(Config.KAFKA_BOOTSTRAP_SERVERS.defaultValue, c.get(Config.KAFKA_BOOTSTRAP_SERVERS));
        assertEquals(13_000, c.get(Config.ZOOKEEPER_SESSION_TIMEOUT_MS).intValue());
    }

    @Test
    public void intervals() {
        new Config(Collections.singletonMap(Config.ZOOKEEPER_SESSION_TIMEOUT_MS.key, "13 seconds"));
        new Config(Collections.singletonMap(Config.ZOOKEEPER_SESSION_TIMEOUT_MS.key, "13seconds"));
        try {
            new Config(Collections.singletonMap(Config.ZOOKEEPER_SESSION_TIMEOUT_MS.key, "13foos"));
            fail();
        } catch (IllegalArgumentException e) {

        }
    }
}
