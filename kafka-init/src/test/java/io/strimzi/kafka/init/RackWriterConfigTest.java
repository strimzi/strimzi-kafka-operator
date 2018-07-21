/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.init;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class RackWriterConfigTest {

    private static Map<String, String> envVars = new HashMap<>(2);

    static {

        envVars.put(RackWriterConfig.NODE_NAME, "localhost");
        envVars.put(RackWriterConfig.RACK_TOPOLOGY_KEY, "failure-domain.beta.kubernetes.io/zone");
    }

    @Test
    public void testEnvVars() {

        RackWriterConfig config = RackWriterConfig.fromMap(envVars);

        assertEquals("localhost", config.getNodeName());
        assertEquals("failure-domain.beta.kubernetes.io/zone", config.getRackTopologyKey());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyEnvVars() {

        RackWriterConfig.fromMap(Collections.emptyMap());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoNodeName() {

        Map<String, String> envVars = new HashMap<>(RackWriterConfigTest.envVars);
        envVars.remove(RackWriterConfig.NODE_NAME);

        RackWriterConfig.fromMap(envVars);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoRackTopologyKey() {

        Map<String, String> envVars = new HashMap<>(RackWriterConfigTest.envVars);
        envVars.remove(RackWriterConfig.RACK_TOPOLOGY_KEY);

        RackWriterConfig.fromMap(envVars);
    }
}
