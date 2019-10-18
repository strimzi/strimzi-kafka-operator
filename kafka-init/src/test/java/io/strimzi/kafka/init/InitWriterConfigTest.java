/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.init;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class InitWriterConfigTest {

    private static Map<String, String> envVars = new HashMap<>(3);

    static {
        envVars.put(InitWriterConfig.NODE_NAME, "localhost");
        envVars.put(InitWriterConfig.RACK_TOPOLOGY_KEY, "failure-domain.beta.kubernetes.io/zone");
        envVars.put(InitWriterConfig.EXTERNAL_ADDRESS, "TRUE");
    }

    @Test
    public void testEnvVars() {
        InitWriterConfig config = InitWriterConfig.fromMap(envVars);

        assertEquals("localhost", config.getNodeName());
        assertEquals("failure-domain.beta.kubernetes.io/zone", config.getRackTopologyKey());
        assertTrue(config.isExternalAddress());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyEnvVars() {

        InitWriterConfig.fromMap(Collections.emptyMap());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoNodeName() {

        Map<String, String> envVars = new HashMap<>(InitWriterConfigTest.envVars);
        envVars.remove(InitWriterConfig.NODE_NAME);

        InitWriterConfig.fromMap(envVars);
    }
}
