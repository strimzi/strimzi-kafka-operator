/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.init;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class InitWriterConfigTest {

    private static Map<String, String> envVars = new HashMap<>(3);
    static {
        envVars.put(InitWriterConfig.NODE_NAME, "localhost");
        envVars.put(InitWriterConfig.RACK_TOPOLOGY_KEY, "failure-domain.beta.kubernetes.io/zone");
        envVars.put(InitWriterConfig.EXTERNAL_ADDRESS, "TRUE");
    }

    @Test
    public void testFromMap() {
        InitWriterConfig config = InitWriterConfig.fromMap(envVars);

        assertThat(config.getNodeName(), is("localhost"));
        assertThat(config.getRackTopologyKey(), is("failure-domain.beta.kubernetes.io/zone"));
        assertThat(config.isExternalAddress(), is(true));
        assertThat(config.getAddressType(), is(nullValue()));
    }

    @Test
    public void testFromMapWithAddressTypeEnvVar() {
        Map<String, String> envs = new HashMap<>(envVars);
        envs.put(InitWriterConfig.EXTERNAL_ADDRESS_TYPE, "InternalDNS");

        InitWriterConfig config = InitWriterConfig.fromMap(envs);
        assertThat(config.getAddressType(), is("InternalDNS"));
    }

    @Test
    public void testFromMapEmptyEnvVarsThrows() {
        assertThrows(IllegalArgumentException.class, () -> InitWriterConfig.fromMap(Collections.emptyMap()));
    }

    @Test
    public void testFromMapMissingNodeNameThrows() {
        Map<String, String> envVars = new HashMap<>(InitWriterConfigTest.envVars);
        envVars.remove(InitWriterConfig.NODE_NAME);

        assertThrows(IllegalArgumentException.class, () -> InitWriterConfig.fromMap(envVars));
    }
}
