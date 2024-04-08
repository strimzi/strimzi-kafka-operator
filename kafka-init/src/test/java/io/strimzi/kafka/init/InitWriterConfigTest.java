/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.init;

import io.strimzi.operator.common.InvalidConfigurationException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class InitWriterConfigTest {
    private static final Map<String, String> ENV_VARS = Map.of(
        InitWriterConfig.NODE_NAME.key(), "localhost",
        InitWriterConfig.RACK_TOPOLOGY_KEY.key(), "failure-domain.beta.kubernetes.io/zone",
        InitWriterConfig.EXTERNAL_ADDRESS.key(), "TRUE"
    );

    @Test
    public void testFromMap() {
        InitWriterConfig config = InitWriterConfig.fromMap(ENV_VARS);

        assertThat(config.getNodeName(), is("localhost"));
        assertThat(config.getRackTopologyKey(), is("failure-domain.beta.kubernetes.io/zone"));
        assertThat(config.isExternalAddress(), is(true));
        assertThat(config.getAddressType(), is(nullValue()));
    }

    @Test
    public void testFromMapWithAddressTypeEnvVar() {
        Map<String, String> envs = new HashMap<>(ENV_VARS);
        envs.put(InitWriterConfig.EXTERNAL_ADDRESS_TYPE.key(), "InternalDNS");

        InitWriterConfig config = InitWriterConfig.fromMap(envs);
        assertThat(config.getAddressType(), is("InternalDNS"));
    }

    @Test
    public void testFromMapEmptyEnvVarsThrows() {
        assertThrows(InvalidConfigurationException.class, () -> InitWriterConfig.fromMap(Map.of()));
    }

    @Test
    public void testFromMapMissingNodeNameThrows() {
        Map<String, String> envVars = new HashMap<>(ENV_VARS);
        envVars.remove(InitWriterConfig.NODE_NAME.key());

        assertThrows(InvalidConfigurationException.class, () -> InitWriterConfig.fromMap(envVars));
    }
}
