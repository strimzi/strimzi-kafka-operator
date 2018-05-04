/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.model;

import io.fabric8.kubernetes.api.model.EnvVar;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class AbstractModelTest {

    private static JvmOptions jvmOptions(String xmx, String xms) {
        JvmOptions result = new JvmOptions();
        result.setXms(xms);
        result.setXmx(xmx);
        return result;
    }

    @Test
    public void testJvmMemoryOptionsExplicit() {
        Map<String, String> env = getStringStringMap("4", "4",
                0.5, 4_000_000_000L);
        assertEquals("-Xms4 -Xmx4", env.get(AbstractModel.ENV_VAR_KAFKA_HEAP_OPTS));
        assertEquals(null, env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_FRACTION));
        assertEquals(null, env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_MAX));
    }

    private Map<String, String> getStringStringMap(String xmx, String xms, double dynamicFraction, long dynamicMax) {
        AbstractModel am = new AbstractModel(null, null, Labels.forCluster("foo")) { };
        am.setJvmOptions(jvmOptions(xmx, xms));
        List<EnvVar> envVars = new ArrayList<>(1);
        am.kafkaHeapOptions(envVars, dynamicFraction, dynamicMax);
        return envVars.stream().collect(Collectors.toMap(e -> e.getName(), e -> e.getValue()));
    }

    @Test
    public void testJvmMemoryOptionsDefault() {
        Map<String, String> env = getStringStringMap(null, "4",
                0.5, 5_000_000_000L);
        assertEquals("-Xms4", env.get(AbstractModel.ENV_VAR_KAFKA_HEAP_OPTS));
        assertEquals("0.5", env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_FRACTION));
        assertEquals("5000000000", env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_MAX));

        env = getStringStringMap(null, "4",
                0.5, 5_000_000_000L);
        assertEquals("-Xms4", env.get(AbstractModel.ENV_VAR_KAFKA_HEAP_OPTS));
        assertEquals("0.5", env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_FRACTION));
        assertEquals("5000000000", env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_MAX));
    }

    @Test
    public void testJvmMemoryOptionsMemoryRequest() {
        Map<String, String> env = getStringStringMap(null, null,
                0.7, 10_000_000_000L);
        assertEquals(null, env.get(AbstractModel.ENV_VAR_KAFKA_HEAP_OPTS));
        assertEquals("0.7", env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_FRACTION));
        assertEquals("10000000000", env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_MAX));
    }
}
