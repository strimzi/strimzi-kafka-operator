/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.strimzi.api.kafka.model.JvmOptions;
import io.strimzi.api.kafka.model.Resources;
import io.strimzi.test.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

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
        AbstractModel am = new AbstractModel(null, null, Labels.forCluster("foo")) {
            @Override
            protected String getDefaultLogConfigFileName() {
                return "";
            }

            @Override
            protected List<Container> getContainers() {
                return emptyList();
            }
        };
        am.setJvmOptions(jvmOptions(xmx, xms));
        List<EnvVar> envVars = new ArrayList<>(1);
        am.heapOptions(envVars, dynamicFraction, dynamicMax);
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

    @Test
    public void testJvmPerformanceOptions() {
        JvmOptions opts = TestUtils.fromJson("{}", JvmOptions.class);

        assertNull(getPerformanceOptions(opts));

        opts = TestUtils.fromJson("{" +
                "  \"-server\": \"true\"" +
                "}", JvmOptions.class);

        assertEquals("-server", getPerformanceOptions(opts));

        opts = TestUtils.fromJson("{" +
                "    \"-XX\":" +
                "            {\"key1\": \"value1\"," +
                "            \"key2\": \"true\"," +
                "            \"key3\": false," +
                "            \"key4\": 10}" +
                "}", JvmOptions.class);

        assertEquals("-XX:key1=value1 -XX:+key2 -XX:-key3 -XX:key4=10", getPerformanceOptions(opts));
    }

    private String getPerformanceOptions(JvmOptions opts) {
        AbstractModel am = new AbstractModel(null, null, Labels.forCluster("foo")) {
            @Override
            protected String getDefaultLogConfigFileName() {
                return "";
            }

            @Override
            protected List<Container> getContainers() {
                return emptyList();
            }
        };
        am.setJvmOptions(opts);
        List<EnvVar> envVars = new ArrayList<>(1);
        am.jvmPerformanceOptions(envVars);

        if (!envVars.isEmpty()) {
            return envVars.get(0).getValue();
        } else {
            return null;
        }
    }


    @Test
    public void testDeserializeSuffixes() {
        Resources opts = TestUtils.fromJson("{\"limits\": {\"memory\": \"10Gi\", \"cpu\": \"1\"}, \"requests\": {\"memory\": \"5G\", \"cpu\": 1}}", Resources.class);
        assertEquals(10737418240L, opts.getLimits().memoryAsLong());
        assertEquals(1000, opts.getLimits().milliCpuAsInt());
        assertEquals("1", opts.getLimits().getMilliCpu());
        assertEquals(5000000000L, opts.getRequests().memoryAsLong());
        assertEquals(1000, opts.getLimits().milliCpuAsInt());
        assertEquals("1", opts.getLimits().getMilliCpu());
        AbstractModel abstractModel = new AbstractModel("", "", Labels.forCluster("")) {
            @Override
            protected String getDefaultLogConfigFileName() {
                return "";
            }

            /**
             * @return a list of containers to add to the StatefulSet/Deployment
             */
            @Override
            protected List<Container> getContainers() {
                return null;
            }
        };
        abstractModel.setResources(opts);
        Assert.assertEquals("1", abstractModel.resources(opts).getLimits().get("cpu").getAmount());
    }
}
