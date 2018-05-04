/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.controller.cluster.model;

import io.vertx.core.json.JsonObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KafkaConfigurationTest {
    @Test
    public void testEmptyConfigurationString() {
        String configuration = "";
        AbstractConfiguration config = new KafkaConfiguration(configuration);
        assertTrue(config.getConfiguration().isEmpty());
    }

    @Test
    public void testEmptyJson() {
        JsonObject configuration = new JsonObject();
        AbstractConfiguration config = new KafkaConfiguration(configuration);
        assertTrue(config.getConfiguration().isEmpty());
    }

    @Test
    public void testNonEmptyConfigurationString() {
        String configuration = "var1=aaa\n" +
                               "var2=bbb\n" +
                               "var3=ccc\n";
        String expectedConfiguration = "var3=ccc\n" +
                               "var2=bbb\n" +
                               "var1=aaa\n";

        AbstractConfiguration config = new KafkaConfiguration(configuration);
        assertEquals(expectedConfiguration, config.getConfiguration());
    }

    @Test
    public void testNonEmptyJson() {
        JsonObject configuration = new JsonObject().put("var1", "aaa").put("var2", "bbb").put("var3", "ccc");
        String expectedConfiguration = "var3=ccc\n" +
                "var2=bbb\n" +
                "var1=aaa\n";

        AbstractConfiguration config = new KafkaConfiguration(configuration);
        assertEquals(expectedConfiguration, config.getConfiguration());
    }

    @Test
    public void testConfigurationStringWithDuplicates() {
        String configuration = "var1=aaa\n" +
                "var2=bbb\n" +
                "var3=ccc\n" +
                "var2=ddd\n";
        String expectedConfiguration = "var3=ccc\n" +
                "var2=ddd\n" +
                "var1=aaa\n";

        AbstractConfiguration config = new KafkaConfiguration(configuration);
        assertEquals(expectedConfiguration, config.getConfiguration());
    }

    @Test
    public void testJsonWithDuplicates() {
        JsonObject configuration = new JsonObject().put("var1", "aaa").put("var2", "bbb").put("var3", "ccc").put("var2", "ddd");
        String expectedConfiguration = "var3=ccc\n" +
                "var2=ddd\n" +
                "var1=aaa\n";

        AbstractConfiguration config = new KafkaConfiguration(configuration);
        assertEquals(expectedConfiguration, config.getConfiguration());
    }

    @Test
    public void testConfigurationStringWithForbiddenKeys() {
        String configuration = "var1=aaa\n" +
                "var2=bbb\n" +
                "var3=ccc\n" +
                "advertised.listeners=ddd\n";
        String expectedConfiguration = "var3=ccc\n" +
                "var2=bbb\n" +
                "var1=aaa\n";

        AbstractConfiguration config = new KafkaConfiguration(configuration);
        assertEquals(expectedConfiguration, config.getConfiguration());
    }

    @Test
    public void testConfigurationStringWithForbiddenKeysInUpperCase() {
        String configuration = "var1=aaa\n" +
                "var2=bbb\n" +
                "var3=ccc\n" +
                "HOST.NAME=ddd\n";
        String expectedConfiguration = "var3=ccc\n" +
                "var2=bbb\n" +
                "var1=aaa\n";

        AbstractConfiguration config = new KafkaConfiguration(configuration);
        assertEquals(expectedConfiguration, config.getConfiguration());
    }

    @Test
    public void testJsonWithForbiddenKeys() {
        JsonObject configuration = new JsonObject().put("var1", "aaa").put("var2", "bbb").put("var3", "ccc").put("advertised.listeners", "ddd");
        String expectedConfiguration = "var3=ccc\n" +
                "var2=bbb\n" +
                "var1=aaa\n";

        AbstractConfiguration config = new KafkaConfiguration(configuration);
        assertEquals(expectedConfiguration, config.getConfiguration());
    }

    @Test
    public void testJsonWithDifferentTypes() {
        JsonObject configuration = new JsonObject().put("var1", 1).put("var2", "bbb").put("var3", new JsonObject().put("xxx", "yyy"));
        String expectedConfiguration = "var2=bbb\n" +
                "var1=1\n";

        AbstractConfiguration config = new KafkaConfiguration(configuration);
        assertEquals(expectedConfiguration, config.getConfiguration());
    }
}
