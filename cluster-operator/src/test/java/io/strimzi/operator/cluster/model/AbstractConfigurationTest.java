/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import java.util.List;
import java.util.Properties;

import io.strimzi.operator.cluster.InvalidConfigMapException;
import io.vertx.core.json.JsonObject;
import org.junit.Test;

import static java.util.Arrays.asList;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AbstractConfigurationTest {
    final private String defaultConfiguration = "default.option=hello\n";

    @Test
    public void testConfigurationStringDefaults()  {
        AbstractConfiguration config = new TestConfiguration("");
        assertEquals(defaultConfiguration, config.getConfiguration());
    }

    @Test
    public void testConfigurationStringOverridingDefaults()  {
        AbstractConfiguration config = new TestConfiguration("default.option=world\n");
        assertEquals("default.option=world\n", config.getConfiguration());
    }

    @Test
    public void testConfigurationStringOverridingDefaultsWithMore()  {
        AbstractConfiguration config = new TestConfiguration("default.option=world\nvar1=aaa\n");

        String expectedConfiguration = "default.option=world\n" +
                "var1=aaa\n";

        assertEquals(expectedConfiguration, config.getConfiguration());
    }

    @Test
    public void testDefaultsFromJson()  {
        AbstractConfiguration config = new TestConfiguration(new JsonObject());
        assertEquals(defaultConfiguration, config.getConfiguration());
    }

    @Test
    public void testJsonOverridingDefaults()  {
        AbstractConfiguration config = new TestConfiguration(new JsonObject().put("default.option", "world"));
        assertEquals("default.option=world\n", config.getConfiguration());
    }

    @Test
    public void testJsonOverridingDefaultsWithMore()  {
        AbstractConfiguration config = new TestConfiguration(new JsonObject().put("default.option", "world").put("var1", "aaa"));

        String expectedConfiguration = "default.option=world\n" +
                "var1=aaa\n";

        assertEquals(expectedConfiguration, config.getConfiguration());
    }

    @Test
    public void testEmptyConfigurationString() {
        String configuration = "";
        AbstractConfiguration config = new TestConfigurationWithoutDefaults(configuration);
        assertTrue(config.getConfiguration().isEmpty());
    }

    @Test
    public void testEmptyJson() {
        JsonObject configuration = new JsonObject();
        AbstractConfiguration config = new TestConfigurationWithoutDefaults(configuration);
        assertTrue(config.getConfiguration().isEmpty());
    }

    @Test
    public void testNonEmptyConfigurationString() {
        String configuration = "var1=aaa\n" +
                "var2=bbb\n" +
                "var3=ccc\n";
        String expectedConfiguration = defaultConfiguration +
                "var3=ccc\n" +
                "var2=bbb\n" +
                "var1=aaa\n";

        AbstractConfiguration config = new TestConfiguration(configuration);
        assertEquals(expectedConfiguration, config.getConfiguration());
    }

    @Test
    public void testNonEmptyJson() {
        JsonObject configuration = new JsonObject().put("var1", "aaa").put("var2", "bbb").put("var3", "ccc");
        String expectedConfiguration = defaultConfiguration +
                "var3=ccc\n" +
                "var2=bbb\n" +
                "var1=aaa\n";

        AbstractConfiguration config = new TestConfiguration(configuration);
        assertEquals(expectedConfiguration, config.getConfiguration());
    }

    @Test
    public void testConfigurationStringWithDuplicates() {
        String configuration = "var1=aaa\n" +
                "var2=bbb\n" +
                "var3=ccc\n" +
                "var2=ddd\n";
        String expectedConfiguration = defaultConfiguration +
                "var3=ccc\n" +
                "var2=ddd\n" +
                "var1=aaa\n";

        AbstractConfiguration config = new TestConfiguration(configuration);
        assertEquals(expectedConfiguration, config.getConfiguration());
    }

    @Test
    public void testJsonWithDuplicates() {
        JsonObject configuration = new JsonObject().put("var1", "aaa").put("var2", "bbb").put("var3", "ccc").put("var2", "ddd");
        String expectedConfiguration = defaultConfiguration +
                "var3=ccc\n" +
                "var2=ddd\n" +
                "var1=aaa\n";

        AbstractConfiguration config = new TestConfiguration(configuration);
        assertEquals(expectedConfiguration, config.getConfiguration());
    }

    @Test
    public void testConfigurationStringWithForbiddenKeys() {
        String configuration = "var1=aaa\n" +
                "var2=bbb\n" +
                "var3=ccc\n" +
                "forbidden.option=ddd\n";
        String expectedConfiguration = defaultConfiguration +
                "var3=ccc\n" +
                "var2=bbb\n" +
                "var1=aaa\n";

        AbstractConfiguration config = new TestConfiguration(configuration);
        assertEquals(expectedConfiguration, config.getConfiguration());
    }

    @Test
    public void testConfigurationStringWithForbiddenKeysInUpperCase() {
        String configuration = "var1=aaa\n" +
                "var2=bbb\n" +
                "var3=ccc\n" +
                "FORBIDDEN.OPTION=ddd\n";
        String expectedConfiguration = defaultConfiguration +
                "var3=ccc\n" +
                "var2=bbb\n" +
                "var1=aaa\n";

        AbstractConfiguration config = new TestConfiguration(configuration);
        assertEquals(expectedConfiguration, config.getConfiguration());
    }

    @Test
    public void testJsonWithForbiddenKeys() {
        JsonObject configuration = new JsonObject().put("var1", "aaa").put("var2", "bbb").put("var3", "ccc").put("forbidden.option", "ddd");
        String expectedConfiguration = defaultConfiguration +
                "var3=ccc\n" +
                "var2=bbb\n" +
                "var1=aaa\n";

        AbstractConfiguration config = new TestConfiguration(configuration);
        assertEquals(expectedConfiguration, config.getConfiguration());
    }

    @Test
    public void testJsonWithForbiddenKeysPrefix() {
        JsonObject configuration = new JsonObject().put("var1", "aaa").put("var2", "bbb").put("var3", "ccc").put("forbidden.option.first", "ddd").put("forbidden.option.second", "ddd");
        String expectedConfiguration = defaultConfiguration +
                "var3=ccc\n" +
                "var2=bbb\n" +
                "var1=aaa\n";

        AbstractConfiguration config = new TestConfiguration(configuration);
        assertEquals(expectedConfiguration, config.getConfiguration());
    }

    @Test
    public void testJsonWithDifferentTypes() {
        JsonObject configuration = new JsonObject().put("var1", 1).put("var2", "bbb").put("var3", new JsonObject().put("xxx", "yyy"));
        String expectedConfiguration = defaultConfiguration +
                "var2=bbb\n" +
                "var1=1\n";
        try {
            AbstractConfiguration config = new TestConfiguration(configuration);
            fail("Expected it to throw an exception");
        } catch (InvalidConfigMapException e) {
            assertEquals("var3", e.getKey());
        }
    }

    @Test
    public void testWithHostPort() {
        JsonObject configuration = new JsonObject().put("option.with.port", "my-server:9092");
        String expectedConfiguration = defaultConfiguration +
                "option.with.port=my-server\\:9092\n";

        AbstractConfiguration config = new TestConfiguration(configuration);
        assertEquals(expectedConfiguration, config.getConfiguration());
    }
}

class TestConfiguration extends AbstractConfiguration {
    private static final List<String> FORBIDDEN_OPTIONS;
    private static final Properties DEFAULTS;

    static {
        FORBIDDEN_OPTIONS = asList(
                "forbidden.option");

        DEFAULTS = new Properties();
        DEFAULTS.setProperty("default.option", "hello");
    }

    /**
     * Constructor used to instantiate this class from String configuration. Should be used to create configuration
     * from the Assembly.
     *
     * @param configuration Configuration in String format. Should contain zero or more lines with with key=value
     *                      pairs.
     */
    public TestConfiguration(String configuration) {
        super(configuration, FORBIDDEN_OPTIONS, DEFAULTS);
    }

    /**
     * Constructor used to instantiate this class from JsonObject. Should be used to create configuration from
     * ConfigMap / CRD.
     *
     * @param jsonOptions     Json object with configuration options as key ad value pairs.
     */
    public TestConfiguration(JsonObject jsonOptions) {
        super(jsonOptions, FORBIDDEN_OPTIONS, DEFAULTS);
    }
}

class TestConfigurationWithoutDefaults extends AbstractConfiguration {
    private static final List<String> FORBIDDEN_OPTIONS;

    static {
        FORBIDDEN_OPTIONS = asList(
                "forbidden.option");
    }

    /**
     * Constructor used to instantiate this class from String configuration. Should be used to create configuration
     * from the Assembly.
     *
     * @param configuration Configuration in String format. Should contain zero or more lines with with key=value
     *                      pairs.
     */
    public TestConfigurationWithoutDefaults(String configuration) {
        super(configuration, FORBIDDEN_OPTIONS);
    }

    /**
     * Constructor used to instantiate this class from JsonObject. Should be used to create configuration from
     * ConfigMap / CRD.
     *
     * @param jsonOptions     Json object with configuration options as key ad value pairs.
     */
    public TestConfigurationWithoutDefaults(JsonObject jsonOptions) {
        super(jsonOptions, FORBIDDEN_OPTIONS);
    }
}