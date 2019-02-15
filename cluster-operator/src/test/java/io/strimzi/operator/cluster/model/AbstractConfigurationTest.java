/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.strimzi.operator.cluster.InvalidConfigParameterException;
import io.vertx.core.json.JsonObject;
import org.junit.Test;

import static io.strimzi.test.TestUtils.LINE_SEPARATOR;
import static java.util.Arrays.asList;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AbstractConfigurationTest {

    static OrderedProperties createProperties(String... keyValues) {
        OrderedProperties properties = new OrderedProperties();
        for (int i = 0; i < keyValues.length; i += 2) {
            properties.addPair(keyValues[i], keyValues[i + 1]);
        }
        return properties;
    }

    OrderedProperties createWithDefaults(String... keyValues) {
        return createProperties(keyValues).addMapPairs(defaultConfiguration.asMap());
    }

    final private OrderedProperties defaultConfiguration = createProperties("default.option", "hello");

    @Test
    public void testConfigurationStringDefaults()  {
        AbstractConfiguration config = new TestConfiguration("");
        assertEquals(defaultConfiguration, config.asOrderedProperties());
    }

    @Test
    public void testConfigurationStringOverridingDefaults()  {
        AbstractConfiguration config = new TestConfiguration("default.option=world");
        assertEquals(createProperties("default.option", "world"), config.asOrderedProperties());
    }

    @Test
    public void testConfigurationStringOverridingDefaultsWithMore()  {
        AbstractConfiguration config = new TestConfiguration("default.option=world" + LINE_SEPARATOR + "var1=aaa" + LINE_SEPARATOR);
        assertEquals(createProperties("default.option", "world", "var1", "aaa"), config.asOrderedProperties());
    }

    @Test
    public void testDefaultsFromJson()  {
        AbstractConfiguration config = new TestConfiguration(new JsonObject());
        assertEquals(defaultConfiguration, config.asOrderedProperties());
    }

    @Test
    public void testJsonOverridingDefaults()  {
        AbstractConfiguration config = new TestConfiguration(new JsonObject().put("default.option", "world"));
        assertEquals(createProperties("default.option", "world"), config.asOrderedProperties());
    }

    @Test
    public void testJsonOverridingDefaultsWithMore()  {
        AbstractConfiguration config = new TestConfiguration(new JsonObject().put("default.option", "world").put("var1", "aaa"));
        assertEquals(createProperties("default.option", "world", "var1", "aaa"), config.asOrderedProperties());
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
        String configuration = "var1=aaa" + LINE_SEPARATOR +
                "var2=bbb" + LINE_SEPARATOR +
                "var3=ccc" + LINE_SEPARATOR;
        OrderedProperties expectedConfiguration = createWithDefaults("var3", "ccc",
                "var2", "bbb",
                "var1", "aaa");

        AbstractConfiguration config = new TestConfiguration(configuration);
        assertEquals(expectedConfiguration, config.asOrderedProperties());
    }

    @Test
    public void testNonEmptyJson() {
        JsonObject configuration = new JsonObject()
                .put("var1", "aaa")
                .put("var2", "bbb")
                .put("var3", "ccc");
        OrderedProperties expectedConfiguration = createWithDefaults("var3", "ccc",
                "var2", "bbb",
                "var1", "aaa");

        AbstractConfiguration config = new TestConfiguration(configuration);
        assertEquals(expectedConfiguration, config.asOrderedProperties());
    }

    @Test
    public void testConfigurationStringWithDuplicates() {
        String configuration = "var1=aaa" + LINE_SEPARATOR +
                "var2=bbb" + LINE_SEPARATOR +
                "var3=ccc" + LINE_SEPARATOR +
                "var2=ddd" + LINE_SEPARATOR;
        OrderedProperties expectedConfiguration = createWithDefaults("var3", "ccc",
                "var2", "ddd",
                "var1", "aaa");

        AbstractConfiguration config = new TestConfiguration(configuration);
        assertEquals(expectedConfiguration, config.asOrderedProperties());
    }

    @Test
    public void testJsonWithDuplicates() {
        JsonObject configuration = new JsonObject()
                .put("var1", "aaa")
                .put("var2", "bbb")
                .put("var3", "ccc")
                .put("var2", "ddd");
        OrderedProperties expectedConfiguration = createWithDefaults("var3", "ccc",
                "var2", "ddd",
                "var1", "aaa");

        AbstractConfiguration config = new TestConfiguration(configuration);
        assertEquals(expectedConfiguration, config.asOrderedProperties());
    }

    @Test
    public void testConfigurationStringWithForbiddenKeys() {
        String configuration = "var1=aaa" + LINE_SEPARATOR +
                "var2=bbb" + LINE_SEPARATOR +
                "var3=ccc" + LINE_SEPARATOR +
                "forbidden.option=ddd" + LINE_SEPARATOR;
        OrderedProperties expectedConfiguration = createWithDefaults("var3", "ccc",
                "var2", "bbb",
                "var1", "aaa");

        AbstractConfiguration config = new TestConfiguration(configuration);
        assertEquals(expectedConfiguration, config.asOrderedProperties());
    }

    @Test
    public void testConfigurationStringWithForbiddenKeysInUpperCase() {
        String configuration = "var1=aaa" + LINE_SEPARATOR +
                "var2=bbb" + LINE_SEPARATOR +
                "var3=ccc" + LINE_SEPARATOR +
                "FORBIDDEN.OPTION=ddd" + LINE_SEPARATOR;
        OrderedProperties expectedConfiguration = createWithDefaults("var3", "ccc",
                "var2", "bbb",
                "var1", "aaa");

        AbstractConfiguration config = new TestConfiguration(configuration);
        assertEquals(expectedConfiguration, config.asOrderedProperties());
    }

    @Test
    public void testJsonWithForbiddenKeys() {
        JsonObject configuration = new JsonObject().put("var1", "aaa")
                .put("var2", "bbb")
                .put("var3", "ccc")
                .put("forbidden.option", "ddd");
        OrderedProperties expectedConfiguration = createWithDefaults("var3", "ccc",
                "var2", "bbb",
                "var1", "aaa");

        AbstractConfiguration config = new TestConfiguration(configuration);
        assertEquals(expectedConfiguration, config.asOrderedProperties());
    }

    @Test
    public void testJsonWithForbiddenKeysPrefix() {
        JsonObject configuration = new JsonObject().put("var1", "aaa")
                .put("var2", "bbb")
                .put("var3", "ccc")
                .put("forbidden.option.first", "ddd")
                .put("forbidden.option.second", "ddd");
        OrderedProperties expectedConfiguration = createWithDefaults("var3", "ccc",
                "var2", "bbb",
                "var1", "aaa");

        AbstractConfiguration config = new TestConfiguration(configuration);
        assertEquals(expectedConfiguration, config.asOrderedProperties());
    }

    @Test
    public void testJsonWithDifferentTypes() {
        JsonObject configuration = new JsonObject().put("var1", 1)
                .put("var2", "bbb")
                .put("var3", new JsonObject().put("xxx", "yyy"));
        try {
            new TestConfiguration(configuration);
            fail("Expected it to throw an exception");
        } catch (InvalidConfigParameterException e) {
            assertEquals("var3", e.getKey());
        }
    }

    @Test
    public void testWithHostPort() {
        JsonObject configuration = new JsonObject().put("option.with.port", "my-server:9092");
        OrderedProperties expectedConfiguration = createWithDefaults("option.with.port", "my-server:9092");

        AbstractConfiguration config = new TestConfiguration(configuration);
        assertEquals(expectedConfiguration, config.asOrderedProperties());
    }

    @Test
    public void testKafkaZookeeperTimeout() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("valid", "validValue");
        conf.put("zookeeper.connection.whatever", "invalid");
        conf.put("security.invalid1", "invalid");
        conf.put("zookeeper.connection.timeout.ms", "42"); // valid
        conf.put("zookeeper.connection.timeout", "42"); // invalid

        KafkaConfiguration kc = new KafkaConfiguration(conf.entrySet());

        assertEquals("validValue", kc.asOrderedProperties().asMap().get("valid"));
        assertNull(kc.asOrderedProperties().asMap().get("zookeeper.connection.whatever"));
        assertNull(kc.asOrderedProperties().asMap().get("security.invalid1"));
        assertEquals("42", kc.asOrderedProperties().asMap().get("zookeeper.connection.timeout.ms"));
        assertNull(kc.asOrderedProperties().asMap().get("zookeeper.connection.timeout"));
    }
}

class TestConfiguration extends AbstractConfiguration {
    private static final List<String> FORBIDDEN_OPTIONS;
    private static final Map<String, String> DEFAULTS;

    static {
        FORBIDDEN_OPTIONS = asList(
                "forbidden.option");

        DEFAULTS = new HashMap<>();
        DEFAULTS.put("default.option", "hello");
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
