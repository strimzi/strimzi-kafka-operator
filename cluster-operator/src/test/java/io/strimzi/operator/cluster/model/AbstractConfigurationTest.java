/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.InvalidConfigParameterException;
import io.strimzi.operator.common.model.OrderedProperties;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.strimzi.test.TestUtils.LINE_SEPARATOR;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

@ParallelSuite
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

    @ParallelTest
    public void testConfigurationStringDefaults()  {
        AbstractConfiguration config = new TestConfiguration("");
        assertThat(config.asOrderedProperties(), is(defaultConfiguration));
    }

    @ParallelTest
    public void testConfigurationStringOverridingDefaults()  {
        AbstractConfiguration config = new TestConfiguration("default.option=world");
        assertThat(config.asOrderedProperties(), is(createProperties("default.option", "world")));
    }

    @ParallelTest
    public void testConfigurationStringOverridingDefaultsWithMore()  {
        AbstractConfiguration config = new TestConfiguration("default.option=world" + LINE_SEPARATOR + "var1=aaa" + LINE_SEPARATOR);
        assertThat(config.asOrderedProperties(), is(createProperties("default.option", "world", "var1", "aaa")));
    }

    @ParallelTest
    public void testDefaultsFromJson()  {
        AbstractConfiguration config = new TestConfiguration(new JsonObject());
        assertThat(config.asOrderedProperties(), is(defaultConfiguration));
    }

    @ParallelTest
    public void testJsonOverridingDefaults()  {
        AbstractConfiguration config = new TestConfiguration(new JsonObject().put("default.option", "world"));
        assertThat(config.asOrderedProperties(), is(createProperties("default.option", "world")));
    }

    @ParallelTest
    public void testJsonOverridingDefaultsWithMore()  {
        AbstractConfiguration config = new TestConfiguration(new JsonObject().put("default.option", "world").put("var1", "aaa"));
        assertThat(config.asOrderedProperties(), is(createProperties("default.option", "world", "var1", "aaa")));
    }

    @ParallelTest
    public void testEmptyConfigurationString() {
        String configuration = "";
        AbstractConfiguration config = new TestConfigurationWithoutDefaults(configuration);
        assertThat(config.getConfiguration().isEmpty(), is(true));
    }

    @ParallelTest
    public void testEmptyJson() {
        JsonObject configuration = new JsonObject();
        AbstractConfiguration config = new TestConfigurationWithoutDefaults(configuration);
        assertThat(config.getConfiguration().isEmpty(), is(true));
    }

    @ParallelTest
    public void testNonEmptyConfigurationString() {
        String configuration = "var1=aaa" + LINE_SEPARATOR +
                "var2=bbb" + LINE_SEPARATOR +
                "var3=ccc" + LINE_SEPARATOR;
        OrderedProperties expectedConfiguration = createWithDefaults("var3", "ccc",
                "var2", "bbb",
                "var1", "aaa");

        AbstractConfiguration config = new TestConfiguration(configuration);
        assertThat(config.asOrderedProperties(), is(expectedConfiguration));
    }

    @ParallelTest
    public void testNonEmptyJson() {
        JsonObject configuration = new JsonObject()
                .put("var1", "aaa")
                .put("var2", "bbb")
                .put("var3", "ccc");
        OrderedProperties expectedConfiguration = createWithDefaults("var3", "ccc",
                "var2", "bbb",
                "var1", "aaa");

        AbstractConfiguration config = new TestConfiguration(configuration);
        assertThat(config.asOrderedProperties(), is(expectedConfiguration));
    }

    @ParallelTest
    public void testConfigurationStringWithDuplicates() {
        String configuration = "var1=aaa" + LINE_SEPARATOR +
                "var2=bbb" + LINE_SEPARATOR +
                "var3=ccc" + LINE_SEPARATOR +
                "var2=ddd" + LINE_SEPARATOR;
        OrderedProperties expectedConfiguration = createWithDefaults("var3", "ccc",
                "var2", "ddd",
                "var1", "aaa");

        AbstractConfiguration config = new TestConfiguration(configuration);
        assertThat(config.asOrderedProperties(), is(expectedConfiguration));
    }

    @ParallelTest
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
        assertThat(config.asOrderedProperties(), is(expectedConfiguration));
    }

    @ParallelTest
    public void testConfigurationStringWithForbiddenKeys() {
        String configuration = "var1=aaa" + LINE_SEPARATOR +
                "var2=bbb" + LINE_SEPARATOR +
                "var3=ccc" + LINE_SEPARATOR +
                "forbidden.prefix=ddd" + LINE_SEPARATOR +
                "forbidden.prefix.first=eee" + LINE_SEPARATOR +
                "forbidden.prefix.second=fff" + LINE_SEPARATOR +
                "forbidden.prefix.exception=ggg" + LINE_SEPARATOR +
                "forbidden.option=hhh" + LINE_SEPARATOR +
                "forbidden.option.not.exact=iii" + LINE_SEPARATOR +
                "forbidden.option=ddd" + LINE_SEPARATOR;
        OrderedProperties expectedConfiguration = createWithDefaults("var3", "ccc",
                "var2", "bbb",
                "var1", "aaa",
                "forbidden.prefix.exception", "ggg",
                "forbidden.option.not.exact", "iii");

        AbstractConfiguration config = new TestConfiguration(configuration);
        assertThat(config.asOrderedProperties(), is(expectedConfiguration));
    }

    @ParallelTest
    public void testConfigurationStringWithForbiddenKeysInUpperCase() {
        String configuration = "var1=aaa" + LINE_SEPARATOR +
                "var2=bbb" + LINE_SEPARATOR +
                "var3=ccc" + LINE_SEPARATOR +
                "FORBIDDEN.OPTION=ddd" + LINE_SEPARATOR +
                "FORBIDDEN.PREFIX=eee" + LINE_SEPARATOR;
        OrderedProperties expectedConfiguration = createWithDefaults("var3", "ccc",
                "var2", "bbb",
                "var1", "aaa");

        AbstractConfiguration config = new TestConfiguration(configuration);
        assertThat(config.asOrderedProperties(), is(expectedConfiguration));
    }

    @ParallelTest
    public void testJsonWithForbiddenPrefixesAndOptions() {
        JsonObject configuration = new JsonObject().put("var1", "aaa")
                .put("var2", "bbb")
                .put("var3", "ccc")
                .put("forbidden.prefix", "ddd")
                .put("forbidden.prefix.first", "eee")
                .put("forbidden.prefix.second", "fff")
                .put("forbidden.prefix.exception", "ggg")
                .put("forbidden.option", "hhh")
                .put("forbidden.option.not.exact", "iii");
        OrderedProperties expectedConfiguration = createWithDefaults("var3", "ccc",
                "var2", "bbb",
                "var1", "aaa",
                "forbidden.prefix.exception", "ggg",
                "forbidden.option.not.exact", "iii");

        AbstractConfiguration config = new TestConfiguration(configuration);
        assertThat(config.asOrderedProperties(), is(expectedConfiguration));
    }

    @ParallelTest
    public void testJsonWithDifferentTypes() {
        JsonObject configuration = new JsonObject().put("var1", 1)
                .put("var2", "bbb")
                .put("var3", new JsonObject().put("xxx", "yyy"));
        try {
            new TestConfiguration(configuration);
            fail("Expected it to throw an exception");
        } catch (InvalidConfigParameterException e) {
            assertThat(e.getKey(), is("var3"));
        }
    }

    @ParallelTest
    public void testWithHostPort() {
        JsonObject configuration = new JsonObject().put("option.with.port", "my-server:9092");
        OrderedProperties expectedConfiguration = createWithDefaults("option.with.port", "my-server:9092");

        AbstractConfiguration config = new TestConfiguration(configuration);
        assertThat(config.asOrderedProperties(), is(expectedConfiguration));
    }

    @ParallelTest
    public void testKafkaCipherSuiteOverride() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("ssl.cipher.suites", "cipher1,cipher2,cipher3"); // valid

        KafkaConfiguration kc = new KafkaConfiguration(Reconciliation.DUMMY_RECONCILIATION, conf.entrySet());

        assertThat(kc.asOrderedProperties().asMap().get("ssl.cipher.suites"), is("cipher1,cipher2,cipher3"));
    }

    @ParallelTest
    public void testKafkaConnectHostnameVerification() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("key.converter", "my.package.Converter"); // valid
        conf.put("ssl.endpoint.identification.algorithm", ""); // valid
        conf.put("ssl.keystore.location", "/tmp/my.keystore"); // invalid

        KafkaConnectConfiguration configuration = new KafkaConnectConfiguration(Reconciliation.DUMMY_RECONCILIATION, conf.entrySet());

        assertThat(configuration.asOrderedProperties().asMap().get("key.converter"), is("my.package.Converter"));
        assertThat(configuration.asOrderedProperties().asMap().get("ssl.keystore.location"), is(nullValue()));
        assertThat(configuration.asOrderedProperties().asMap().get("ssl.endpoint.identification.algorithm"), is(""));
    }

    @ParallelTest
    public void testCreateOrAddListConfigDoesNotExists() {
        OrderedProperties props1 = new OrderedProperties();
        AbstractConfiguration.createOrAddListProperty(props1, "test-key", "test-value");
        assertThat(props1.asMap().get("test-key"), is("test-value"));

        OrderedProperties props2 = new OrderedProperties();
        AbstractConfiguration.createOrAddListProperty(props2, "test-key", "test-value-1,test-value-2");
        assertThat(props2.asMap().get("test-key"), is("test-value-1,test-value-2"));
    }

    @ParallelTest
    public void testCreateOrAddListConfigExists() {
        OrderedProperties props1 = new OrderedProperties();
        props1.addPair("test-key", "test-value-1");

        AbstractConfiguration.createOrAddListProperty(props1, "test-key", "test-value-2");
        assertThat(props1.asMap().get("test-key"), is("test-value-1,test-value-2"));

        OrderedProperties props2 = new OrderedProperties();
        props2.addPair("test-key", "test-value-1,test-value-2");
        AbstractConfiguration.createOrAddListProperty(props2, "test-key", "test-value-3");
        assertThat(props2.asMap().get("test-key"), is("test-value-1,test-value-2,test-value-3"));
    }

    @ParallelTest
    public void testCreateOrAddListConfigExistsAndContainsValue() {
        OrderedProperties props1 = new OrderedProperties();
        props1.addPair("test-key", "test-value-1");
        AbstractConfiguration.createOrAddListProperty(props1, "test-key", "test-value-1");
        assertThat(props1.asMap().get("test-key"), is("test-value-1"));

        OrderedProperties props2 = new OrderedProperties();
        props2.addPair("test-key", "test-value-1,test-value-2");
        AbstractConfiguration.createOrAddListProperty(props2, "test-key", "test-value-1");
        assertThat(props2.asMap().get("test-key"), is("test-value-1,test-value-2"));
    }

    @ParallelTest
    public void testCreateOrAddListConfigWithDuplicates() {
        OrderedProperties props = new OrderedProperties();
        props.addPair("test-key", "test-value-1,test-value-1,test-value-2");
        AbstractConfiguration.createOrAddListProperty(props, "test-key", "test-value-3,test-value-3");
        assertThat(props.asMap().get("test-key"), is("test-value-1,test-value-2,test-value-3"));
    }

    @ParallelTest
    public void testCreateOrAddListConfigOrdering() {
        OrderedProperties props = new OrderedProperties();
        props.addPair("test-key", "test-value-2,test-value-1");
        AbstractConfiguration.createOrAddListProperty(props, "test-key", "test-value-3,");
        assertThat(props.asMap().get("test-key"), is("test-value-2,test-value-1,test-value-3"));
    }

    @ParallelTest
    public void testCreateOrAddListConfigWithNullConfig() {
        OrderedProperties props = new OrderedProperties();
        props.addPair("test-key", "test-value-1,test-value-2");
        Exception e = assertThrows(IllegalArgumentException.class,
                () -> AbstractConfiguration.createOrAddListProperty(null, "test-key", "test-value-3"));
        assertThat(e.getMessage(), is("Configuration is required"));
    }

    @ParallelTest
    public void testCreateOrAddListConfigWithNullKey() {
        OrderedProperties props = new OrderedProperties();
        props.addPair("test-key", "test-value-1,test-value-2");
        Stream.of(null, "", " ")
                .map(key -> assertThrows(IllegalArgumentException.class, () -> AbstractConfiguration.createOrAddListProperty(props, key, "test-value-3")))
                .forEach(e -> assertThat(e.getMessage(), is("Configuration key is required")));
    }

    @ParallelTest
    public void testCreateOrAddListConfigWithNullValue() {
        OrderedProperties props = new OrderedProperties();
        props.addPair("test-key", "test-value-1,test-value-2");
        Stream.of(null, "", " ")
                .map(value -> assertThrows(IllegalArgumentException.class, () -> AbstractConfiguration.createOrAddListProperty(props, "test-key", value)))
                .forEach(e -> assertThat(e.getMessage(), is("Configuration values are required")));
    }
}

class TestConfiguration extends AbstractConfiguration {
    private static final List<String> FORBIDDEN_OPTIONS;
    private static final List<String> FORBIDDEN_PREFIXES;
    private static final List<String> FORBIDDEN_PREFIXES_EXCEPTIONS;
    private static final Map<String, String> DEFAULTS;

    static {
        FORBIDDEN_PREFIXES = List.of("forbidden.prefix");
        FORBIDDEN_PREFIXES_EXCEPTIONS = List.of("forbidden.prefix.exception");
        FORBIDDEN_OPTIONS = List.of("forbidden.option");

        DEFAULTS = new HashMap<>();
        DEFAULTS.put("default.option", "hello");
    }

    /**
     * Constructor used to instantiate this class from String configuration. Should be used to create configuration
     * from the Assembly.
     *
     * @param configuration Configuration in String format. Should contain zero or more lines with key=value
     *                      pairs.
     */
    public TestConfiguration(String configuration) {
        super(Reconciliation.DUMMY_RECONCILIATION, configuration, FORBIDDEN_PREFIXES, FORBIDDEN_PREFIXES_EXCEPTIONS, FORBIDDEN_OPTIONS, DEFAULTS);
    }

    /**
     * Constructor used to instantiate this class from JsonObject. Should be used to create configuration from
     * ConfigMap / CRD.
     *
     * @param jsonOptions     Json object with configuration options as key ad value pairs.
     */
    public TestConfiguration(JsonObject jsonOptions) {
        super(Reconciliation.DUMMY_RECONCILIATION, jsonOptions, FORBIDDEN_PREFIXES, FORBIDDEN_PREFIXES_EXCEPTIONS, FORBIDDEN_OPTIONS, DEFAULTS);
    }
}

class TestConfigurationWithoutDefaults extends AbstractConfiguration {
    private static final List<String> FORBIDDEN_OPTIONS;
    private static final List<String> FORBIDDEN_PREFIXES;
    private static final List<String> FORBIDDEN_PREFIXES_EXCEPTIONS;

    static {
        FORBIDDEN_PREFIXES = List.of("forbidden.prefix");
        FORBIDDEN_PREFIXES_EXCEPTIONS = List.of("forbidden.prefix.exception");
        FORBIDDEN_OPTIONS = List.of("forbidden.option");
    }

    /**
     * Constructor used to instantiate this class from String configuration. Should be used to create configuration
     * from the Assembly.
     *
     * @param configuration Configuration in String format. Should contain zero or more lines with key=value
     *                      pairs.
     */
    public TestConfigurationWithoutDefaults(String configuration) {
        super(Reconciliation.DUMMY_RECONCILIATION, configuration, FORBIDDEN_PREFIXES, FORBIDDEN_PREFIXES_EXCEPTIONS, FORBIDDEN_OPTIONS, Map.of());
    }

    /**
     * Constructor used to instantiate this class from JsonObject. Should be used to create configuration from
     * ConfigMap / CRD.
     *
     * @param jsonOptions     Json object with configuration options as key ad value pairs.
     */
    public TestConfigurationWithoutDefaults(JsonObject jsonOptions) {
        super(Reconciliation.DUMMY_RECONCILIATION, jsonOptions, FORBIDDEN_PREFIXES, FORBIDDEN_PREFIXES_EXCEPTIONS, FORBIDDEN_OPTIONS, Map.of());
    }
}
