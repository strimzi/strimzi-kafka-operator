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

import static io.strimzi.test.TestUtils.LINE_SEPARATOR;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
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
                "forbidden.option=ddd" + LINE_SEPARATOR;
        OrderedProperties expectedConfiguration = createWithDefaults("var3", "ccc",
                "var2", "bbb",
                "var1", "aaa");

        AbstractConfiguration config = new TestConfiguration(configuration);
        assertThat(config.asOrderedProperties(), is(expectedConfiguration));
    }

    @ParallelTest
    public void testConfigurationStringWithForbiddenKeysInUpperCase() {
        String configuration = "var1=aaa" + LINE_SEPARATOR +
                "var2=bbb" + LINE_SEPARATOR +
                "var3=ccc" + LINE_SEPARATOR +
                "FORBIDDEN.OPTION=ddd" + LINE_SEPARATOR;
        OrderedProperties expectedConfiguration = createWithDefaults("var3", "ccc",
                "var2", "bbb",
                "var1", "aaa");

        AbstractConfiguration config = new TestConfiguration(configuration);
        assertThat(config.asOrderedProperties(), is(expectedConfiguration));
    }

    @ParallelTest
    public void testJsonWithForbiddenKeys() {
        JsonObject configuration = new JsonObject().put("var1", "aaa")
                .put("var2", "bbb")
                .put("var3", "ccc")
                .put("forbidden.option", "ddd");
        OrderedProperties expectedConfiguration = createWithDefaults("var3", "ccc",
                "var2", "bbb",
                "var1", "aaa");

        AbstractConfiguration config = new TestConfiguration(configuration);
        assertThat(config.asOrderedProperties(), is(expectedConfiguration));
    }

    @ParallelTest
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
    public void testKafkaZookeeperTimeout() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("valid", "validValue");
        conf.put("zookeeper.connection.whatever", "invalid");
        conf.put("security.invalid1", "invalid");
        conf.put("zookeeper.connection.timeout.ms", "42"); // valid
        conf.put("zookeeper.connection.timeout", "42"); // invalid

        KafkaConfiguration kc = new KafkaConfiguration(Reconciliation.DUMMY_RECONCILIATION, conf.entrySet());

        assertThat(kc.asOrderedProperties().asMap().get("valid"), is("validValue"));
        assertThat(kc.asOrderedProperties().asMap().get("zookeeper.connection.whatever"), is(nullValue()));
        assertThat(kc.asOrderedProperties().asMap().get("security.invalid1"), is(nullValue()));
        assertThat(kc.asOrderedProperties().asMap().get("zookeeper.connection.timeout.ms"), is("42"));
        assertThat(kc.asOrderedProperties().asMap().get("zookeeper.connection.timeout"), is(nullValue()));
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
    public void testKafkaMirrorMakerConsumerHostnameVerification() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("compression.type", "zstd"); // valid
        conf.put("ssl.endpoint.identification.algorithm", ""); // valid
        conf.put("ssl.keystore.location", "/tmp/my.keystore"); // invalid

        KafkaMirrorMakerConsumerConfiguration configuration = new KafkaMirrorMakerConsumerConfiguration(Reconciliation.DUMMY_RECONCILIATION, conf.entrySet());

        assertThat(configuration.asOrderedProperties().asMap().get("compression.type"), is("zstd"));
        assertThat(configuration.asOrderedProperties().asMap().get("ssl.keystore.location"), is(nullValue()));
        assertThat(configuration.asOrderedProperties().asMap().get("ssl.endpoint.identification.algorithm"), is(""));
    }

    @ParallelTest
    public void testKafkaMirrorMakerProducerHostnameVerification() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("compression.type", "zstd"); // valid
        conf.put("ssl.endpoint.identification.algorithm", ""); // valid
        conf.put("ssl.keystore.location", "/tmp/my.keystore"); // invalid

        KafkaMirrorMakerProducerConfiguration configuration = new KafkaMirrorMakerProducerConfiguration(Reconciliation.DUMMY_RECONCILIATION, conf.entrySet());

        assertThat(configuration.asOrderedProperties().asMap().get("compression.type"), is("zstd"));
        assertThat(configuration.asOrderedProperties().asMap().get("ssl.keystore.location"), is(nullValue()));
        assertThat(configuration.asOrderedProperties().asMap().get("ssl.endpoint.identification.algorithm"), is(""));
    }

    @ParallelTest
    public void testSplittingOfPrefixes()   {
        String prefixes = "prefix1.field,prefix2.field , prefix3.field, prefix4.field,, ";
        List<String> prefixList = asList("prefix1.field", "prefix2.field", "prefix3.field", "prefix4.field");

        assertThat(AbstractConfiguration.splitPrefixesToList(prefixes).equals(prefixList), is(true));
    }
}

class TestConfiguration extends AbstractConfiguration {
    private static final List<String> FORBIDDEN_PREFIXES;
    private static final Map<String, String> DEFAULTS;

    static {
        FORBIDDEN_PREFIXES = asList(
                "forbidden.option");

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
        super(Reconciliation.DUMMY_RECONCILIATION, configuration, FORBIDDEN_PREFIXES, DEFAULTS);
    }

    /**
     * Constructor used to instantiate this class from JsonObject. Should be used to create configuration from
     * ConfigMap / CRD.
     *
     * @param jsonOptions     Json object with configuration options as key ad value pairs.
     */
    public TestConfiguration(JsonObject jsonOptions) {
        super(Reconciliation.DUMMY_RECONCILIATION, jsonOptions, FORBIDDEN_PREFIXES, DEFAULTS);
    }
}

class TestConfigurationWithoutDefaults extends AbstractConfiguration {
    private static final List<String> FORBIDDEN_PREFIXES;

    static {
        FORBIDDEN_PREFIXES = asList(
                "forbidden.option");
    }

    /**
     * Constructor used to instantiate this class from String configuration. Should be used to create configuration
     * from the Assembly.
     *
     * @param configuration Configuration in String format. Should contain zero or more lines with key=value
     *                      pairs.
     */
    public TestConfigurationWithoutDefaults(String configuration) {
        super(Reconciliation.DUMMY_RECONCILIATION, configuration, FORBIDDEN_PREFIXES);
    }

    /**
     * Constructor used to instantiate this class from JsonObject. Should be used to create configuration from
     * ConfigMap / CRD.
     *
     * @param jsonOptions     Json object with configuration options as key ad value pairs.
     */
    public TestConfigurationWithoutDefaults(JsonObject jsonOptions) {
        super(Reconciliation.DUMMY_RECONCILIATION, jsonOptions, FORBIDDEN_PREFIXES);
    }
}
