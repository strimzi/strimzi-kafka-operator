/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.strimzi.operator.common.InvalidConfigurationException;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ConfigTest {
    private static final Map<String, String> MANDATORY = new HashMap<>();

    static {
        MANDATORY.put(Config.ZOOKEEPER_CONNECT.key, "localhost:2181");
        MANDATORY.put(Config.KAFKA_BOOTSTRAP_SERVERS.key, "localhost:9092");
        MANDATORY.put(Config.NAMESPACE.key, "default");
        MANDATORY.put(Config.CLIENT_ID.key, "default-client-id");
    }

    @Test
    public void testUnknownKeyThrows() {
        assertThrows(IllegalArgumentException.class, () -> new Config(Collections.singletonMap("foo", "bar")));
    }

    @Test
    public void testEmptyMapThrows() {
        assertThrows(IllegalArgumentException.class, () -> new Config(Collections.emptyMap()));
    }

    @Test
    public void testDefaultInput() {
        Map<String, String> map = new HashMap<>(MANDATORY);
        Config c = new Config(map);
        assertThat(c.get(Config.ZOOKEEPER_SESSION_TIMEOUT_MS).intValue(), is(18_000));
    }

    @Test
    public void testOverrideZookeeperSessionTimeout() {
        Map<String, String> map = new HashMap<>(MANDATORY);
        map.put(Config.ZOOKEEPER_SESSION_TIMEOUT_MS.key, "13000");

        Config c = new Config(map);
        assertThat(c.get(Config.ZOOKEEPER_SESSION_TIMEOUT_MS).intValue(), is(13_000));
    }

    @Test
    public void testNewInvalidTimeout() {
        Map<String, String> map = new HashMap<>(MANDATORY);
        map.put(Config.ZOOKEEPER_SESSION_TIMEOUT_MS.key, "13000");

        assertDoesNotThrow(() -> new Config(map));

        map.put(Config.ZOOKEEPER_SESSION_TIMEOUT_MS.key, "foos");
        assertThrows(IllegalArgumentException.class, () -> new Config(map));
    }

    @Test
    public void testTopicMetadataMaxAttemptsIsSetCorrectly() {

        Map<String, String> map = new HashMap<>(MANDATORY);
        map.put(Config.TC_TOPIC_METADATA_MAX_ATTEMPTS, "3");

        Config c = new Config(map);
        assertThat(c.get(Config.TOPIC_METADATA_MAX_ATTEMPTS), is(3));
    }

    @Test
    public void testDefaultConfig() {
        Map<String, String> map = new HashMap<>(MANDATORY);

        Config config = new Config(map);
        Session session = new Session(null, config);
        Properties adminClientProps = session.adminClientProperties();

        assertThat(adminClientProps.getProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG), is("PLAINTEXT"));
        assertNull(adminClientProps.getProperty(SaslConfigs.SASL_MECHANISM));
        assertNull(adminClientProps.getProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG));
    }

    @Test
    public void testTlsEnabledConfig() {
        Map<String, String> map = new HashMap<>(MANDATORY);
        map.put(Config.TLS_ENABLED.key, "true");

        Config config = new Config(map);
        Session session = new Session(null, config);
        Properties adminClientProps = session.adminClientProperties();

        assertThat(adminClientProps.getProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG), is("HTTPS"));
        assertThat(adminClientProps.getProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG), is("SSL"));
    }

    @Test
    public void testSecurityProtocolConfig() {
        Map<String, String> map = new HashMap<>(MANDATORY);
        map.put(Config.SECURITY_PROTOCOL.key, "SSL");

        Config config = new Config(map);
        Session session = new Session(null, config);
        Properties adminClientProps = session.adminClientProperties();

        assertThat(adminClientProps.getProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG), is("HTTPS"));
        assertThat(adminClientProps.getProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG), is("SSL"));
    }

    @Test
    public void testInvalidSaslConfig() {
        Map<String, String> map = new HashMap<>(MANDATORY);
        map.put(Config.SASL_ENABLED.key, "true");

        Config configNoMechanism = new Config(map);
        Session sessionNoMechanism = new Session(null, configNoMechanism);
        var noMechanismException =  assertThrows(IllegalArgumentException.class, sessionNoMechanism::adminClientProperties);
        assertEquals("Invalid SASL_MECHANISM: ''", noMechanismException.getMessage());

        map.put(Config.SASL_MECHANISM.key, "plain");

        String username = "admin";
        String password = "password";

        map.put(Config.SASL_PASSWORD.key, password);
        Config configWithoutUsername = new Config(map);
        Session sessionWithoutUsername = new Session(null, configWithoutUsername);
        var noUsernameException = assertThrows(InvalidConfigurationException.class, sessionWithoutUsername::adminClientProperties);
        assertEquals("SASL credentials are not set", noUsernameException.getMessage());

        map.put(Config.SASL_USERNAME.key, password);
        map.remove(Config.SASL_PASSWORD.key);
        Config configWithoutPassword = new Config(map);
        Session sessionWithoutPassword = new Session(null, configWithoutPassword);
        var noPasswordException = assertThrows(InvalidConfigurationException.class, sessionWithoutPassword::adminClientProperties);
        assertEquals("SASL credentials are not set", noPasswordException.getMessage());

        map.put(Config.SASL_MECHANISM.key, "custom");
        map.remove(Config.SASL_USERNAME.key);
        Config configCustomMechanism = new Config(map);
        Session sessionCustomMechanism = new Session(null, configCustomMechanism);
        var customMechanismException = assertThrows(InvalidConfigurationException.class, sessionCustomMechanism::adminClientProperties);
        assertEquals("Custom SASL config properties are not set", customMechanismException.getMessage());

        map.put(Config.SASL_CUSTOM_CONFIG.key, "{}");
        Config configCustomProps = new Config(map);
        Session sessionCustomProps = new Session(null, configCustomProps);
        var customPropsEmptyException = assertThrows(InvalidConfigurationException.class, sessionCustomProps::adminClientProperties);
        assertEquals("SASL custom config properties empty", customPropsEmptyException.getMessage());

        map.put(Config.SASL_CUSTOM_CONFIG.key, "{");
        Config configCustomPropsInvalid = new Config(map);
        Session sessionCustomPropsInvalid = new Session(null, configCustomPropsInvalid);
        var customPropsInvalidException = assertThrows(InvalidConfigurationException.class, sessionCustomPropsInvalid::adminClientProperties);
        assertEquals("SASL custom config properties deserialize failed. customProperties: '{'", customPropsInvalidException.getMessage());

        map.put(Config.SASL_CUSTOM_CONFIG.key, "{ \"a\": \"b\" }");
        Config configCustomPropsNotSasl = new Config(map);
        Session sessionCustomPropsNotSasl = new Session(null, configCustomPropsNotSasl);
        var customPropsNotSaslException = assertThrows(InvalidConfigurationException.class, sessionCustomPropsNotSasl::adminClientProperties);
        assertEquals("SASL custom config properties not SASL properties. customProperty: 'a' = 'b'", customPropsNotSaslException.getMessage());

        map.put(Config.SASL_CUSTOM_CONFIG.key, "{ \"\": \"b\" }");
        configCustomPropsNotSasl = new Config(map);
        sessionCustomPropsNotSasl = new Session(null, configCustomPropsNotSasl);
        customPropsNotSaslException = assertThrows(InvalidConfigurationException.class, sessionCustomPropsNotSasl::adminClientProperties);
        assertEquals("SASL custom config properties not SASL properties. customProperty: '' = 'b'", customPropsNotSaslException.getMessage());
    }

    @Test
    public void testInvalidTlsSecurityProtocolConfig() {
        Map<String, String> map = new HashMap<>(MANDATORY);
        map.put(Config.SECURITY_PROTOCOL.key, "PLAINTEXT");
        map.put(Config.TLS_ENABLED.key, "true");

        Config config = new Config(map);
        Session session = new Session(null, config);
        assertThrows(InvalidConfigurationException.class, session::adminClientProperties);
    }

    @Test
    public void testInvalidKeystoreConfig() {
        Map<String, String> map = new HashMap<>(MANDATORY);
        map.put(Config.TLS_ENABLED.key, "true");
        map.put(Config.TLS_TRUSTSTORE_PASSWORD.key, "password");

        Config config = new Config(map);
        Session session = new Session(null, config);
        assertThrows(InvalidConfigurationException.class, session::adminClientProperties);
    }

    @Test
    public void testScramSaslConfig() {
        Map<String, String> map = new HashMap<>(MANDATORY);
        map.put(Config.SASL_ENABLED.key, "true");

        String username = "admin";
        String password = "password";
        map.put(Config.SASL_USERNAME.key, username);
        map.put(Config.SASL_PASSWORD.key, password);
        String scramJaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + username + "\" password=\"" + password + "\";";
        String plainJaasConfig = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + username + "\" password=\"" + password + "\";";

        map.put(Config.SASL_MECHANISM.key, "scram-sha-256");
        Config configSHA256 = new Config(map);
        Session sessionSHA256 = new Session(null, configSHA256);

        Properties adminClientPropsSHA256 = sessionSHA256.adminClientProperties();
        assertThat(adminClientPropsSHA256.getProperty(SaslConfigs.SASL_MECHANISM), is("SCRAM-SHA-256"));
        assertThat(adminClientPropsSHA256.getProperty(SaslConfigs.SASL_JAAS_CONFIG), is(scramJaasConfig));

        map.put(Config.SASL_MECHANISM.key, "scram-sha-512");
        Config configSHA512 = new Config(map);
        Session sessionSHA512 = new Session(null, configSHA512);

        Properties adminClientPropsSHA512 = sessionSHA512.adminClientProperties();
        assertThat(adminClientPropsSHA512.getProperty(SaslConfigs.SASL_MECHANISM), is("SCRAM-SHA-512"));
        assertThat(adminClientPropsSHA512.getProperty(SaslConfigs.SASL_JAAS_CONFIG), is(scramJaasConfig));

        map.put(Config.SASL_MECHANISM.key, "plain");
        Config configPlain = new Config(map);
        Session sessionPlain = new Session(null, configPlain);

        Properties adminClientPropsPlain = sessionPlain.adminClientProperties();
        assertThat(adminClientPropsPlain.getProperty(SaslConfigs.SASL_MECHANISM), is("PLAIN"));
        assertThat(adminClientPropsPlain.getProperty(SaslConfigs.SASL_JAAS_CONFIG), is(plainJaasConfig));
    }

    @Test
    public void testCustomSaslConfig() {
        Map<String, String> map = new HashMap<>(MANDATORY);
        map.put(Config.SASL_ENABLED.key, "true");
        map.put(Config.SASL_MECHANISM.key, "custom");
        map.put(Config.SASL_CUSTOM_CONFIG.key, """
                    {
                        "sasl.mechanism": "WAS_SMK_IAM",
                        "sasl.jaas.config": "some.custom.auth.iam.IAMLoginModule required;",
                        "sasl.client.callback.handler.class": "some.other.nonstandard.iam.IAMClientCallbackHandler"
                    }
                    """);

        String jaasConfig = "some.custom.auth.iam.IAMLoginModule required;";

        Config customSaslconfig = new Config(map);
        Session customSsession = new Session(null, customSaslconfig);

        Properties adminClientCustomProps = customSsession.adminClientProperties();
        assertThat(adminClientCustomProps.getProperty(SaslConfigs.SASL_MECHANISM), is("WAS_SMK_IAM"));
        assertThat(adminClientCustomProps.getProperty(SaslConfigs.SASL_JAAS_CONFIG), is(jaasConfig));
        assertThat(adminClientCustomProps.getProperty("sasl.client.callback.handler.class"), is("some.other.nonstandard.iam.IAMClientCallbackHandler"));
    }
}
