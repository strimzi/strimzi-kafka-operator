/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.featuregates.FeatureGates;
import io.strimzi.test.ReadWriteUtils;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopicOperatorConfigTest {
    private static final String NAMESPACE = TopicOperatorTestUtil.namespaceName(TopicOperatorConfigTest.class);

    @Test
    void shouldConnectWithPlaintextAndNotAuthn() {
        // given
        var config = TopicOperatorConfig.buildFromMap(Map.of(
                TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "my-kafka:9092",
                TopicOperatorConfig.NAMESPACE.key(), NAMESPACE));

        // then
        assertEquals(NAMESPACE, config.namespace());

        var adminConfig = config.adminClientConfig();
        // client.id is random, so check it's there then remove for an easier assertion on the rest of the map
        assertTrue(!adminConfig.get("client.id").toString().isEmpty());
        adminConfig.remove("client.id");
        assertEquals(Map.of(
                "security.protocol", "PLAINTEXT",
                "bootstrap.servers", "my-kafka:9092"), adminConfig);
    }

    @Test
    void shouldConnectWithTls() {
        // given
        var config = TopicOperatorConfig.buildFromMap(Map.of(
                TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "my-kafka:9092",
                TopicOperatorConfig.NAMESPACE.key(), NAMESPACE,
                TopicOperatorConfig.TLS_ENABLED.key(), "true",
                TopicOperatorConfig.TRUSTSTORE_LOCATION.key(), "my/trusstore.p12",
                TopicOperatorConfig.TRUSTSTORE_PASSWORD.key(), "123456"));

        // then
        assertEquals(NAMESPACE, config.namespace());

        var adminConfig = config.adminClientConfig();
        // client.id is random, so check it's there then remove for an easier assertion on the rest of the map
        assertTrue(!adminConfig.get("client.id").toString().isEmpty());
        adminConfig.remove("client.id");
        assertEquals(Map.of(
                "security.protocol", "SSL",
                "bootstrap.servers", "my-kafka:9092",
                "ssl.truststore.location", "my/trusstore.p12",
                "ssl.truststore.password", "123456",
                "ssl.endpoint.identification.algorithm", "HTTPS"), adminConfig);
    }

    @Test
    void shouldConnectWithSaslPlain() {
        // given
        var config = TopicOperatorConfig.buildFromMap(Map.of(
                TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "my-kafka:9092",
                TopicOperatorConfig.NAMESPACE.key(), NAMESPACE,
                TopicOperatorConfig.SECURITY_PROTOCOL.key(), "SASL_PLAINTEXT",
                TopicOperatorConfig.SASL_ENABLED.key(), "true",
                TopicOperatorConfig.SASL_MECHANISM.key(), "plain",
                TopicOperatorConfig.SASL_USERNAME.key(), "foo",
                TopicOperatorConfig.SASL_PASSWORD.key(), "foo"
        ));

        // then
        assertEquals(NAMESPACE, config.namespace());

        var adminConfig = config.adminClientConfig();
        // client.id is random, so check it's there then remove for an easier assertion on the rest of the map
        assertTrue(!adminConfig.get("client.id").toString().isEmpty());
        adminConfig.remove("client.id");
        assertEquals(Map.of(
                "security.protocol", "SASL_PLAINTEXT",
                "bootstrap.servers", "my-kafka:9092",
                "sasl.mechanism", "PLAIN",
                "sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"foo\" password=\"foo\";"), adminConfig);
    }

    @Test
    void shouldConnectWithSaslPlainWithTls() {
        // given
        var config = TopicOperatorConfig.buildFromMap(Map.of(
                TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "my-kafka:9092",
                TopicOperatorConfig.NAMESPACE.key(), NAMESPACE,
                TopicOperatorConfig.SECURITY_PROTOCOL.key(), "SASL_SSL",
                TopicOperatorConfig.TLS_ENABLED.key(), "true",
                TopicOperatorConfig.TRUSTSTORE_LOCATION.key(), "my/trusstore.p12",
                TopicOperatorConfig.TRUSTSTORE_PASSWORD.key(), "123456",
                TopicOperatorConfig.SASL_ENABLED.key(), "true",
                TopicOperatorConfig.SASL_MECHANISM.key(), "plain",
                TopicOperatorConfig.SASL_USERNAME.key(), "foo",
                TopicOperatorConfig.SASL_PASSWORD.key(), "foo"
        ));

        // then
        assertEquals(NAMESPACE, config.namespace());

        var adminConfig = config.adminClientConfig();
        // client.id is random, so check it's there then remove for an easier assertion on the rest of the map
        assertTrue(!adminConfig.get("client.id").toString().isEmpty());
        adminConfig.remove("client.id");
        assertEquals(Map.of(
                "security.protocol", "SASL_SSL",
                "bootstrap.servers", "my-kafka:9092",
                "sasl.mechanism", "PLAIN",
                "sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"foo\" password=\"foo\";",
                "ssl.truststore.location", "my/trusstore.p12",
                "ssl.truststore.password", "123456",
                "ssl.endpoint.identification.algorithm", "HTTPS"), adminConfig);
    }

    @Test
    void shouldAcceptSaslScramSha256() {
        saslScramSha(256);
    }

    @Test
    void shouldAcceptSaslScramSha512() {
        saslScramSha(512);
    }

    @Test
    void shouldThrowWithDuplicateConfig() {
        var thrown =  assertThrows(IllegalArgumentException.class, 
            () -> TopicOperatorConfig.buildFromMap(Map.of(
                TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "my-kafka:9092",
                TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "my-kafka:9093",
                TopicOperatorConfig.NAMESPACE.key(), NAMESPACE
            )));
        assertEquals("duplicate key: STRIMZI_KAFKA_BOOTSTRAP_SERVERS", thrown.getMessage());
    }

    @Test
    void shouldThrowWithMissingRequired() {
        var thrown1 =  assertThrows(InvalidConfigurationException.class,
            () -> TopicOperatorConfig.buildFromMap(Map.of(
                TopicOperatorConfig.NAMESPACE.key(), NAMESPACE
            )));
        assertEquals("Config value: STRIMZI_KAFKA_BOOTSTRAP_SERVERS is mandatory", thrown1.getMessage());

        var thrown2 =  assertThrows(InvalidConfigurationException.class,
            () -> TopicOperatorConfig.buildFromMap(Map.of(
                TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "my-kafka:9092"
            )));
        assertEquals("Config value: STRIMZI_NAMESPACE is mandatory", thrown2.getMessage());
    }

    @Test
    void shouldRejectInvalidSaslMechanism() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> saslScramSha(511));
        assertEquals("Invalid SASL_MECHANISM type: scram-sha-511", e.getMessage());
    }

    void saslScramSha(int bits) {
        // given
        var config = TopicOperatorConfig.buildFromMap(Map.of(
                TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234",
                TopicOperatorConfig.NAMESPACE.key(), NAMESPACE,
                TopicOperatorConfig.SASL_ENABLED.key(), "true",
                TopicOperatorConfig.SASL_MECHANISM.key(), "scram-sha-"  + bits,
                TopicOperatorConfig.SASL_USERNAME.key(), "foo",
                TopicOperatorConfig.SASL_PASSWORD.key(), "pa55word"
                ));

        // then
        assertEquals(NAMESPACE, config.namespace());

        var adminConfig = config.adminClientConfig();
        // client.id is random, so check it's there then remove for an easier assertion on the rest of the map
        assertTrue(!adminConfig.get("client.id").toString().isEmpty());
        adminConfig.remove("client.id");
        assertEquals(Map.of(
                "security.protocol", "PLAINTEXT",
                "bootstrap.servers", "localhost:1234",
                "sasl.mechanism", "SCRAM-SHA-" + bits,
                "sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"foo\" password=\"pa55word\";"), adminConfig);
    }
    
    @Test
    void shouldThrowIfSecurityProtocolInconsistentWithTls() {
        // given
        var config = TopicOperatorConfig.buildFromMap(Map.of(
                TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234",
                TopicOperatorConfig.NAMESPACE.key(), NAMESPACE,
                TopicOperatorConfig.TLS_ENABLED.key(), "true",
                TopicOperatorConfig.SECURITY_PROTOCOL.key(), "PLAINTEXT"));

        // then
        var e = assertThrows(InvalidConfigurationException.class, () -> config.adminClientConfig());
        assertEquals("TLS is enabled but the security protocol does not match SSL or SASL_SSL", e.getMessage());
    }

    @Test
    void shouldThrowIfSaslButNotCredentials() {
        // given
        var config = TopicOperatorConfig.buildFromMap(Map.of(
                TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234",
                TopicOperatorConfig.NAMESPACE.key(), NAMESPACE,
                TopicOperatorConfig.SECURITY_PROTOCOL.key(), "SASL_PLAINTEXT",
                TopicOperatorConfig.SASL_ENABLED.key(), "true",
                TopicOperatorConfig.SASL_MECHANISM.key(), "plain"
        ));

        // then
        var e = assertThrows(InvalidConfigurationException.class, () -> config.adminClientConfig());
        assertEquals("SASL credentials are not set", e.getMessage());
    }

    @Test
    void shouldThrowIfTrustStorePasswordButNoLocation() {
        // given
        var config = TopicOperatorConfig.buildFromMap(Map.of(
                TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234",
                TopicOperatorConfig.NAMESPACE.key(), NAMESPACE,
                TopicOperatorConfig.TLS_ENABLED.key(), "true",
                TopicOperatorConfig.TRUSTSTORE_PASSWORD.key(), "some_password"));

        // then
        assertEquals(NAMESPACE, config.namespace());

        var e = assertThrows(InvalidConfigurationException.class, () -> config.adminClientConfig());
        assertEquals("TLS_TRUSTSTORE_PASSWORD was supplied but TLS_TRUSTSTORE_LOCATION was not supplied", e.getMessage());
    }

    @Test
    void shouldThrowIfKeyStorePasswordButNoLocation() {
        // given
        var config = TopicOperatorConfig.buildFromMap(Map.of(
                TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234",
                TopicOperatorConfig.NAMESPACE.key(), NAMESPACE,
                TopicOperatorConfig.TLS_ENABLED.key(), "true",
                TopicOperatorConfig.TRUSTSTORE_LOCATION.key(), "/path/to/truststore",
                TopicOperatorConfig.TRUSTSTORE_PASSWORD.key(), "password_from_truststore",
                TopicOperatorConfig.KEYSTORE_LOCATION.key(), "/path/to/keystore",
                TopicOperatorConfig.KEYSTORE_PASSWORD.key(), "password_for_keystore"));

        // then
        assertEquals(NAMESPACE, config.namespace());

        var adminConfig = config.adminClientConfig();
        assertEquals("/path/to/keystore", adminConfig.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
        assertEquals("password_for_keystore", adminConfig.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG));
    }

    @Test
    void shouldMaskSecuritySensitiveConfigsInToString() {
        var config = TopicOperatorConfig.buildFromMap(Map.ofEntries(
                Map.entry(TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234"),
                Map.entry(TopicOperatorConfig.NAMESPACE.key(), NAMESPACE),
                Map.entry(TopicOperatorConfig.SECURITY_PROTOCOL.key(), "SASL_SSL"),
                Map.entry(TopicOperatorConfig.TLS_ENABLED.key(), "true"),
                Map.entry(TopicOperatorConfig.TRUSTSTORE_LOCATION.key(), "/some/path"),
                Map.entry(TopicOperatorConfig.TRUSTSTORE_PASSWORD.key(), "FORBIDDEN"),
                Map.entry(TopicOperatorConfig.KEYSTORE_LOCATION.key(), "/some/path"),
                Map.entry(TopicOperatorConfig.KEYSTORE_PASSWORD.key(), "FORBIDDEN"),
                Map.entry(TopicOperatorConfig.SASL_ENABLED.key(), "true"),
                Map.entry(TopicOperatorConfig.SASL_MECHANISM.key(), "plain"),
                Map.entry(TopicOperatorConfig.SASL_USERNAME.key(), "foo"),
                Map.entry(TopicOperatorConfig.SASL_PASSWORD.key(), "FORBIDDEN")
        ));

        assertFalse(config.toString().contains("FORBIDDEN"));
    }

    @Test
    public void shouldConfigureCustomSasl() {
        // given
        var config = TopicOperatorConfig.buildFromMap(Map.of(
              TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234",
              TopicOperatorConfig.NAMESPACE.key(), NAMESPACE,
              TopicOperatorConfig.SASL_ENABLED.key(), "true",
              TopicOperatorConfig.SASL_CUSTOM_CONFIG_JSON.key(), """
                    {
                        "sasl.mechanism": "WAS_SMK_IAM",
                        "sasl.jaas.config": "some.custom.auth.iam.IAMLoginModule required;",
                        "sasl.client.callback.handler.class": "some.other.nonstandard.iam.IAMClientCallbackHandler"
                    }
                    """
        ));

        // then
        assertEquals(NAMESPACE, config.namespace());

        var adminConfig = config.adminClientConfig();
        adminConfig.put("client.id", "foo");
        assertEquals(Map.of(
              "client.id", "foo",
              "security.protocol", "PLAINTEXT",
              "bootstrap.servers", "localhost:1234",
              "sasl.mechanism", "WAS_SMK_IAM",
              "sasl.jaas.config", "some.custom.auth.iam.IAMLoginModule required;",
              "sasl.client.callback.handler.class", "some.other.nonstandard.iam.IAMClientCallbackHandler"), adminConfig);
    }

    @Test
    void shouldThrowIfCustomConfigPropertyEmpty() {
        var config = TopicOperatorConfig.buildFromMap(Map.of(
              TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234",
              TopicOperatorConfig.NAMESPACE.key(), NAMESPACE,
              TopicOperatorConfig.SASL_ENABLED.key(), "true",
              TopicOperatorConfig.SASL_CUSTOM_CONFIG_JSON.key(), "{}"
        ));

        var customMechanismException = assertThrows(InvalidConfigurationException.class, config::adminClientConfig);
        assertEquals("SASL custom config properties empty", customMechanismException.getMessage());
    }

    @Test
    void shouldThrowIfCustomConfigInvalid() {
        var config = TopicOperatorConfig.buildFromMap(Map.of(
              TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234",
              TopicOperatorConfig.NAMESPACE.key(), NAMESPACE,
              TopicOperatorConfig.SASL_ENABLED.key(), "true",
              TopicOperatorConfig.SASL_CUSTOM_CONFIG_JSON.key(), "{"
        ));

        var customMechanismException = assertThrows(InvalidConfigurationException.class, config::adminClientConfig);
        assertEquals("SASL custom config properties deserialize failed. customProperties: '{'", customMechanismException.getMessage());
    }

    @Test
    void shouldThrowIfCustomConfigHasNonSaslProperties() {
        var config = TopicOperatorConfig.buildFromMap(Map.of(
              TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234",
              TopicOperatorConfig.NAMESPACE.key(), NAMESPACE,
              TopicOperatorConfig.SASL_ENABLED.key(), "true",
              TopicOperatorConfig.SASL_CUSTOM_CONFIG_JSON.key(), "{ \"a\": \"b\" }"
        ));

        var customMechanismException = assertThrows(InvalidConfigurationException.class, config::adminClientConfig);
        assertEquals("SASL custom config properties not SASL properties. customProperty: 'a' = 'b'", customMechanismException.getMessage());
    }

    @Test
    void shouldThrowIfCustomConfigHasEmptyProperties() {
        var config = TopicOperatorConfig.buildFromMap(Map.of(
              TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234",
              TopicOperatorConfig.NAMESPACE.key(), NAMESPACE,
              TopicOperatorConfig.SASL_ENABLED.key(), "true",
              TopicOperatorConfig.SASL_CUSTOM_CONFIG_JSON.key(), "{ \"\": \"b\" }"
        ));

        var customMechanismException = assertThrows(InvalidConfigurationException.class, config::adminClientConfig);
        assertEquals("SASL custom config properties not SASL properties. customProperty: '' = 'b'", customMechanismException.getMessage());
    }

    @Test
    void shouldDefaultToFalseForSkipClusterConfigCheck() {
        var config = TopicOperatorConfig.buildFromMap(Map.of(
              TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234",
              TopicOperatorConfig.NAMESPACE.key(), NAMESPACE
        ));

        assertFalse(config.skipClusterConfigReview());
    }

    @Test
    void shouldDefaultToAllForAlterableTopicConfig() {
        var config = TopicOperatorConfig.buildFromMap(Map.of(
              TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234",
              TopicOperatorConfig.NAMESPACE.key(), NAMESPACE
        ));

        assertEquals("ALL", config.alterableTopicConfig());
    }

    @Test
    public void testDefaultFeatureGates()    {
        TopicOperatorConfig config = TopicOperatorConfig.buildFromMap(Map.of(TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234", TopicOperatorConfig.NAMESPACE.key(), NAMESPACE));
        assertEquals(config.featureGates(), new FeatureGates(""));
    }

    @Test
    public void testFeatureGatesParsing()    {
        // We test that the configuration is really parsing the feature gates environment variable. We test it on
        // non-existing feature gate instead of a real one so that we do not have to change it when the FGs are promoted
        Map<String, String> envVars = Map.of(TopicOperatorConfig.FEATURE_GATES.key(), "-NonExistingGate");

        InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class, () -> TopicOperatorConfig.buildFromMap(envVars));
        assertEquals(e.getMessage(), "Unknown feature gate NonExistingGate found in the configuration");
    }

    @Test
    public void shouldConfigureCruiseControl() throws IOException {
        var inputFile = ReadWriteUtils.tempFile(UUID.randomUUID().toString(), ".txt");
        try (PrintWriter out = new PrintWriter(inputFile.getAbsolutePath())) {
            out.print("foo");
        }
        var expectedBytes = Files.readAllBytes(Path.of(inputFile.getAbsolutePath()));
        
        var config = TopicOperatorConfig.buildFromMap(Map.of(
            TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234",
            TopicOperatorConfig.NAMESPACE.key(), NAMESPACE,
            TopicOperatorConfig.CRUISE_CONTROL_ENABLED.key(), "true",
            TopicOperatorConfig.CRUISE_CONTROL_HOSTNAME.key(), "my-cruise-control",
            TopicOperatorConfig.CRUISE_CONTROL_PORT.key(), "9090",
            TopicOperatorConfig.CRUISE_CONTROL_SSL_ENABLED.key(), "true",
            TopicOperatorConfig.CRUISE_CONTROL_CRT_FILE_PATH.key(), inputFile.getAbsolutePath(),
            TopicOperatorConfig.CRUISE_CONTROL_AUTH_ENABLED.key(), "true",
            TopicOperatorConfig.CRUISE_CONTROL_API_USER_PATH.key(), inputFile.getAbsolutePath(),
            TopicOperatorConfig.CRUISE_CONTROL_API_PASS_PATH.key(), inputFile.getAbsolutePath()
        ));
        
        assertTrue(config.cruiseControlEnabled());
        assertEquals("my-cruise-control", config.cruiseControlHostname());
        assertEquals(9090, config.cruiseControlPort());
        assertTrue(config.cruiseControlSslEnabled());
        assertArrayEquals(expectedBytes, TopicOperatorUtil.getFileContent(config.cruiseControlCrtFilePath()));
        assertTrue(config.cruiseControlAuthEnabled());
        assertArrayEquals(expectedBytes, TopicOperatorUtil.getFileContent(config.cruiseControlApiUserPath()));
        assertArrayEquals(expectedBytes, TopicOperatorUtil.getFileContent(config.cruiseControlApiPassPath()));
    }

    @Test
    public void shouldThrowIfCruiseControlConfigHasNonExistentCertificate() {
        var config = TopicOperatorConfig.buildFromMap(Map.of(
            TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234",
            TopicOperatorConfig.NAMESPACE.key(), NAMESPACE,
            TopicOperatorConfig.CRUISE_CONTROL_ENABLED.key(), "true",
            TopicOperatorConfig.CRUISE_CONTROL_HOSTNAME.key(), "my-cruise-control",
            TopicOperatorConfig.CRUISE_CONTROL_PORT.key(), "9090",
            TopicOperatorConfig.CRUISE_CONTROL_SSL_ENABLED.key(), "true",
            TopicOperatorConfig.CRUISE_CONTROL_CRT_FILE_PATH.key(), "/4sa654a/d65sa65da"
        ));

        var thrown = assertThrows(IllegalArgumentException.class, config::cruiseControlClient);
        assertEquals("File not found: /4sa654a/d65sa65da", thrown.getMessage());
    }

    @Test
    public void shouldThrowIfCruiseControlClientHasNegativePort() {
        var thrown = assertThrows(InvalidConfigurationException.class,
            () -> TopicOperatorConfig.buildFromMap(Map.of(
                TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234",
                TopicOperatorConfig.NAMESPACE.key(), NAMESPACE,
                TopicOperatorConfig.CRUISE_CONTROL_ENABLED.key(), "true",
                TopicOperatorConfig.CRUISE_CONTROL_HOSTNAME.key(), "my-cruise-control",
                TopicOperatorConfig.CRUISE_CONTROL_PORT.key(), "-7000"
            )));
        assertEquals("Failed to parse. Negative value is not supported for this configuration", thrown.getMessage());
    }

    @Test
    public void shouldThrowIfCruiseControlConfigHasNonExistentUsernamePath() {
        var config = TopicOperatorConfig.buildFromMap(Map.of(
            TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234",
            TopicOperatorConfig.NAMESPACE.key(), NAMESPACE,
            TopicOperatorConfig.CRUISE_CONTROL_ENABLED.key(), "true",
            TopicOperatorConfig.CRUISE_CONTROL_HOSTNAME.key(), "my-cruise-control",
            TopicOperatorConfig.CRUISE_CONTROL_PORT.key(), "9090",
            TopicOperatorConfig.CRUISE_CONTROL_AUTH_ENABLED.key(), "true",
            TopicOperatorConfig.CRUISE_CONTROL_API_USER_PATH.key(), "/d4sa6a/das45da4s"
        ));

        var thrown = assertThrows(IllegalArgumentException.class, config::cruiseControlClient);
        assertEquals("File not found: /d4sa6a/das45da4s", thrown.getMessage());
    }
    
    @Test
    public void shouldThrowIfCruiseControlConfigHasNonExistentPasswordFile() {
        var inputFile = ReadWriteUtils.tempFile(UUID.randomUUID().toString(), ".txt");
        
        var config = TopicOperatorConfig.buildFromMap(Map.of(
            TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234",
            TopicOperatorConfig.NAMESPACE.key(), NAMESPACE,
            TopicOperatorConfig.CRUISE_CONTROL_ENABLED.key(), "true",
            TopicOperatorConfig.CRUISE_CONTROL_HOSTNAME.key(), "my-cruise-control",
            TopicOperatorConfig.CRUISE_CONTROL_PORT.key(), "9090",
            TopicOperatorConfig.CRUISE_CONTROL_AUTH_ENABLED.key(), "true",
            TopicOperatorConfig.CRUISE_CONTROL_API_USER_PATH.key(), inputFile.getAbsolutePath(),
            TopicOperatorConfig.CRUISE_CONTROL_API_PASS_PATH.key(), "/sd4sa6/fds6f7sfs"
        ));

        var thrown = assertThrows(IllegalArgumentException.class, config::cruiseControlClient);
        assertEquals("File not found: /sd4sa6/fds6f7sfs", thrown.getMessage());
    }
}
