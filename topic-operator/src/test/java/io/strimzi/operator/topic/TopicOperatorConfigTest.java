/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.SaslPlainAuth;
import io.kroxylicious.testing.kafka.common.Tls;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.featuregates.FeatureGates;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(KafkaClusterExtension.class)
class TopicOperatorConfigTest {

    @Test
    void shouldConnectWithPlaintextAndNotAuthn(KafkaCluster kc) throws ExecutionException, InterruptedException {
        // given
        var config = TopicOperatorConfig.buildFromMap(Map.of(
                TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), kc.getBootstrapServers(),
                TopicOperatorConfig.NAMESPACE.key(), "some-namespace"));

        // then
        assertEquals("some-namespace", config.namespace());

        var adminConfig = config.adminClientConfig();
        // client.id is random, so check it's there then remove for an easier assertion on the rest of the map
        assertTrue(!adminConfig.get("client.id").toString().isEmpty());
        adminConfig.remove("client.id");
        assertEquals(Map.of(
                "security.protocol", "PLAINTEXT",
                "bootstrap.servers", kc.getBootstrapServers()), adminConfig);
        Admin.create(adminConfig).describeCluster().clusterId().get();
    }

    @Test
    void shouldConnectWithTls(
                @Tls
                KafkaCluster kc) throws ExecutionException, InterruptedException {
        // given
        Map<String, Object> kafkaClientConfiguration = kc.getKafkaClientConfiguration();
        var config = TopicOperatorConfig.buildFromMap(Map.of(
                TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), kc.getBootstrapServers(),
                TopicOperatorConfig.NAMESPACE.key(), "some-namespace",
                TopicOperatorConfig.TLS_ENABLED.key(), "true",
                TopicOperatorConfig.TRUSTSTORE_LOCATION.key(), kafkaClientConfiguration.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG).toString(),
                TopicOperatorConfig.TRUSTSTORE_PASSWORD.key(), kafkaClientConfiguration.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG).toString()));

        // then
        assertEquals("some-namespace", config.namespace());

        var adminConfig = config.adminClientConfig();
        // client.id is random, so check it's there then remove for an easier assertion on the rest of the map
        assertTrue(!adminConfig.get("client.id").toString().isEmpty());
        adminConfig.remove("client.id");
        assertEquals(Map.of(
                "security.protocol", "SSL",
                "bootstrap.servers", kc.getBootstrapServers(),
                "ssl.truststore.location", kafkaClientConfiguration.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG).toString(),
                "ssl.truststore.password", kafkaClientConfiguration.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG).toString(),
                "ssl.endpoint.identification.algorithm", "HTTPS"), adminConfig);
        Admin.create(adminConfig).describeCluster().clusterId().get();
    }

    @Test
    void shouldConnectWithSaslPlain(
                    @SaslPlainAuth(user = "foo", password = "foo")
                    KafkaCluster kc) throws ExecutionException, InterruptedException {
        // given
        var config = TopicOperatorConfig.buildFromMap(Map.of(
                TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), kc.getBootstrapServers(),
                TopicOperatorConfig.NAMESPACE.key(), "some-namespace",
                TopicOperatorConfig.SECURITY_PROTOCOL.key(), "SASL_PLAINTEXT",
                TopicOperatorConfig.SASL_ENABLED.key(), "true",
                TopicOperatorConfig.SASL_MECHANISM.key(), "plain",
                TopicOperatorConfig.SASL_USERNAME.key(), "foo",
                TopicOperatorConfig.SASL_PASSWORD.key(), "foo"
        ));

        // then
        assertEquals("some-namespace", config.namespace());

        var adminConfig = config.adminClientConfig();
        // client.id is random, so check it's there then remove for an easier assertion on the rest of the map
        assertTrue(!adminConfig.get("client.id").toString().isEmpty());
        adminConfig.remove("client.id");
        assertEquals(Map.of(
                "security.protocol", "SASL_PLAINTEXT",
                "bootstrap.servers", kc.getBootstrapServers(),
                "sasl.mechanism", "PLAIN",
                "sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"foo\" password=\"foo\";"), adminConfig);

        Admin.create(adminConfig).describeCluster().clusterId().get();
    }

    @Test
    void shouldConnectWithSaslPlainWithTls(
                    @Tls
                    @SaslPlainAuth(user = "foo", password = "foo")
                    KafkaCluster kc) throws ExecutionException, InterruptedException {
        // given
        Map<String, Object> kafkaClientConfiguration = kc.getKafkaClientConfiguration("foo", "foo");
        var config = TopicOperatorConfig.buildFromMap(Map.of(
                TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), kc.getBootstrapServers(),
                TopicOperatorConfig.NAMESPACE.key(), "some-namespace",
                TopicOperatorConfig.SECURITY_PROTOCOL.key(), "SASL_SSL",
                TopicOperatorConfig.TLS_ENABLED.key(), "true",
                TopicOperatorConfig.TRUSTSTORE_LOCATION.key(), kafkaClientConfiguration.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG).toString(),
                TopicOperatorConfig.TRUSTSTORE_PASSWORD.key(), kafkaClientConfiguration.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG).toString(),
                TopicOperatorConfig.SASL_ENABLED.key(), "true",
                TopicOperatorConfig.SASL_MECHANISM.key(), "plain",
                TopicOperatorConfig.SASL_USERNAME.key(), "foo",
                TopicOperatorConfig.SASL_PASSWORD.key(), "foo"
        ));

        // then
        assertEquals("some-namespace", config.namespace());

        var adminConfig = config.adminClientConfig();
        // client.id is random, so check it's there then remove for an easier assertion on the rest of the map
        assertTrue(!adminConfig.get("client.id").toString().isEmpty());
        adminConfig.remove("client.id");
        assertEquals(Map.of(
                "security.protocol", "SASL_SSL",
                "bootstrap.servers", kc.getBootstrapServers(),
                "sasl.mechanism", "PLAIN",
                "sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"foo\" password=\"foo\";",
                "ssl.truststore.location", kafkaClientConfiguration.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG).toString(),
                "ssl.truststore.password", kafkaClientConfiguration.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG).toString(),
                "ssl.endpoint.identification.algorithm", "HTTPS"), adminConfig);

        Admin.create(adminConfig).describeCluster().clusterId().get();
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
    void shouldRejectInvalidSaslMechanism() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> saslScramSha(511));
        assertEquals("Invalid SASL_MECHANISM type: scram-sha-511", e.getMessage());
    }

    void saslScramSha(int bits) {
        // given
        var config = TopicOperatorConfig.buildFromMap(Map.of(
                TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234",
                TopicOperatorConfig.NAMESPACE.key(), "some-namespace",
                TopicOperatorConfig.SASL_ENABLED.key(), "true",
                TopicOperatorConfig.SASL_MECHANISM.key(), "scram-sha-"  + bits,
                TopicOperatorConfig.SASL_USERNAME.key(), "foo",
                TopicOperatorConfig.SASL_PASSWORD.key(), "pa55word"
                ));

        // then
        assertEquals("some-namespace", config.namespace());

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
                TopicOperatorConfig.NAMESPACE.key(), "some-namespace",
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
                TopicOperatorConfig.NAMESPACE.key(), "some-namespace",
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
                TopicOperatorConfig.NAMESPACE.key(), "some-namespace",
                TopicOperatorConfig.TLS_ENABLED.key(), "true",
                TopicOperatorConfig.TRUSTSTORE_PASSWORD.key(), "some_password"));

        // then
        assertEquals("some-namespace", config.namespace());

        var e = assertThrows(InvalidConfigurationException.class, () -> config.adminClientConfig());
        assertEquals("TLS_TRUSTSTORE_PASSWORD was supplied but TLS_TRUSTSTORE_LOCATION was not supplied", e.getMessage());
    }

    @Test
    void shouldThrowIfKeyStorePasswordButNoLocation() {
        // given
        var config = TopicOperatorConfig.buildFromMap(Map.of(
                TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234",
                TopicOperatorConfig.NAMESPACE.key(), "some-namespace",
                TopicOperatorConfig.TLS_ENABLED.key(), "true",
                TopicOperatorConfig.TRUSTSTORE_LOCATION.key(), "/path/to/truststore",
                TopicOperatorConfig.TRUSTSTORE_PASSWORD.key(), "password_from_truststore",
                TopicOperatorConfig.KEYSTORE_LOCATION.key(), "/path/to/keystore",
                TopicOperatorConfig.KEYSTORE_PASSWORD.key(), "password_for_keystore"));

        // then
        assertEquals("some-namespace", config.namespace());

        var adminConfig = config.adminClientConfig();
        assertEquals("/path/to/keystore", adminConfig.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
        assertEquals("password_for_keystore", adminConfig.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG));
    }

    @Test
    void shouldMaskSecuritySensitiveConfigsInToString() {
        var config = TopicOperatorConfig.buildFromMap(Map.ofEntries(
                Map.entry(TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234"),
                Map.entry(TopicOperatorConfig.NAMESPACE.key(), "some-namespace"),
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
              TopicOperatorConfig.NAMESPACE.key(), "some-namespace",
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
        assertEquals("some-namespace", config.namespace());

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
              TopicOperatorConfig.NAMESPACE.key(), "some-namespace",
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
              TopicOperatorConfig.NAMESPACE.key(), "some-namespace",
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
              TopicOperatorConfig.NAMESPACE.key(), "some-namespace",
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
              TopicOperatorConfig.NAMESPACE.key(), "some-namespace",
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
              TopicOperatorConfig.NAMESPACE.key(), "some-namespace"
        ));

        assertFalse(config.skipClusterConfigReview());
    }

    @Test
    void shouldDefaultToAllForAlterableTopicConfig() {
        var config = TopicOperatorConfig.buildFromMap(Map.of(
              TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234",
              TopicOperatorConfig.NAMESPACE.key(), "some-namespace"
        ));

        assertEquals("ALL", config.alterableTopicConfig());
    }

    @Test
    public void testDefaultFeatureGates()    {
        TopicOperatorConfig config = TopicOperatorConfig.buildFromMap(Map.of(TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234", TopicOperatorConfig.NAMESPACE.key(), "some-namespace"));
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
}