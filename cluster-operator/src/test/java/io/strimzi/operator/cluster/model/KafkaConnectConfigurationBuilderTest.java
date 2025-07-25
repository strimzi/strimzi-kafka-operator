/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.common.ClientTls;
import io.strimzi.api.kafka.model.common.ClientTlsBuilder;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationOAuth;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationOAuthBuilder;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationPlain;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationPlainBuilder;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationScramSha256;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationScramSha256Builder;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationScramSha512;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationScramSha512Builder;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationTls;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationTlsBuilder;
import io.strimzi.api.kafka.model.common.metrics.StrimziMetricsReporterBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnectSpecBuilder;
import io.strimzi.operator.cluster.model.metrics.MetricsModel;
import io.strimzi.operator.cluster.model.metrics.StrimziMetricsReporterConfig;
import io.strimzi.operator.cluster.model.metrics.StrimziMetricsReporterModel;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.strimzi.operator.cluster.TestUtils.IsEquivalent.isEquivalent;
import static org.hamcrest.MatcherAssert.assertThat;

@ParallelSuite
class KafkaConnectConfigurationBuilderTest {

    private static final String BOOTSTRAP_SERVERS = "my-cluster-kafka-bootstrap:9092";

    @ParallelTest
    public void testBuild()  {
        String configuration = new KafkaConnectConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BOOTSTRAP_SERVERS).build();
        assertThat(configuration, isEquivalent(
                "bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "security.protocol=PLAINTEXT",
                "producer.security.protocol=PLAINTEXT",
                "consumer.security.protocol=PLAINTEXT",
                "admin.security.protocol=PLAINTEXT"
        ));
    }

    @ParallelTest
    public void testWithTls() {
        ClientTls clientTls = new ClientTlsBuilder()
                .addNewTrustedCertificate()
                    .withSecretName("tls-trusted-certificate")
                    .withCertificate("pem-content")
                .endTrustedCertificate()
                .build();

        String configuration = new KafkaConnectConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BOOTSTRAP_SERVERS)
                .withTls(clientTls, "my-cluster")
                .build();

        assertThat(configuration, isEquivalent(
                "bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "security.protocol=SSL",
                "producer.security.protocol=SSL",
                "consumer.security.protocol=SSL",
                "admin.security.protocol=SSL",
                "ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-connect-tls-trusted-certs:*.crt}",
                "ssl.truststore.type=PEM",
                "producer.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-connect-tls-trusted-certs:*.crt}",
                "producer.ssl.truststore.type=PEM",
                "consumer.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-connect-tls-trusted-certs:*.crt}",
                "consumer.ssl.truststore.type=PEM",
                "admin.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-connect-tls-trusted-certs:*.crt}",
                "admin.ssl.truststore.type=PEM"
        ));
    }

    @ParallelTest
    public void testWithTlsAndClientAuthentication() {
        ClientTls clientTls = new ClientTlsBuilder()
                .addNewTrustedCertificate()
                    .withSecretName("tls-trusted-certificate")
                    .withCertificate("pem-content")
                .endTrustedCertificate()
                .build();

        KafkaClientAuthenticationTls tlsAuth = new KafkaClientAuthenticationTlsBuilder()
                .withNewCertificateAndKey()
                    .withSecretName("tls-keystore")
                    .withCertificate("pem-content")
                .endCertificateAndKey()
                .build();

        String configuration = new KafkaConnectConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BOOTSTRAP_SERVERS)
                .withTls(clientTls, "my-cluster")
                .withAuthentication(tlsAuth, "my-cluster")
                .build();

        assertThat(configuration, isEquivalent(
                "bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "security.protocol=SSL",
                "producer.security.protocol=SSL",
                "consumer.security.protocol=SSL",
                "admin.security.protocol=SSL",
                "ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-connect-tls-trusted-certs:*.crt}",
                "ssl.truststore.type=PEM",
                "producer.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-connect-tls-trusted-certs:*.crt}",
                "producer.ssl.truststore.type=PEM",
                "consumer.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-connect-tls-trusted-certs:*.crt}",
                "consumer.ssl.truststore.type=PEM",
                "admin.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-connect-tls-trusted-certs:*.crt}",
                "admin.ssl.truststore.type=PEM",
                "ssl.keystore.certificate.chain=${strimzisecrets:namespace/tls-keystore:pem-content}",
                "ssl.keystore.key=${strimzisecrets:namespace/tls-keystore:null}",
                "ssl.keystore.type=PEM",
                "producer.ssl.keystore.certificate.chain=${strimzisecrets:namespace/tls-keystore:pem-content}",
                "producer.ssl.keystore.key=${strimzisecrets:namespace/tls-keystore:null}",
                "producer.ssl.keystore.type=PEM",
                "consumer.ssl.keystore.certificate.chain=${strimzisecrets:namespace/tls-keystore:pem-content}",
                "consumer.ssl.keystore.key=${strimzisecrets:namespace/tls-keystore:null}",
                "consumer.ssl.keystore.type=PEM",
                "admin.ssl.keystore.certificate.chain=${strimzisecrets:namespace/tls-keystore:pem-content}",
                "admin.ssl.keystore.key=${strimzisecrets:namespace/tls-keystore:null}",
                "admin.ssl.keystore.type=PEM"
        ));
    }

    @ParallelTest
    public void testWithPlainAndSaslMechanism() {
        KafkaClientAuthenticationPlain authPlain = new KafkaClientAuthenticationPlainBuilder()
                .withUsername("user1")
                .withNewPasswordSecret()
                    .withSecretName("my-auth-secret")
                    .withPassword("my-password-key")
                .endPasswordSecret()
                .build();

        String configuration = new KafkaConnectConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BOOTSTRAP_SERVERS)
                .withAuthentication(authPlain, "my-cluster")
                .build();

        assertThat(configuration, isEquivalent(
                "bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "security.protocol=SASL_PLAINTEXT",
                "producer.security.protocol=SASL_PLAINTEXT",
                "consumer.security.protocol=SASL_PLAINTEXT",
                "admin.security.protocol=SASL_PLAINTEXT",
                "sasl.mechanism=PLAIN",
                "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user1\" password=\"${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key}\";",
                "producer.sasl.mechanism=PLAIN",
                "producer.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user1\" password=\"${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key}\";",
                "consumer.sasl.mechanism=PLAIN",
                "consumer.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user1\" password=\"${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key}\";",
                "admin.sasl.mechanism=PLAIN",
                "admin.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user1\" password=\"${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key}\";"
        ));
    }

    @ParallelTest
    public void testWithTlsAndSaslMechanism() {
        ClientTls clientTls = new ClientTlsBuilder()
                .addNewTrustedCertificate()
                    .withSecretName("tls-trusted-certificate")
                    .withCertificate("pem-content")
                .endTrustedCertificate()
                .build();

        KafkaClientAuthenticationPlain authPlain = new KafkaClientAuthenticationPlainBuilder()
                .withUsername("user1")
                .withNewPasswordSecret()
                    .withSecretName("my-auth-secret")
                    .withPassword("my-password-key")
                .endPasswordSecret()
                .build();

        String configuration = new KafkaConnectConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BOOTSTRAP_SERVERS)
                .withTls(clientTls, "my-cluster")
                .withAuthentication(authPlain, "my-cluster")
                .build();

        assertThat(configuration, isEquivalent(
                "bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "security.protocol=SASL_SSL",
                "producer.security.protocol=SASL_SSL",
                "consumer.security.protocol=SASL_SSL",
                "admin.security.protocol=SASL_SSL",
                "ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-connect-tls-trusted-certs:*.crt}",
                "ssl.truststore.type=PEM",
                "producer.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-connect-tls-trusted-certs:*.crt}",
                "producer.ssl.truststore.type=PEM",
                "consumer.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-connect-tls-trusted-certs:*.crt}",
                "consumer.ssl.truststore.type=PEM",
                "admin.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-connect-tls-trusted-certs:*.crt}",
                "admin.ssl.truststore.type=PEM",
                "sasl.mechanism=PLAIN",
                "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user1\" password=\"${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key}\";",
                "producer.sasl.mechanism=PLAIN",
                "producer.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user1\" password=\"${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key}\";",
                "consumer.sasl.mechanism=PLAIN",
                "consumer.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user1\" password=\"${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key}\";",
                "admin.sasl.mechanism=PLAIN",
                "admin.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user1\" password=\"${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key}\";"
        ));
    }

    @ParallelTest
    public void testWithPlainAndScramSha256() {
        KafkaClientAuthenticationScramSha256 authScramSha256 = new KafkaClientAuthenticationScramSha256Builder()
                .withUsername("my-user")
                .withNewPasswordSecret()
                    .withSecretName("my-auth-secret")
                    .withPassword("my-password-key")
                .endPasswordSecret()
                .build();

        String configuration = new KafkaConnectConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BOOTSTRAP_SERVERS)
                .withAuthentication(authScramSha256, "my-cluster")
                .build();

        assertThat(configuration, isEquivalent(
                "bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "security.protocol=SASL_PLAINTEXT",
                "producer.security.protocol=SASL_PLAINTEXT",
                "consumer.security.protocol=SASL_PLAINTEXT",
                "admin.security.protocol=SASL_PLAINTEXT",
                "sasl.mechanism=SCRAM-SHA-256",
                "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"my-user\" password=\"${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key}\";",
                "producer.sasl.mechanism=SCRAM-SHA-256",
                "producer.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"my-user\" password=\"${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key}\";",
                "consumer.sasl.mechanism=SCRAM-SHA-256",
                "consumer.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"my-user\" password=\"${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key}\";",
                "admin.sasl.mechanism=SCRAM-SHA-256",
                "admin.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"my-user\" password=\"${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key}\";"
        ));
    }

    @ParallelTest
    public void testWithTlsAndScramSha256() {
        ClientTls clientTls = new ClientTlsBuilder()
                .addNewTrustedCertificate()
                    .withSecretName("tls-trusted-certificate")
                    .withCertificate("pem-content")
                .endTrustedCertificate()
                .build();

        KafkaClientAuthenticationScramSha256 authScramSha256 = new KafkaClientAuthenticationScramSha256Builder()
                .withUsername("my-user")
                .withNewPasswordSecret()
                    .withSecretName("my-auth-secret")
                    .withPassword("my-password-key")
                .endPasswordSecret()
                .build();

        String configuration = new KafkaConnectConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BOOTSTRAP_SERVERS)
                .withTls(clientTls, "my-cluster")
                .withAuthentication(authScramSha256, "my-cluster")
                .build();

        assertThat(configuration, isEquivalent(
                "bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "security.protocol=SASL_SSL",
                "producer.security.protocol=SASL_SSL",
                "consumer.security.protocol=SASL_SSL",
                "admin.security.protocol=SASL_SSL",
                "ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-connect-tls-trusted-certs:*.crt}",
                "ssl.truststore.type=PEM",
                "producer.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-connect-tls-trusted-certs:*.crt}",
                "producer.ssl.truststore.type=PEM",
                "consumer.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-connect-tls-trusted-certs:*.crt}",
                "consumer.ssl.truststore.type=PEM",
                "admin.ssl.truststore.certificates=${strimzisecrets:namespace/my-cluster-connect-tls-trusted-certs:*.crt}",
                "admin.ssl.truststore.type=PEM",
                "sasl.mechanism=SCRAM-SHA-256",
                "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"my-user\" password=\"${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key}\";",
                "producer.sasl.mechanism=SCRAM-SHA-256",
                "producer.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"my-user\" password=\"${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key}\";",
                "consumer.sasl.mechanism=SCRAM-SHA-256",
                "consumer.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"my-user\" password=\"${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key}\";",
                "admin.sasl.mechanism=SCRAM-SHA-256",
                "admin.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"my-user\" password=\"${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key}\";"
        ));
    }

    @ParallelTest
    public void testWithPlainAndScramSha512() {
        KafkaClientAuthenticationScramSha512 authScramSha512 = new KafkaClientAuthenticationScramSha512Builder()
                .withUsername("my-user")
                .withNewPasswordSecret()
                    .withSecretName("my-auth-secret")
                    .withPassword("my-password-key")
                .endPasswordSecret()
                .build();

        String configuration = new KafkaConnectConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BOOTSTRAP_SERVERS)
                .withAuthentication(authScramSha512, "my-cluster")
                .build();

        assertThat(configuration, isEquivalent(
                "bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "security.protocol=SASL_PLAINTEXT",
                "producer.security.protocol=SASL_PLAINTEXT",
                "consumer.security.protocol=SASL_PLAINTEXT",
                "admin.security.protocol=SASL_PLAINTEXT",
                "sasl.mechanism=SCRAM-SHA-512",
                "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"my-user\" password=\"${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key}\";",
                "producer.sasl.mechanism=SCRAM-SHA-512",
                "producer.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"my-user\" password=\"${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key}\";",
                "consumer.sasl.mechanism=SCRAM-SHA-512",
                "consumer.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"my-user\" password=\"${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key}\";",
                "admin.sasl.mechanism=SCRAM-SHA-512",
                "admin.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"my-user\" password=\"${strimzidir:/opt/kafka/connect-password/my-auth-secret:my-password-key}\";"
        ));
    }

    @ParallelTest
    public void testWithAuthOauth() {
        KafkaClientAuthenticationOAuth authOAuth = new KafkaClientAuthenticationOAuthBuilder()
                .withClientId("oauth-client-id")
                .withTokenEndpointUri("http://token-endpoint-uri")
                .withUsername("oauth-username")
                .withNewClientSecret()
                    .withSecretName("my-client-secret-secret")
                    .withKey("my-client-secret-key")
                .endClientSecret()
                .withNewRefreshToken()
                    .withSecretName("my-refresh-token-secret")
                    .withKey("my-refresh-token-key")
                .endRefreshToken()
                .withNewAccessToken()
                    .withSecretName("my-refresh-token-secret")
                    .withKey("my-access-token-key")
                .endAccessToken()
                .withNewPasswordSecret()
                    .withSecretName("my-password-secret-secret")
                    .withPassword("my-password-key")
                .endPasswordSecret()
                .addNewTlsTrustedCertificate()
                    .withSecretName("my-tls-trusted-certificate")
                    .withCertificate("pem-content")
                .endTlsTrustedCertificate()
                .build();

        String configuration = new KafkaConnectConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BOOTSTRAP_SERVERS)
                .withAuthentication(authOAuth, "my-cluster")
                .build();

        String saslJaasConfig = "sasl.jaas.config=" +
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required" +
                " oauth.client.id=\"oauth-client-id\"" +
                " oauth.password.grant.username=\"oauth-username\"" +
                " oauth.token.endpoint.uri=\"http://token-endpoint-uri\"" +
                " oauth.client.secret=\"${strimzidir:/opt/kafka/oauth/my-client-secret-secret:my-client-secret-key}\"" +
                " oauth.refresh.token=\"${strimzidir:/opt/kafka/oauth/my-refresh-token-secret:my-refresh-token-key}\"" +
                " oauth.access.token=\"${strimzidir:/opt/kafka/oauth/my-refresh-token-secret:my-access-token-key}\"" +
                " oauth.password.grant.password=\"${strimzidir:/opt/kafka/oauth/my-password-secret-secret:my-password-key}\"" +
                " oauth.ssl.truststore.location=\"/opt/kafka/oauth-certs/my-cluster-connect-oauth-trusted-certs/my-cluster-connect-oauth-trusted-certs.crt\"" +
                " oauth.ssl.truststore.type=\"PEM\";";

        assertThat(configuration, isEquivalent(
                "bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "security.protocol=SASL_PLAINTEXT",
                "producer.security.protocol=SASL_PLAINTEXT",
                "consumer.security.protocol=SASL_PLAINTEXT",
                "admin.security.protocol=SASL_PLAINTEXT",
                "sasl.mechanism=OAUTHBEARER",
                saslJaasConfig,
                "sasl.login.callback.handler.class=io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler",
                "producer.sasl.mechanism=OAUTHBEARER",
                "producer." + saslJaasConfig,
                "producer.sasl.login.callback.handler.class=io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler",
                "consumer.sasl.mechanism=OAUTHBEARER",
                "consumer." + saslJaasConfig,
                "consumer.sasl.login.callback.handler.class=io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler",
                "admin.sasl.mechanism=OAUTHBEARER",
                "admin." + saslJaasConfig,
                "admin.sasl.login.callback.handler.class=io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler"
                ));
    }

    @ParallelTest
    public void testWithRackId() {
        String configuration = new KafkaConnectConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BOOTSTRAP_SERVERS)
                .withRackId()
                .build();

        assertThat(configuration, isEquivalent(
                "bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "security.protocol=PLAINTEXT",
                "producer.security.protocol=PLAINTEXT",
                "consumer.security.protocol=PLAINTEXT",
                "admin.security.protocol=PLAINTEXT",
                "consumer.client.rack=${strimzidir:/opt/kafka/init:rack.id}"
        ));

    }

    @ParallelTest
    public void testWithConfigProviders() {
        String configuration = new KafkaConnectConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BOOTSTRAP_SERVERS)
                .withUserConfiguration(null, false, false)
                .build();

        assertThat(configuration, isEquivalent(
                "bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "security.protocol=PLAINTEXT",
                "producer.security.protocol=PLAINTEXT",
                "consumer.security.protocol=PLAINTEXT",
                "admin.security.protocol=PLAINTEXT",
                "config.providers=strimzienv,strimzifile,strimzidir,strimzisecrets",
                "config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider",
                "config.providers.strimzienv.param.allowlist.pattern=.*",
                "config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider",
                "config.providers.strimzidir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider",
                "config.providers.strimzidir.param.allowed.paths=/opt/kafka",
                "config.providers.strimzisecrets.class=io.strimzi.kafka.KubernetesSecretConfigProvider",
                "group.id=connect-cluster",
                "offset.storage.topic=connect-cluster-offsets",
                "config.storage.topic=connect-cluster-configs",
                "status.storage.topic=connect-cluster-status",
                "key.converter=org.apache.kafka.connect.json.JsonConverter",
                "value.converter=org.apache.kafka.connect.json.JsonConverter")
        );
    }

    @ParallelTest
    public void testWithUserProvidedAndDefaultConfigurations() {
        Map<String, Object> userConfiguration = new HashMap<>();
        userConfiguration.put("myconfig", "abc");
        userConfiguration.put("myconfig2", 123);
        KafkaConnectConfiguration configurations = new KafkaConnectConfiguration(Reconciliation.DUMMY_RECONCILIATION, userConfiguration.entrySet());

        String configuration = new KafkaConnectConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BOOTSTRAP_SERVERS)
                .withUserConfiguration(configurations, false, false)
                .build();

        assertThat(configuration, isEquivalent(
                "bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "security.protocol=PLAINTEXT",
                "producer.security.protocol=PLAINTEXT",
                "consumer.security.protocol=PLAINTEXT",
                "admin.security.protocol=PLAINTEXT",
                "config.providers=strimzienv,strimzifile,strimzidir,strimzisecrets",
                "config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider",
                "config.providers.strimzienv.param.allowlist.pattern=.*",
                "config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider",
                "config.providers.strimzidir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider",
                "config.providers.strimzisecrets.class=io.strimzi.kafka.KubernetesSecretConfigProvider",
                "config.providers.strimzidir.param.allowed.paths=/opt/kafka",
                "myconfig=abc",
                "myconfig2=123",
                "group.id=connect-cluster",
                "offset.storage.topic=connect-cluster-offsets",
                "config.storage.topic=connect-cluster-configs",
                "status.storage.topic=connect-cluster-status",
                "key.converter=org.apache.kafka.connect.json.JsonConverter",
                "value.converter=org.apache.kafka.connect.json.JsonConverter")
        );
    }

    @ParallelTest
    public void testWithUserProvidedConfigMaps() {
        Map<String, Object> userConfiguration = new HashMap<>();
        userConfiguration.put("config.providers", "userenv");
        userConfiguration.put("config.providers.userenv.class", "org.apache.kafka.common.config.provider.EnvVarConfigProvider");
        KafkaConnectConfiguration configurations = new KafkaConnectConfiguration(Reconciliation.DUMMY_RECONCILIATION, userConfiguration.entrySet());

        String configuration = new KafkaConnectConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BOOTSTRAP_SERVERS)
                .withUserConfiguration(configurations, false, false)
                .build();

        assertThat(configuration, isEquivalent(
                "bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "security.protocol=PLAINTEXT",
                "producer.security.protocol=PLAINTEXT",
                "consumer.security.protocol=PLAINTEXT",
                "admin.security.protocol=PLAINTEXT",
                "config.providers=userenv,strimzienv,strimzifile,strimzidir,strimzisecrets",
                "config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider",
                "config.providers.strimzienv.param.allowlist.pattern=.*",
                "config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider",
                "config.providers.strimzidir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider",
                "config.providers.strimzidir.param.allowed.paths=/opt/kafka",
                "config.providers.userenv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider",
                "config.providers.strimzisecrets.class=io.strimzi.kafka.KubernetesSecretConfigProvider",
                "group.id=connect-cluster",
                "offset.storage.topic=connect-cluster-offsets",
                "config.storage.topic=connect-cluster-configs",
                "status.storage.topic=connect-cluster-status",
                "key.converter=org.apache.kafka.connect.json.JsonConverter",
                "value.converter=org.apache.kafka.connect.json.JsonConverter")
        );
    }

    @ParallelTest
    public void testWithRestListeners() {
        String configuration = new KafkaConnectConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BOOTSTRAP_SERVERS)
                .withRestListeners(8083)
                .build();

        assertThat(configuration, isEquivalent(
                "bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "security.protocol=PLAINTEXT",
                "producer.security.protocol=PLAINTEXT",
                "consumer.security.protocol=PLAINTEXT",
                "admin.security.protocol=PLAINTEXT",
                "rest.advertised.host.name=${strimzienv:ADVERTISED_HOSTNAME}",
                "rest.advertised.port=8083"
        ));
    }

    @ParallelTest
    public void withPluginPath() {
        String configuration = new KafkaConnectConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BOOTSTRAP_SERVERS)
                .withPluginPath().build();

        assertThat(configuration, isEquivalent(
                "bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "security.protocol=PLAINTEXT",
                "producer.security.protocol=PLAINTEXT",
                "consumer.security.protocol=PLAINTEXT",
                "admin.security.protocol=PLAINTEXT",
                "plugin.path=/opt/kafka/plugins"
        ));
    }

    @ParallelTest
    public void testStrimziMetricsReporterEnabled() {
        StrimziMetricsReporterModel model = new StrimziMetricsReporterModel(
                new KafkaConnectSpecBuilder()
                .withMetricsConfig(new StrimziMetricsReporterBuilder()
                    .withNewValues()
                        .withAllowList("kafka_connect_connector_metrics.*", "kafka_connect_connector_task_metrics.*")
                    .endValues()
                    .build())
                .build(), List.of(".*"));

        String configuration = new KafkaConnectConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BOOTSTRAP_SERVERS)
                .withStrimziMetricsReporter(model)
                .build();

        assertThat(configuration, isEquivalent(
                "admin.security.protocol=PLAINTEXT",
                "bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "security.protocol=PLAINTEXT",
                "producer.security.protocol=PLAINTEXT",
                "consumer.security.protocol=PLAINTEXT",
                StrimziMetricsReporterConfig.LISTENER_ENABLE + "=true",
                StrimziMetricsReporterConfig.LISTENER + "=http://:" + MetricsModel.METRICS_PORT,
                StrimziMetricsReporterConfig.ALLOW_LIST + "=kafka_connect_connector_metrics.*,kafka_connect_connector_task_metrics.*",
                "admin." + StrimziMetricsReporterConfig.LISTENER_ENABLE + "=true",
                "admin." + StrimziMetricsReporterConfig.LISTENER + "=http://:" + MetricsModel.METRICS_PORT,
                "admin." + StrimziMetricsReporterConfig.ALLOW_LIST + "=kafka_connect_connector_metrics.*,kafka_connect_connector_task_metrics.*",
                "producer." + StrimziMetricsReporterConfig.LISTENER_ENABLE + "=true",
                "producer." + StrimziMetricsReporterConfig.LISTENER + "=http://:" + MetricsModel.METRICS_PORT,
                "producer." + StrimziMetricsReporterConfig.ALLOW_LIST + "=kafka_connect_connector_metrics.*,kafka_connect_connector_task_metrics.*",
                "consumer." + StrimziMetricsReporterConfig.LISTENER_ENABLE + "=true",
                "consumer." + StrimziMetricsReporterConfig.LISTENER + "=http://:" + MetricsModel.METRICS_PORT,
                "consumer." + StrimziMetricsReporterConfig.ALLOW_LIST + "=kafka_connect_connector_metrics.*,kafka_connect_connector_task_metrics.*"
        ));
    }
    
    @ParallelTest
    public void testStrimziMetricsReporterDisabled() {
        String configuration = new KafkaConnectConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BOOTSTRAP_SERVERS)
                .withStrimziMetricsReporter(null)
                .build();

        assertThat(configuration, isEquivalent(
                "admin.security.protocol=PLAINTEXT",
                "bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "security.protocol=PLAINTEXT",
                "producer.security.protocol=PLAINTEXT",
                "consumer.security.protocol=PLAINTEXT",
                StrimziMetricsReporterConfig.LISTENER_ENABLE + "=false",
                "admin." + StrimziMetricsReporterConfig.LISTENER_ENABLE + "=false",
                "producer." + StrimziMetricsReporterConfig.LISTENER_ENABLE + "=false",
                "consumer." + StrimziMetricsReporterConfig.LISTENER_ENABLE + "=false"));
    }

    static Stream<Arguments> sourceUserConfigWithMetricsReporters() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("metric.reporters", "my.domain.CustomMetricReporter");

        KafkaConnectConfiguration userConfig = new KafkaConnectConfiguration(Reconciliation.DUMMY_RECONCILIATION, configMap.entrySet());
        String expectedConfig = "admin.security.protocol=PLAINTEXT\n"
                + "bootstrap.servers=my-cluster-kafka-bootstrap:9092\n"
                + "consumer.security.protocol=PLAINTEXT\n"
                + "config.providers.strimzidir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider\n"
                + "config.providers.strimzidir.param.allowed.paths=/opt/kafka\n"
                + "config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider\n"
                + "config.providers.strimzienv.param.allowlist.pattern=.*\n"
                + "config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider\n"
                + "config.providers=strimzienv,strimzifile,strimzidir,strimzisecrets\n"
                + "config.providers.strimzisecrets.class=io.strimzi.kafka.KubernetesSecretConfigProvider\n"
                + "config.storage.topic=connect-cluster-configs\n"
                + "key.converter=org.apache.kafka.connect.json.JsonConverter\n"
                + "offset.storage.topic=connect-cluster-offsets\n"
                + "producer.security.protocol=PLAINTEXT\n"
                + "security.protocol=PLAINTEXT\n"
                + "status.storage.topic=connect-cluster-status\n"
                + "group.id=connect-cluster\n"
                + "value.converter=org.apache.kafka.connect.json.JsonConverter\n";

        // testing 4 combinations of 2 boolean values
        return Stream.of(
                Arguments.of(userConfig, false, false,
                        expectedConfig
                                + "metric.reporters="
                                + "my.domain.CustomMetricReporter"
                ),
                Arguments.of(userConfig, true, false,
                        expectedConfig
                                + "metric.reporters="
                                + "my.domain.CustomMetricReporter,"
                                + "org.apache.kafka.common.metrics.JmxReporter\n"
                                + "admin.metric.reporters="
                                + "org.apache.kafka.common.metrics.JmxReporter\n"
                                + "producer.metric.reporters="
                                + "org.apache.kafka.common.metrics.JmxReporter\n"
                                + "consumer.metric.reporters="
                                + "org.apache.kafka.common.metrics.JmxReporter"
                ),
                Arguments.of(userConfig, false, true,
                        expectedConfig
                                + "metric.reporters="
                                + "my.domain.CustomMetricReporter,"
                                + StrimziMetricsReporterConfig.KAFKA_CLASS + "\n"
                                + "admin.metric.reporters="
                                + StrimziMetricsReporterConfig.KAFKA_CLASS + "\n"
                                + "producer.metric.reporters="
                                + StrimziMetricsReporterConfig.KAFKA_CLASS + "\n"
                                + "consumer.metric.reporters="
                                + StrimziMetricsReporterConfig.KAFKA_CLASS + "\n"
                ),

                Arguments.of(userConfig, true, true,
                        expectedConfig
                                + "metric.reporters="
                                + "my.domain.CustomMetricReporter,"
                                + "org.apache.kafka.common.metrics.JmxReporter,"
                                + StrimziMetricsReporterConfig.KAFKA_CLASS + "\n"
                                + "admin.metric.reporters="
                                + "org.apache.kafka.common.metrics.JmxReporter,"
                                + StrimziMetricsReporterConfig.KAFKA_CLASS + "\n"
                                + "producer.metric.reporters="
                                + "org.apache.kafka.common.metrics.JmxReporter,"
                                + StrimziMetricsReporterConfig.KAFKA_CLASS + "\n"
                                + "consumer.metric.reporters="
                                + "org.apache.kafka.common.metrics.JmxReporter,"
                                + StrimziMetricsReporterConfig.KAFKA_CLASS + "\n"
                ));
    }

    @ParameterizedTest
    @MethodSource("sourceUserConfigWithMetricsReporters")
    public void testUserConfigurationWithMetricReporters(
            KafkaConnectConfiguration userConfig,
            boolean injectJmx,
            boolean injectStrimzi,
            String expectedConfig) {
        String actualConfig = new KafkaConnectConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BOOTSTRAP_SERVERS)
                .withUserConfiguration(userConfig, injectJmx, injectStrimzi)
                .build();
        assertThat(actualConfig, isEquivalent(expectedConfig));
    }

    @ParallelTest
    public void testStrimziMetricsReporterViaUserAndMetricsConfigs() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("metric.reporters", StrimziMetricsReporterConfig.KAFKA_CLASS);
        configMap.put("kafka.metrics.reporters", StrimziMetricsReporterConfig.YAMMER_CLASS);
        KafkaConfiguration userConfig = new KafkaConfiguration(Reconciliation.DUMMY_RECONCILIATION, configMap.entrySet());
        
        StrimziMetricsReporterModel model = new StrimziMetricsReporterModel(
                new KafkaConnectSpecBuilder()
                .withMetricsConfig(new StrimziMetricsReporterBuilder()
                    .withNewValues()
                        .withAllowList("kafka_connect_connector_metrics.*", "kafka_connect_connector_task_metrics.*")
                    .endValues()
                    .build())
                .build(), List.of(".*"));

        String configuration = new KafkaConnectConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BOOTSTRAP_SERVERS)
                .withUserConfiguration(userConfig, false, true)
                .withStrimziMetricsReporter(model)
                .build();

        assertThat(configuration, isEquivalent("bootstrap.servers=my-cluster-kafka-bootstrap:9092\n"
                + "config.providers=strimzienv,strimzifile,strimzidir,strimzisecrets\n"
                + "config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider\n"
                + "config.providers.strimzienv.param.allowlist.pattern=.*\n"
                + "config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider\n"
                + "config.providers.strimzidir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider\n"
                + "config.providers.strimzidir.param.allowed.paths=/opt/kafka\n"
                + "config.providers.strimzisecrets.class=io.strimzi.kafka.KubernetesSecretConfigProvider\n"
                + "admin.metric.reporters=io.strimzi.kafka.metrics.KafkaPrometheusMetricsReporter\n"
                + "producer.metric.reporters=io.strimzi.kafka.metrics.KafkaPrometheusMetricsReporter\n"
                + "consumer.metric.reporters=io.strimzi.kafka.metrics.KafkaPrometheusMetricsReporter\n"
                + "offset.storage.topic=connect-cluster-offsets\n"
                + "value.converter=org.apache.kafka.connect.json.JsonConverter\n"
                + "config.storage.topic=connect-cluster-configs\n"
                + "key.converter=org.apache.kafka.connect.json.JsonConverter\n"
                + "group.id=connect-cluster\n"
                + "status.storage.topic=connect-cluster-status\n"
                + StrimziMetricsReporterConfig.LISTENER_ENABLE + "=true\n"
                + StrimziMetricsReporterConfig.LISTENER + "=http://:" + MetricsModel.METRICS_PORT + "\n"
                + StrimziMetricsReporterConfig.ALLOW_LIST + "=kafka_connect_connector_metrics.*,kafka_connect_connector_task_metrics.*\n"
                + "admin." + StrimziMetricsReporterConfig.LISTENER_ENABLE + "=true\n"
                + "admin." + StrimziMetricsReporterConfig.LISTENER + "=http://:" + MetricsModel.METRICS_PORT + "\n"
                + "admin." + StrimziMetricsReporterConfig.ALLOW_LIST + "=kafka_connect_connector_metrics.*,kafka_connect_connector_task_metrics.*\n"
                + "producer." + StrimziMetricsReporterConfig.LISTENER_ENABLE + "=true\n"
                + "producer." + StrimziMetricsReporterConfig.LISTENER + "=http://:" + MetricsModel.METRICS_PORT + "\n"
                + "producer." + StrimziMetricsReporterConfig.ALLOW_LIST + "=kafka_connect_connector_metrics.*,kafka_connect_connector_task_metrics.*\n"
                + "consumer." + StrimziMetricsReporterConfig.LISTENER_ENABLE + "=true\n"
                + "consumer." + StrimziMetricsReporterConfig.LISTENER + "=http://:" + MetricsModel.METRICS_PORT + "\n"
                + "consumer." + StrimziMetricsReporterConfig.ALLOW_LIST + "=kafka_connect_connector_metrics.*,kafka_connect_connector_task_metrics.*\n"
                + "security.protocol=PLAINTEXT\n"
                + "producer.security.protocol=PLAINTEXT\n"
                + "consumer.security.protocol=PLAINTEXT\n"
                + "admin.security.protocol=PLAINTEXT\n"
                + "metric.reporters=" + StrimziMetricsReporterConfig.KAFKA_CLASS));
    }
}