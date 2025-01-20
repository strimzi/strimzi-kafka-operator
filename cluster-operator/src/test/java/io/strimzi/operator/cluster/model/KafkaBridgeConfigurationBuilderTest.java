/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.bridge.KafkaBridgeAdminClientSpec;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeAdminClientSpecBuilder;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeConsumerSpec;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeConsumerSpecBuilder;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeHttpConfig;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeHttpConfigBuilder;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeProducerSpec;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeProducerSpecBuilder;
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
import io.strimzi.api.kafka.model.common.tracing.OpenTelemetryTracing;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.util.Map;

import static io.strimzi.operator.cluster.TestUtils.IsEquivalent.isEquivalent;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

@ParallelSuite
public class KafkaBridgeConfigurationBuilderTest {

    private static final String BRIDGE_CLUSTER = "my-bridge";
    private static final String BRIDGE_BOOTSTRAP_SERVERS = "my-cluster-kafka-bootstrap:9092";

    @ParallelTest
    public void testBaseConfiguration()  {
        // test base/default bridge configuration
        String configuration = new KafkaBridgeConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BRIDGE_CLUSTER, BRIDGE_BOOTSTRAP_SERVERS).build();
        assertThat(configuration, isEquivalent(
                "bridge.id=my-bridge",
                "kafka.bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "kafka.security.protocol=PLAINTEXT"
        ));
    }

    @ParallelTest
    public void testConfigProviders() {
        // test config providers setting
        String configuration = new KafkaBridgeConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BRIDGE_CLUSTER, BRIDGE_BOOTSTRAP_SERVERS)
                .withConfigProviders()
                .build();
        assertThat(configuration, isEquivalent(
                "bridge.id=my-bridge",
                "kafka.bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "kafka.security.protocol=PLAINTEXT",
                "kafka.config.providers=strimzienv,strimzifile,strimzidir",
                "kafka.config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider",
                "kafka.config.providers.strimzienv.param.allowlist.pattern=.*",
                "kafka.config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider",
                "kafka.config.providers.strimzifile.param.allowed.paths=/opt/strimzi",
                "kafka.config.providers.strimzidir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider",
                "kafka.config.providers.strimzidir.param.allowed.paths=/opt/strimzi"
        ));
    }

    @ParallelTest
    public void testTracing() {
        // test no tracing configured
        String configuration = new KafkaBridgeConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BRIDGE_CLUSTER, BRIDGE_BOOTSTRAP_SERVERS).build();
        assertThat(configuration, not(containsString("bridge.tracing")));

        // test opentelemetry tracing enabled
        configuration = new KafkaBridgeConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BRIDGE_CLUSTER, BRIDGE_BOOTSTRAP_SERVERS)
                .withTracing(new OpenTelemetryTracing())
                .build();
        assertThat(configuration, isEquivalent(
                "bridge.id=my-bridge",
                "kafka.bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "kafka.security.protocol=PLAINTEXT",
                "bridge.tracing=opentelemetry"
        ));
    }

    @ParallelTest
    public void testTls() {
        // test TLS configuration (only server authentication, encryption)
        ClientTls clientTls = new ClientTlsBuilder()
                .addNewTrustedCertificate()
                    .withSecretName("tls-trusted-certificate")
                    .withCertificate("pem-content")
                .endTrustedCertificate()
                .build();

        String configuration = new KafkaBridgeConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BRIDGE_CLUSTER, BRIDGE_BOOTSTRAP_SERVERS)
                .withTls(clientTls)
                .build();
        assertThat(configuration, isEquivalent(
                "bridge.id=my-bridge",
                "kafka.bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "kafka.security.protocol=SSL",
                "kafka.ssl.truststore.location=/tmp/strimzi/bridge.truststore.p12",
                "kafka.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "kafka.ssl.truststore.type=PKCS12"
        ));

        // test TLS with mutual authentication (mTLS, server and client authentication)
        KafkaClientAuthenticationTls tlsAuth = new KafkaClientAuthenticationTlsBuilder()
                .withNewCertificateAndKey()
                    .withSecretName("tls-keystore")
                    .withCertificate("pem-content")
                .endCertificateAndKey()
                .build();

        configuration = new KafkaBridgeConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BRIDGE_CLUSTER, BRIDGE_BOOTSTRAP_SERVERS)
                .withTls(clientTls)
                .withAuthentication(tlsAuth)
                .build();
        assertThat(configuration, isEquivalent(
                "bridge.id=my-bridge",
                "kafka.bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "kafka.security.protocol=SSL",
                "kafka.ssl.truststore.location=/tmp/strimzi/bridge.truststore.p12",
                "kafka.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "kafka.ssl.truststore.type=PKCS12",
                "kafka.ssl.keystore.location=/tmp/strimzi/bridge.keystore.p12",
                "kafka.ssl.keystore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "kafka.ssl.keystore.type=PKCS12"
        ));
    }

    @ParallelTest
    public void testSaslMechanism() {
        // test plain authentication
        KafkaClientAuthenticationPlain authPlain = new KafkaClientAuthenticationPlainBuilder()
                .withNewPasswordSecret()
                    .withSecretName("my-auth-secret")
                    .withPassword("my-password-key")
                .endPasswordSecret()
                .build();
        String configuration = new KafkaBridgeConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BRIDGE_CLUSTER, BRIDGE_BOOTSTRAP_SERVERS)
                .withAuthentication(authPlain)
                .build();
        assertThat(configuration, isEquivalent(
                "bridge.id=my-bridge",
                "kafka.bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "kafka.security.protocol=SASL_PLAINTEXT",
                "kafka.sasl.mechanism=PLAIN",
                "kafka.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=${strimzienv:KAFKA_BRIDGE_SASL_USERNAME} password=${strimzidir:/opt/strimzi/bridge-password/my-auth-secret:my-password-key};"
        ));

        // test plain authentication but with TLS as well (server authentication only, encryption)
        ClientTls clientTls = new ClientTlsBuilder()
                .addNewTrustedCertificate()
                    .withSecretName("tls-trusted-certificate")
                    .withCertificate("pem-content")
                .endTrustedCertificate()
                .build();
        configuration = new KafkaBridgeConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BRIDGE_CLUSTER, BRIDGE_BOOTSTRAP_SERVERS)
                .withTls(clientTls)
                .withAuthentication(authPlain)
                .build();
        assertThat(configuration, isEquivalent(
                "bridge.id=my-bridge",
                "kafka.bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "kafka.security.protocol=SASL_SSL",
                "kafka.sasl.mechanism=PLAIN",
                "kafka.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=${strimzienv:KAFKA_BRIDGE_SASL_USERNAME} password=${strimzidir:/opt/strimzi/bridge-password/my-auth-secret:my-password-key};",
                "kafka.ssl.truststore.location=/tmp/strimzi/bridge.truststore.p12",
                "kafka.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD}",
                "kafka.ssl.truststore.type=PKCS12"
                ));

        // test scram-sha-256 authentication
        KafkaClientAuthenticationScramSha256 authScramSha256 = new KafkaClientAuthenticationScramSha256Builder()
                .withNewPasswordSecret()
                    .withSecretName("my-auth-secret")
                    .withPassword("my-password-key")
                .endPasswordSecret()
                .build();
        configuration = new KafkaBridgeConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BRIDGE_CLUSTER, BRIDGE_BOOTSTRAP_SERVERS)
                .withAuthentication(authScramSha256)
                .build();
        assertThat(configuration, isEquivalent(
                "bridge.id=my-bridge",
                "kafka.bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "kafka.security.protocol=SASL_PLAINTEXT",
                "kafka.sasl.mechanism=SCRAM-SHA-256",
                "kafka.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=${strimzienv:KAFKA_BRIDGE_SASL_USERNAME} password=${strimzidir:/opt/strimzi/bridge-password/my-auth-secret:my-password-key};"
        ));

        // test scram-sha-512 authentication
        KafkaClientAuthenticationScramSha512 authScramSha512 = new KafkaClientAuthenticationScramSha512Builder()
                .withNewPasswordSecret()
                    .withSecretName("my-auth-secret")
                    .withPassword("my-password-key")
                .endPasswordSecret()
                .build();
        configuration = new KafkaBridgeConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BRIDGE_CLUSTER, BRIDGE_BOOTSTRAP_SERVERS)
                .withAuthentication(authScramSha512)
                .build();
        assertThat(configuration, isEquivalent(
                "bridge.id=my-bridge",
                "kafka.bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "kafka.security.protocol=SASL_PLAINTEXT",
                "kafka.sasl.mechanism=SCRAM-SHA-512",
                "kafka.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=${strimzienv:KAFKA_BRIDGE_SASL_USERNAME} password=${strimzidir:/opt/strimzi/bridge-password/my-auth-secret:my-password-key};"
        ));

        // test oauth authentication
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
                    .withSecretName("my-access-token-secret")
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
        configuration = new KafkaBridgeConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BRIDGE_CLUSTER, BRIDGE_BOOTSTRAP_SERVERS)
                .withAuthentication(authOAuth)
                .build();
        assertThat(configuration, isEquivalent(
                "bridge.id=my-bridge",
                "kafka.bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "kafka.security.protocol=SASL_PLAINTEXT",
                "kafka.sasl.mechanism=OAUTHBEARER",
                "kafka.sasl.jaas.config=" +
                        "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required ${strimzienv:KAFKA_BRIDGE_OAUTH_CONFIG}" +
                        " oauth.client.secret=${strimzienv:KAFKA_BRIDGE_OAUTH_CLIENT_SECRET}" +
                        " oauth.refresh.token=${strimzienv:KAFKA_BRIDGE_OAUTH_REFRESH_TOKEN}" +
                        " oauth.access.token=${strimzienv:KAFKA_BRIDGE_OAUTH_ACCESS_TOKEN}" +
                        " oauth.password.grant.password=${strimzienv:KAFKA_BRIDGE_OAUTH_PASSWORD_GRANT_PASSWORD}" +
                        " oauth.ssl.truststore.location=\"/tmp/strimzi/oauth.truststore.p12\" oauth.ssl.truststore.password=${strimzienv:CERTS_STORE_PASSWORD} oauth.ssl.truststore.type=\"PKCS12\";",
                "kafka.sasl.login.callback.handler.class=io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler"
        ));
    }

    @ParallelTest
    public void testKafkaProducer() {
        // test missing Kafka Producer configuration
        String configuration = new KafkaBridgeConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BRIDGE_CLUSTER, BRIDGE_BOOTSTRAP_SERVERS)
                .build();
        assertThat(configuration, not(containsString("kafka.producer.")));

        // test some Kafka Producer parameters
        KafkaBridgeProducerSpec kafkaBridgeProducer = new KafkaBridgeProducerSpecBuilder()
                .withConfig(
                        Map.of(
                                "acks", 1,
                                "linger.ms", 100,
                                "key.serializer", "my-producer-key-serializer",
                                "value.serializer", "my-producer-value-serializer"
                        ))
                .build();
        configuration = new KafkaBridgeConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BRIDGE_CLUSTER, BRIDGE_BOOTSTRAP_SERVERS)
                .withKafkaProducer(kafkaBridgeProducer)
                .build();
        assertThat(configuration, isEquivalent(
                "bridge.id=my-bridge",
                "kafka.bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "kafka.security.protocol=PLAINTEXT",
                "kafka.producer.acks=1",
                "kafka.producer.linger.ms=100",
                "kafka.producer.key.serializer=my-producer-key-serializer",
                "kafka.producer.value.serializer=my-producer-value-serializer"
        ));
    }

    @ParallelTest
    public void testKafkaConsumer() {
        // test missing Kafka Consumer configuration
        String configuration = new KafkaBridgeConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BRIDGE_CLUSTER, BRIDGE_BOOTSTRAP_SERVERS)
                .build();
        assertThat(configuration, not(containsString("kafka.consumer.")));

        // test some Kafka Consumer parameters
        KafkaBridgeConsumerSpec kafkaBridgeConsumer = new KafkaBridgeConsumerSpecBuilder()
                .withConfig(
                        Map.of(
                                "auto.offset.reset", "earliest",
                                "key.deserializer", "my-consumer-key-deserializer",
                                "value.deserializer", "my-consumer-value-deserializer"
                        ))
                .build();
        configuration = new KafkaBridgeConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BRIDGE_CLUSTER, BRIDGE_BOOTSTRAP_SERVERS)
                .withKafkaConsumer(kafkaBridgeConsumer)
                .build();
        assertThat(configuration, isEquivalent(
                "bridge.id=my-bridge",
                "kafka.bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "kafka.security.protocol=PLAINTEXT",
                "kafka.consumer.auto.offset.reset=earliest",
                "kafka.consumer.key.deserializer=my-consumer-key-deserializer",
                "kafka.consumer.value.deserializer=my-consumer-value-deserializer",
                "kafka.consumer.client.rack=${strimzidir:/opt/strimzi/init:rack.id}"
        ));
    }

    @ParallelTest
    public void testKafkaAdminClient() {
        // test missing Kafka Admin configuration
        String configuration = new KafkaBridgeConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BRIDGE_CLUSTER, BRIDGE_BOOTSTRAP_SERVERS)
                .build();
        assertThat(configuration, not(containsString("kafka.admin.")));

        // test some Kafka Admin parameters
        KafkaBridgeAdminClientSpec kafkaBridgeAdminClient = new KafkaBridgeAdminClientSpecBuilder()
                .withConfig(
                        Map.of(
                                "client.id", "my-admin-client",
                                "bootstrap.controllers", "my-bootstrap-controllers"
                        ))
                .build();
        configuration = new KafkaBridgeConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BRIDGE_CLUSTER, BRIDGE_BOOTSTRAP_SERVERS)
                .withKafkaAdminClient(kafkaBridgeAdminClient)
                .build();
        assertThat(configuration, isEquivalent(
                "bridge.id=my-bridge",
                "kafka.bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "kafka.security.protocol=PLAINTEXT",
                "kafka.admin.client.id=my-admin-client",
                "kafka.admin.bootstrap.controllers=my-bootstrap-controllers"
        ));
    }

    @ParallelTest
    public void testHttp() {
        // test default HTTP configuration.
        // NOTE: the "http" section is mandatory when using the KafkaBridge custom resource, so we define and set it
        KafkaBridgeHttpConfig http = new KafkaBridgeHttpConfigBuilder()
                .build();
        String configuration = new KafkaBridgeConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BRIDGE_CLUSTER, BRIDGE_BOOTSTRAP_SERVERS)
                .withHttp(http, null, null)
                .build();
        assertThat(configuration, isEquivalent(
                "bridge.id=my-bridge",
                "kafka.bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "kafka.security.protocol=PLAINTEXT",
                "http.host=0.0.0.0",
                "http.port=8080",
                "http.cors.enabled=false",
                "http.consumer.enabled=true",
                "http.timeoutSeconds=-1",
                "http.producer.enabled=true"
        ));

        // test different consumer timeout
        KafkaBridgeConsumerSpec kafkaBridgeConsumer = new KafkaBridgeConsumerSpecBuilder()
                .withTimeoutSeconds(10000)
                .build();
        configuration = new KafkaBridgeConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BRIDGE_CLUSTER, BRIDGE_BOOTSTRAP_SERVERS)
                .withHttp(http, null, kafkaBridgeConsumer)
                .build();
        assertThat(configuration, isEquivalent(
                "bridge.id=my-bridge",
                "kafka.bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "kafka.security.protocol=PLAINTEXT",
                "http.host=0.0.0.0",
                "http.port=8080",
                "http.cors.enabled=false",
                "http.consumer.enabled=true",
                "http.timeoutSeconds=10000",
                "http.producer.enabled=true"
        ));

        // test disabling HTTP part of the consumer and producer
        kafkaBridgeConsumer = new KafkaBridgeConsumerSpecBuilder()
                .withEnabled(false)
                .build();
        KafkaBridgeProducerSpec kafkaBridgeProducer = new KafkaBridgeProducerSpecBuilder()
                .withEnabled(false)
                .build();
        configuration = new KafkaBridgeConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BRIDGE_CLUSTER, BRIDGE_BOOTSTRAP_SERVERS)
                .withHttp(http, kafkaBridgeProducer, kafkaBridgeConsumer)
                .build();
        assertThat(configuration, isEquivalent(
                "bridge.id=my-bridge",
                "kafka.bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "kafka.security.protocol=PLAINTEXT",
                "http.host=0.0.0.0",
                "http.port=8080",
                "http.cors.enabled=false",
                "http.consumer.enabled=false",
                "http.timeoutSeconds=-1",
                "http.producer.enabled=false"
        ));

        // test different HTTP port
        http = new KafkaBridgeHttpConfigBuilder()
                .withPort(8081)
                .build();
        configuration = new KafkaBridgeConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BRIDGE_CLUSTER, BRIDGE_BOOTSTRAP_SERVERS)
                .withHttp(http, null, null)
                .build();
        assertThat(configuration, isEquivalent(
                "bridge.id=my-bridge",
                "kafka.bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "kafka.security.protocol=PLAINTEXT",
                "http.host=0.0.0.0",
                "http.port=8081",
                "http.cors.enabled=false",
                "http.consumer.enabled=true",
                "http.timeoutSeconds=-1",
                "http.producer.enabled=true"
        ));

        // test CORS configuration
        http = new KafkaBridgeHttpConfigBuilder()
                .withNewCors()
                    .withAllowedOrigins("https://strimzi.io", "https://cncf.io")
                    .withAllowedMethods("GET", "POST", "PUT", "DELETE", "PATCH")
                .endCors()
                .build();
        configuration = new KafkaBridgeConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, BRIDGE_CLUSTER, BRIDGE_BOOTSTRAP_SERVERS)
                .withHttp(http, null, null)
                .build();
        assertThat(configuration, isEquivalent(
                "bridge.id=my-bridge",
                "kafka.bootstrap.servers=my-cluster-kafka-bootstrap:9092",
                "kafka.security.protocol=PLAINTEXT",
                "http.host=0.0.0.0",
                "http.port=8080",
                "http.cors.enabled=true",
                "http.cors.allowedOrigins=https://strimzi.io,https://cncf.io",
                "http.cors.allowedMethods=GET,POST,PUT,DELETE,PATCH",
                "http.consumer.enabled=true",
                "http.timeoutSeconds=-1",
                "http.producer.enabled=true"
        ));
    }
}
