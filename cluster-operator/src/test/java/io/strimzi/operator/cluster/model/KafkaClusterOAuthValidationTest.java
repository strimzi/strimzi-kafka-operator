/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaAuthorizationKeycloakBuilder;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationOAuthBuilder;
import io.strimzi.api.kafka.model.storage.EphemeralStorage;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class KafkaClusterOAuthValidationTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withName("my-cluster")
                .withNamespace("my-namespace")
            .endMetadata()
            .withNewSpec()
                .withNewKafka()
                    .withReplicas(3)
                    .withStorage(new EphemeralStorage())
                .endKafka()
                .withNewZookeeper()
                    .withReplicas(3)
                .endZookeeper()
            .endSpec()
            .build();

    @Test
    public void testOAuthValidationWithIntrospectionMinimalPlain() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                .editKafka()
                .withNewListeners()
                .withNewPlain()
                .withAuth(new KafkaListenerAuthenticationOAuthBuilder()
                        .withClientId("my-client-id")
                        .withValidIssuerUri("http://valid-issuer")
                        .withIntrospectionEndpointUri("http://introspection")
                        .withNewClientSecret()
                        .withSecretName("my-secret-secret")
                        .withKey("my-secret-key")
                        .endClientSecret().build())
                .endPlain()
                .endListeners()
                .endKafka()
                .endSpec()
                .build();

        KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
    }

    @Test
    public void testOAuthValidationWithJwksAndKeycloakAuthzAndManyOptionsPlain() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                .editKafka()
                .withAuthorization(new KafkaAuthorizationKeycloakBuilder()
                        .withTokenEndpointUri("http://token-endpoint")
                        .withClientId("my-client-id")
                        .withDelegateToKafkaAcls(true)
                        .withGrantsRefreshPeriodSeconds(60)
                        .withGrantsRefreshPoolSize(5)
                        .withSuperUsers("CN=my-cluster-kafka,O=io.strimzi",
                                "CN=my-cluster-entity-operator,O=io.strimzi",
                                "CN=my-cluster-kafka-exporter,O=io.strimzi",
                                "CN=my-cluster-cruise-control,O=io.strimzi",
                                "CN=cluster-operator,O=io.strimzi",
                                "alice",
                                "CN=alice")
                        .build()
                )
                .withNewListeners()
                .withNewPlain()
                .withAuth(new KafkaListenerAuthenticationOAuthBuilder()
                        .withClientId("my-client-id")
                        .withValidIssuerUri("http://valid-issuer")
                        .withJwksEndpointUri("http://jwks-endpoint")
                        .withJwksRefreshSeconds(30)
                        .withJwksExpirySeconds(90)
                        .withJwksMinRefreshPauseSeconds(5)
                        .withMaxSecondsWithoutReauthentication(1800)
                        .withNewClientSecret()
                        .withSecretName("my-secret-secret")
                        .withKey("my-secret-key")
                        .endClientSecret().build())
                .endPlain()
                .endListeners()
                .endKafka()
                .endSpec()
                .build();

        KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
    }

    @Test
    public void testOAuthValidationWithJwksMinRefreshPauseAndIntrospectionPlain() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                    .editKafka()
                    .withNewListeners()
                    .withNewPlain()
                    .withAuth(new KafkaListenerAuthenticationOAuthBuilder()
                            .withClientId("my-client-id")
                            .withValidIssuerUri("http://valid-issuer")
                            .withIntrospectionEndpointUri("http://introspection")
                            .withJwksMinRefreshPauseSeconds(5)
                            .withNewClientSecret()
                            .withSecretName("my-secret-secret")
                            .withKey("my-secret-key")
                            .endClientSecret().build())
                    .endPlain()
                    .endListeners()
                    .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationWithJwksExpiryAndIntrospectionPlain() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                    .editKafka()
                    .withNewListeners()
                    .withNewPlain()
                    .withAuth(new KafkaListenerAuthenticationOAuthBuilder()
                            .withClientId("my-client-id")
                            .withValidIssuerUri("http://valid-issuer")
                            .withIntrospectionEndpointUri("http://introspection")
                            .withJwksExpirySeconds(120)
                            .withNewClientSecret()
                            .withSecretName("my-secret-secret")
                            .withKey("my-secret-key")
                            .endClientSecret().build())
                    .endPlain()
                    .endListeners()
                    .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationWithJwksRefreshAndIntrospectionPlain() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                    .editKafka()
                    .withNewListeners()
                    .withNewPlain()
                    .withAuth(new KafkaListenerAuthenticationOAuthBuilder()
                            .withClientId("my-client-id")
                            .withValidIssuerUri("http://valid-issuer")
                            .withIntrospectionEndpointUri("http://introspection")
                            .withJwksRefreshSeconds(60)
                            .withNewClientSecret()
                            .withSecretName("my-secret-secret")
                            .withKey("my-secret-key")
                            .endClientSecret().build())
                    .endPlain()
                    .endListeners()
                    .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationWithReauthAndIntrospectionPlain() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                .editKafka()
                .withNewListeners()
                .withNewPlain()
                .withAuth(new KafkaListenerAuthenticationOAuthBuilder()
                        .withClientId("my-client-id")
                        .withValidIssuerUri("http://valid-issuer")
                        .withIntrospectionEndpointUri("http://introspection")
                        .withMaxSecondsWithoutReauthentication(1800)
                        .withNewClientSecret()
                        .withSecretName("my-secret-secret")
                        .withKey("my-secret-key")
                        .endClientSecret().build())
                .endPlain()
                .endListeners()
                .endKafka()
                .endSpec()
                .build();

        KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
    }

    @Test
    public void testOAuthValidationMissingValidIssuerUriPlain() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                    .editKafka()
                    .withNewListeners()
                    .withNewPlain()
                    .withAuth(new KafkaListenerAuthenticationOAuthBuilder()
                            .withClientId("my-client-id")
                            .withIntrospectionEndpointUri("http://introspection")
                            .withNewClientSecret()
                            .withSecretName("my-secret-secret")
                            .withKey("my-secret-key")
                            .endClientSecret().build())
                    .endPlain()
                    .endListeners()
                    .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationMissingValidIssuerUriTls() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                    .editKafka()
                    .withNewListeners()
                    .withNewTls()
                    .withAuth(new KafkaListenerAuthenticationOAuthBuilder()
                            .withClientId("my-client-id")
                            .withIntrospectionEndpointUri("http://introspection")
                            .withNewClientSecret()
                            .withSecretName("my-secret-secret")
                            .withKey("my-secret-key")
                            .endClientSecret().build())
                    .endTls()
                    .endListeners()
                    .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationMissingValidIssuerUriExternal() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                    .editKafka()
                    .withNewListeners()
                    .withNewKafkaListenerExternalIngress()
                    .withAuth(new KafkaListenerAuthenticationOAuthBuilder()
                            .withClientId("my-client-id")
                            .withIntrospectionEndpointUri("http://introspection")
                            .withNewClientSecret()
                            .withSecretName("my-secret-secret")
                            .withKey("my-secret-key")
                            .endClientSecret().build())
                    .endKafkaListenerExternalIngress()
                    .endListeners()
                    .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationRefreshSecondsRelationWithExpirySecondsPlain() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                    .editKafka()
                    .withNewListeners()
                    .withNewPlain()
                    .withAuth(new KafkaListenerAuthenticationOAuthBuilder()
                            .withValidIssuerUri("http://valid-issuer")
                            .withJwksEndpointUri("http://jwks-endpoint")
                            .withJwksRefreshSeconds(30)
                            .withJwksExpirySeconds(89)
                            .build())
                    .endPlain()
                    .endListeners()
                    .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationRefreshSecondsRelationWithExpirySecondsTls() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                    .editKafka()
                    .withNewListeners()
                    .withNewTls()
                    .withAuth(new KafkaListenerAuthenticationOAuthBuilder()
                            .withValidIssuerUri("http://valid-issuer")
                            .withJwksEndpointUri("http://jwks-endpoint")
                            .withJwksRefreshSeconds(30)
                            .withJwksExpirySeconds(89)
                            .build())
                    .endTls()
                    .endListeners()
                    .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationRefreshSecondsRelationWithExpirySecondsExternal() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                    .editKafka()
                    .withNewListeners()
                    .withNewKafkaListenerExternalLoadBalancer()
                    .withAuth(new KafkaListenerAuthenticationOAuthBuilder()
                            .withValidIssuerUri("http://valid-issuer")
                            .withJwksEndpointUri("http://jwks-endpoint")
                            .withJwksRefreshSeconds(30)
                            .withJwksExpirySeconds(89)
                            .build())
                    .endKafkaListenerExternalLoadBalancer()
                    .endListeners()
                    .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationRefreshSecondsSetWithExpirySecondsNotSetPlain() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                    .editKafka()
                    .withNewListeners()
                    .withNewPlain()
                    .withAuth(new KafkaListenerAuthenticationOAuthBuilder()
                            .withValidIssuerUri("http://valid-issuer")
                            .withJwksEndpointUri("http://jwks-endpoint")
                            .withJwksRefreshSeconds(333).build())
                    .endPlain()
                    .endListeners()
                    .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationRefreshSecondsSetWithExpirySecondsNotSetTls() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                    .editKafka()
                    .withNewListeners()
                    .withNewTls()
                    .withAuth(new KafkaListenerAuthenticationOAuthBuilder()
                            .withValidIssuerUri("http://valid-issuer")
                            .withJwksEndpointUri("http://jwks-endpoint")
                            .withJwksRefreshSeconds(333).build())
                    .endTls()
                    .endListeners()
                    .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationRefreshSecondsSetWithExpirySecondsNotSetExternal() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                    .editKafka()
                    .withNewListeners()
                    .withNewKafkaListenerExternalNodePort()
                    .withAuth(new KafkaListenerAuthenticationOAuthBuilder()
                            .withValidIssuerUri("http://valid-issuer")
                            .withJwksEndpointUri("http://jwks-endpoint")
                            .withJwksRefreshSeconds(333)
                            .build())
                    .endKafkaListenerExternalNodePort()
                    .endListeners()
                    .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationRefreshSecondsNotSetWithExpirySecondsSetPlain() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                    .editKafka()
                    .withNewListeners()
                    .withNewPlain()
                    .withAuth(
                            new KafkaListenerAuthenticationOAuthBuilder()
                                    .withValidIssuerUri("http://valid-issuer")
                                    .withJwksEndpointUri("http://jwks-endpoint")
                                    .withJwksExpirySeconds(150)
                                    .build())
                    .endPlain()
                    .endListeners()
                    .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationRefreshSecondsNotSetWithExpirySecondsSetTls() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                    .editKafka()
                    .withNewListeners()
                    .withNewTls()
                    .withAuth(
                            new KafkaListenerAuthenticationOAuthBuilder()
                                    .withValidIssuerUri("http://valid-issuer")
                                    .withJwksEndpointUri("http://jwks-endpoint")
                                    .withJwksExpirySeconds(150)
                                    .build())
                    .endTls()
                    .endListeners()
                    .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationRefreshSecondsNotSetWithExpirySecondsSetExternal() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                    .editKafka()
                    .withNewListeners()
                    .withNewKafkaListenerExternalRoute()
                    .withAuth(
                            new KafkaListenerAuthenticationOAuthBuilder()
                                    .withValidIssuerUri("http://valid-issuer")
                                    .withJwksEndpointUri("http://jwks-endpoint")
                                    .withJwksExpirySeconds(150)
                                    .build())
                    .endKafkaListenerExternalRoute()
                    .endListeners()
                    .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationNoUriSpecifiedPlain() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                    .editKafka()
                    .withNewListeners()
                    .withNewPlain()
                    .withAuth(
                            new KafkaListenerAuthenticationOAuthBuilder()
                                    .build())
                    .endPlain()
                    .endListeners()
                    .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationNoUriSpecifiedTls() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                    .editKafka()
                    .withNewListeners()
                    .withNewTls()
                    .withAuth(
                            new KafkaListenerAuthenticationOAuthBuilder()
                                    .build())
                    .endTls()
                    .endListeners()
                    .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationNoUriSpecifiedExternal() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                    .editKafka()
                    .withNewListeners()
                    .withNewKafkaListenerExternalIngress()
                    .withAuth(
                            new KafkaListenerAuthenticationOAuthBuilder()
                                    .build())
                    .endKafkaListenerExternalIngress()
                    .endListeners()
                    .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationIntrospectionEndpointUriWithoutClientIdPlain() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                    .editKafka()
                    .withNewListeners()
                    .withNewPlain()
                    .withAuth(
                            new KafkaListenerAuthenticationOAuthBuilder()
                                    .withIntrospectionEndpointUri("http://introspection")
                                    .withNewClientSecret()
                                    .withSecretName("my-secret-secret")
                                    .withKey("my-secret-key")
                                    .endClientSecret()
                                    .build())
                    .endPlain()
                    .endListeners()
                    .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationIntrospectionEndpointUriWithoutClientIdTls() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                    .editKafka()
                    .withNewListeners()
                    .withNewTls()
                    .withAuth(
                            new KafkaListenerAuthenticationOAuthBuilder()
                                    .withIntrospectionEndpointUri("http://introspection")
                                    .withNewClientSecret()
                                    .withSecretName("my-secret-secret")
                                    .withKey("my-secret-key")
                                    .endClientSecret()
                                    .build())
                    .endTls()
                    .endListeners()
                    .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationIntrospectionEndpointUriWithoutClientIdExternal() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                    .editKafka()
                    .withNewListeners()
                    .withNewKafkaListenerExternalIngress()
                    .withAuth(
                            new KafkaListenerAuthenticationOAuthBuilder()
                                    .withIntrospectionEndpointUri("http://introspection")
                                    .withNewClientSecret()
                                    .withSecretName("my-secret-secret")
                                    .withKey("my-secret-key")
                                    .endClientSecret()
                                    .build())
                    .endKafkaListenerExternalIngress()
                    .endListeners()
                    .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationIntrospectionEndpointUriWithoutClientSecretPlain() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                    .editKafka()
                    .withNewListeners()
                    .withNewPlain()
                    .withAuth(
                            new KafkaListenerAuthenticationOAuthBuilder()
                                    .withClientId("my-client-id")
                                    .withIntrospectionEndpointUri("http://introspection")
                                    .build())
                    .endPlain()
                    .endListeners()
                    .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationIntrospectionEndpointUriWithoutClientSecretTls() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                    .editKafka()
                    .withNewListeners()
                    .withNewTls()
                    .withAuth(
                            new KafkaListenerAuthenticationOAuthBuilder()
                                    .withClientId("my-client-id")
                                    .withIntrospectionEndpointUri("http://introspection")
                                    .build())
                    .endTls()
                    .endListeners()
                    .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationIntrospectionEndpointUriWithoutClientSecretExternal() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                    .editKafka()
                    .withNewListeners()
                    .withNewKafkaListenerExternalIngress()
                    .withAuth(
                            new KafkaListenerAuthenticationOAuthBuilder()
                                    .withClientId("my-client-id")
                                    .withIntrospectionEndpointUri("http://introspection")
                                    .build())
                    .endKafkaListenerExternalIngress()
                    .endListeners()
                    .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationExpirySecondsWithoutEndpointUriPlain() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                    .editKafka()
                    .withNewListeners()
                    .withNewPlain()
                    .withAuth(
                            new KafkaListenerAuthenticationOAuthBuilder()
                                    .withIntrospectionEndpointUri("http://introspection")
                                    .withClientId("my-client-id")
                                    .withJwksExpirySeconds(100)
                                    .build())
                    .endPlain()
                    .endListeners()
                    .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationExpirySecondsWithoutEndpointUriTls() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                    .editKafka()
                    .withNewListeners()
                    .withNewTls()
                    .withAuth(
                            new KafkaListenerAuthenticationOAuthBuilder()
                                    .withIntrospectionEndpointUri("http://introspection")
                                    .withClientId("my-client-id")
                                    .withJwksExpirySeconds(100)
                                    .build())
                    .endTls()
                    .endListeners()
                    .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationExpirySecondsWithoutEndpointUriExternal() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                    .editKafka()
                    .withNewListeners()
                    .withNewKafkaListenerExternalIngress()
                    .withAuth(
                            new KafkaListenerAuthenticationOAuthBuilder()
                                    .withIntrospectionEndpointUri("http://introspection")
                                    .withClientId("my-client-id")
                                    .withJwksExpirySeconds(100)
                                    .build())
                    .endKafkaListenerExternalIngress()
                    .endListeners()
                    .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationRefreshSecondsWithoutEndpointUriPlain() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                    .editKafka()
                    .withNewListeners()
                    .withNewPlain()
                    .withAuth(
                            new KafkaListenerAuthenticationOAuthBuilder()
                                    .withIntrospectionEndpointUri("http://introspection")
                                    .withClientId("my-client-id")
                                    .withJwksRefreshSeconds(40)
                                    .build())
                    .endPlain()
                    .endListeners()
                    .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationRefreshSecondsWithoutEndpointUriTls() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                    .editKafka()
                    .withNewListeners()
                    .withNewTls()
                    .withAuth(
                            new KafkaListenerAuthenticationOAuthBuilder()
                                    .withIntrospectionEndpointUri("http://introspection")
                                    .withClientId("my-client-id")
                                    .withJwksRefreshSeconds(40)
                                    .build())
                    .endTls()
                    .endListeners()
                    .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationRefreshSecondsWithoutEndpointUriExternal() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                    .editKafka()
                    .withNewListeners()
                    .withNewKafkaListenerExternalIngress()
                    .withAuth(
                            new KafkaListenerAuthenticationOAuthBuilder()
                                    .withIntrospectionEndpointUri("http://introspection")
                                    .withClientId("my-client-id")
                                    .withJwksRefreshSeconds(40)
                                    .build())
                    .endKafkaListenerExternalIngress()
                    .endListeners()
                    .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationWithOAuthWithIntrospectionWithNoTypeCheck() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                    .editKafka()
                    .withNewListeners()
                    .withNewPlain()
                    .withAuth(
                            new KafkaListenerAuthenticationOAuthBuilder()
                                    .withValidIssuerUri("http://valid-issuer")
                                    .withClientId("my-kafka-id")
                                    .withNewClientSecret()
                                    .withSecretName("my-secret-secret")
                                    .withKey("my-secret-key")
                                    .endClientSecret()
                                    .withIntrospectionEndpointUri("http://introspection-endpoint")
                                    .withCheckAccessTokenType(false)
                                    .build())
                    .endPlain()
                    .endListeners()
                    .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }

    @Test
    public void testOAuthValidationWithOAuthWithJwksWithNotJwt() {
        assertThrows(InvalidResourceException.class, () -> {
            Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                    .editSpec()
                    .editKafka()
                    .withNewListeners()
                    .withNewPlain()
                    .withAuth(
                            new KafkaListenerAuthenticationOAuthBuilder()
                                    .withValidIssuerUri("http://valid-issuer")
                                    .withJwksEndpointUri("http://jwks-endpoint")
                                    .withJwksExpirySeconds(160)
                                    .withJwksRefreshSeconds(50)
                                    .withUserNameClaim("preferred_username")
                                    .withAccessTokenIsJwt(false)
                                    .build())
                    .endPlain()
                    .endListeners()
                    .endKafka()
                    .endSpec()
                    .build();

            KafkaCluster.fromCrd(kafkaAssembly, VERSIONS);
        });
    }
}
