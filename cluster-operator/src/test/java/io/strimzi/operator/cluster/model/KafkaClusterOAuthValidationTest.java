/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.kafka.EphemeralStorage;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaAuthorizationKeycloakBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationOAuth;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationOAuthBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationScramSha512Builder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.model.nodepools.NodePoolUtils;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertThrows;

@ParallelSuite
public class KafkaClusterOAuthValidationTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();
    private final static Set<NodeRef> THREE_NODES = Set.of(
            new NodeRef("name-kafka-0", 0, "kafka", false, true),
            new NodeRef("name-kafka-1", 1, "kafka", false, true),
            new NodeRef("name-kafka-2", 2, "kafka", false, true));

    private List<GenericKafkaListener> getListeners(KafkaListenerAuthenticationOAuth auth)   {
        GenericKafkaListener listener1 = new GenericKafkaListenerBuilder()
                .withName("listener1")
                .withPort(9900)
                .withType(KafkaListenerType.INTERNAL)
                .withAuth(auth)
                .build();

        return List.of(listener1);
    }

    @ParallelTest
    public void testOAuthValidationWithIntrospectionMinimalPlain() {
        KafkaListenerAuthenticationOAuth auth = new KafkaListenerAuthenticationOAuthBuilder()
                .withClientId("my-client-id")
                .withValidIssuerUri("http://valid-issuer")
                .withIntrospectionEndpointUri("http://introspection")
                .withNewClientSecret()
                .withSecretName("my-secret-secret")
                .withKey("my-secret-key")
                .endClientSecret()
                .build();

        ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, getListeners(auth));
    }

    @ParallelTest
    public void testOAuthAuthnAuthz() {
        List<GenericKafkaListener> listeners = List.of(new GenericKafkaListenerBuilder()
                .withName("listener1")
                .withPort(9900)
                .withType(KafkaListenerType.INTERNAL)
                .withAuth(new KafkaListenerAuthenticationOAuthBuilder()
                        .withClientId("my-client-id")
                        .withValidIssuerUri("http://valid-issuer")
                        .withJwksEndpointUri("http://jwks-endpoint")
                        .withJwksRefreshSeconds(30)
                        .withJwksExpirySeconds(90)
                        .withJwksMinRefreshPauseSeconds(5)
                        .withConnectTimeoutSeconds(20)
                        .withReadTimeoutSeconds(20)
                        .withGroupsClaim("$.groups")
                        .withMaxSecondsWithoutReauthentication(1800)
                        .withNewClientSecret()
                            .withSecretName("my-secret-secret")
                            .withKey("my-secret-key")
                        .endClientSecret()
                        .build())
                .build());

        Kafka kafkaAssembly = new KafkaBuilder()
                .withNewMetadata()
                    .withName("my-cluster")
                    .withNamespace("my-namespace")
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withStorage(new EphemeralStorage())
                        .withListeners(listeners)
                        .withAuthorization(new KafkaAuthorizationKeycloakBuilder()
                                .withTokenEndpointUri("http://token-endpoint")
                                .withClientId("my-client-id")
                                .withDelegateToKafkaAcls(true)
                                .withGrantsRefreshPeriodSeconds(60)
                                .withGrantsRefreshPoolSize(5)
                                .withSuperUsers("alice",
                                        "CN=alice")
                                .build())
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(3)
                        .withStorage(new EphemeralStorage())
                    .endZookeeper()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), false, SHARED_ENV_PROVIDER);
        KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
    }

    @ParallelTest
    public void testOAuthAuthzWithoutAuthn() {
        assertThrows(InvalidResourceException.class, () -> {
            List<GenericKafkaListener> listeners = List.of(new GenericKafkaListenerBuilder()
                    .withName("listener1")
                    .withPort(9900)
                    .withType(KafkaListenerType.INTERNAL)
                    .withAuth(new KafkaListenerAuthenticationScramSha512Builder()
                            .build())
                    .build());

            Kafka kafkaAssembly = new KafkaBuilder()
                    .withNewMetadata()
                        .withName("my-cluster")
                        .withNamespace("my-namespace")
                    .endMetadata()
                    .withNewSpec()
                        .withNewKafka()
                            .withReplicas(3)
                            .withStorage(new EphemeralStorage())
                            .withListeners(listeners)
                            .withAuthorization(new KafkaAuthorizationKeycloakBuilder()
                                    .withTokenEndpointUri("http://token-endpoint")
                                    .withClientId("my-client-id")
                                    .withDelegateToKafkaAcls(true)
                                    .withGrantsRefreshPeriodSeconds(60)
                                    .withGrantsRefreshPoolSize(5)
                                    .withSuperUsers("alice",
                                            "CN=alice")
                                    .build())
                        .endKafka()
                        .withNewZookeeper()
                            .withReplicas(3)
                            .withStorage(new EphemeralStorage())
                        .endZookeeper()
                    .endSpec()
                    .build();

            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, null, Map.of(), Map.of(), false, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        });
    }

    @ParallelTest
    public void testOAuthValidationWithJwksMinRefreshPauseAndIntrospection() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaListenerAuthenticationOAuth auth = new KafkaListenerAuthenticationOAuthBuilder()
                    .withClientId("my-client-id")
                    .withValidIssuerUri("http://valid-issuer")
                    .withIntrospectionEndpointUri("http://introspection")
                    .withJwksMinRefreshPauseSeconds(5)
                    .withNewClientSecret()
                    .withSecretName("my-secret-secret")
                    .withKey("my-secret-key")
                    .endClientSecret()
                    .build();

            ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, getListeners(auth));
        });
    }

    @ParallelTest
    public void testOAuthValidationWithJwksExpiryAndIntrospection() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaListenerAuthenticationOAuth auth = new KafkaListenerAuthenticationOAuthBuilder()
                    .withClientId("my-client-id")
                    .withValidIssuerUri("http://valid-issuer")
                    .withIntrospectionEndpointUri("http://introspection")
                    .withJwksExpirySeconds(120)
                    .withNewClientSecret()
                    .withSecretName("my-secret-secret")
                    .withKey("my-secret-key")
                    .endClientSecret()
                    .build();

            ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, getListeners(auth));
        });
    }

    @ParallelTest
    public void testOAuthValidationWithJwksRefreshAndIntrospection() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaListenerAuthenticationOAuth auth = new KafkaListenerAuthenticationOAuthBuilder()
                    .withClientId("my-client-id")
                    .withValidIssuerUri("http://valid-issuer")
                    .withIntrospectionEndpointUri("http://introspection")
                    .withJwksRefreshSeconds(60)
                    .withNewClientSecret()
                    .withSecretName("my-secret-secret")
                    .withKey("my-secret-key")
                    .endClientSecret()
                    .build();

            ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, getListeners(auth));
        });
    }

    @ParallelTest
    public void testOAuthValidationWithReauthAndIntrospection() {
        KafkaListenerAuthenticationOAuth auth = new KafkaListenerAuthenticationOAuthBuilder()
                .withClientId("my-client-id")
                .withValidIssuerUri("http://valid-issuer")
                .withIntrospectionEndpointUri("http://introspection")
                .withMaxSecondsWithoutReauthentication(1800)
                .withNewClientSecret()
                .withSecretName("my-secret-secret")
                .withKey("my-secret-key")
                .endClientSecret()
                .build();

        ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, getListeners(auth));
    }

    @ParallelTest
    public void testOAuthValidationMissingValidIssuerUri() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaListenerAuthenticationOAuth auth = new KafkaListenerAuthenticationOAuthBuilder()
                    .withClientId("my-client-id")
                    .withIntrospectionEndpointUri("http://introspection")
                    .withNewClientSecret()
                    .withSecretName("my-secret-secret")
                    .withKey("my-secret-key")
                    .endClientSecret()
                    .build();

            ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, getListeners(auth));
        });
    }

    @ParallelTest
    public void testOAuthValidationRefreshSecondsRelationWithExpirySeconds() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaListenerAuthenticationOAuth auth = new KafkaListenerAuthenticationOAuthBuilder()
                    .withValidIssuerUri("http://valid-issuer")
                    .withJwksEndpointUri("http://jwks-endpoint")
                    .withJwksRefreshSeconds(30)
                    .withJwksExpirySeconds(89)
                    .build();

            ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, getListeners(auth));
        });
    }

    @ParallelTest
    public void testOAuthValidationRefreshSecondsSetWithExpirySecondsNotSet() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaListenerAuthenticationOAuth auth = new KafkaListenerAuthenticationOAuthBuilder()
                    .withValidIssuerUri("http://valid-issuer")
                    .withJwksEndpointUri("http://jwks-endpoint")
                    .withJwksRefreshSeconds(333)
                    .build();

            ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, getListeners(auth));
        });
    }

    @ParallelTest
    public void testOAuthValidationRefreshSecondsNotSetWithExpirySecondsSet() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaListenerAuthenticationOAuth auth = new KafkaListenerAuthenticationOAuthBuilder()
                    .withValidIssuerUri("http://valid-issuer")
                    .withJwksEndpointUri("http://jwks-endpoint")
                    .withJwksExpirySeconds(150)
                    .build();

            ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, getListeners(auth));
        });
    }

    @ParallelTest
    public void testOAuthValidationNoUriSpecified() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaListenerAuthenticationOAuth auth = new KafkaListenerAuthenticationOAuthBuilder().build();

            ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, getListeners(auth));
        });
    }

    @ParallelTest
    public void testOAuthValidationWithMinimumJWKS() {
        KafkaListenerAuthenticationOAuth auth = new KafkaListenerAuthenticationOAuthBuilder()
                .withValidIssuerUri("http://valid-issuer")
                .withJwksEndpointUri("http://jwks-endpoint")
                .build();

        ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, getListeners(auth));
    }

    @ParallelTest
    public void testOAuthValidationWithConnectTimeout() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaListenerAuthenticationOAuth auth = new KafkaListenerAuthenticationOAuthBuilder()
                    .withValidIssuerUri("http://valid-issuer")
                    .withJwksEndpointUri("http://jwks-endpoint")
                    .withConnectTimeoutSeconds(0)
                    .build();

            ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, getListeners(auth));
        });
    }

    @ParallelTest
    public void testOAuthValidationWithReadTimeout() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaListenerAuthenticationOAuth auth = new KafkaListenerAuthenticationOAuthBuilder()
                    .withValidIssuerUri("http://valid-issuer")
                    .withJwksEndpointUri("http://jwks-endpoint")
                    .withReadTimeoutSeconds(0)
                    .build();

            ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, getListeners(auth));
        });
    }

    @ParallelTest
    public void testOAuthValidationWithGroupsClaim() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaListenerAuthenticationOAuth auth = new KafkaListenerAuthenticationOAuthBuilder()
                    .withValidIssuerUri("http://valid-issuer")
                    .withJwksEndpointUri("http://jwks-endpoint")
                    .withGroupsClaim("['bad'.'query']")
                    .build();

            ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, getListeners(auth));
        });
    }

    @ParallelTest
    public void testOAuthValidationIntrospectionEndpointUriWithoutClientId() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaListenerAuthenticationOAuth auth = new KafkaListenerAuthenticationOAuthBuilder()
                    .withIntrospectionEndpointUri("http://introspection")
                    .withNewClientSecret()
                    .withSecretName("my-secret-secret")
                    .withKey("my-secret-key")
                    .endClientSecret()
                    .build();

            ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, getListeners(auth));
        });
    }

    @ParallelTest
    public void testOAuthValidationIntrospectionEndpointUriWithoutClientSecret() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaListenerAuthenticationOAuth auth = new KafkaListenerAuthenticationOAuthBuilder()
                    .withClientId("my-client-id")
                    .withIntrospectionEndpointUri("http://introspection")
                    .build();

            ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, getListeners(auth));
        });
    }

    @ParallelTest
    public void testOAuthValidationExpirySecondsWithoutEndpointUri() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaListenerAuthenticationOAuth auth = new KafkaListenerAuthenticationOAuthBuilder()
                    .withIntrospectionEndpointUri("http://introspection")
                    .withClientId("my-client-id")
                    .withJwksExpirySeconds(100)
                    .build();

            ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, getListeners(auth));
        });
    }

    @ParallelTest
    public void testOAuthValidationRefreshSecondsWithoutEndpointUri() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaListenerAuthenticationOAuth auth = new KafkaListenerAuthenticationOAuthBuilder()
                    .withIntrospectionEndpointUri("http://introspection")
                    .withClientId("my-client-id")
                    .withJwksRefreshSeconds(40)
                    .build();

            ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, getListeners(auth));
        });
    }

    @ParallelTest
    public void testOAuthValidationWithOAuthWithIntrospectionWithNoTypeCheck() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaListenerAuthenticationOAuth auth = new KafkaListenerAuthenticationOAuthBuilder()
                    .withValidIssuerUri("http://valid-issuer")
                    .withClientId("my-kafka-id")
                    .withNewClientSecret()
                    .withSecretName("my-secret-secret")
                    .withKey("my-secret-key")
                    .endClientSecret()
                    .withIntrospectionEndpointUri("http://introspection-endpoint")
                    .withCheckAccessTokenType(false)
                    .build();

            ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, getListeners(auth));
        });
    }

    @ParallelTest
    public void testOAuthValidationWithOAuthWithJwksWithNotJwt() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaListenerAuthenticationOAuth auth = new KafkaListenerAuthenticationOAuthBuilder()
                    .withValidIssuerUri("http://valid-issuer")
                    .withJwksEndpointUri("http://jwks-endpoint")
                    .withJwksExpirySeconds(160)
                    .withJwksRefreshSeconds(50)
                    .withUserNameClaim("preferred_username")
                    .withAccessTokenIsJwt(false)
                    .build();

            ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, getListeners(auth));
        });
    }
}