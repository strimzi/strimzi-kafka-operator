/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationOAuth;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationOAuthBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.InvalidResourceException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class KafkaClusterOAuthValidationTest {
    private final static Set<NodeRef> THREE_NODES = Set.of(
            new NodeRef("my-cluster-mixed-0", 0, "mixed", true, true),
            new NodeRef("my-cluster-mixed-1", 1, "mixed", true, true),
            new NodeRef("my-cluster-mixed-2", 2, "mixed", true, true));

    @SuppressWarnings("deprecation") // OAuth authentication is deprecated
    private static List<GenericKafkaListener> getListeners(KafkaListenerAuthenticationOAuth auth)   {
        GenericKafkaListener listener1 = new GenericKafkaListenerBuilder()
                .withName("listener1")
                .withPort(9900)
                .withType(KafkaListenerType.INTERNAL)
                .withAuth(auth)
                .build();

        return List.of(listener1);
    }

    @SuppressWarnings("deprecation") // OAuth authentication is deprecated
    @Test
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

    @SuppressWarnings("deprecation") // OAuth authentication is deprecated
    @Test
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

    @SuppressWarnings("deprecation") // OAuth authentication is deprecated
    @Test
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

    @SuppressWarnings("deprecation") // OAuth authentication is deprecated
    @Test
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

    @SuppressWarnings("deprecation") // OAuth authentication is deprecated
    @Test
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

    @SuppressWarnings("deprecation") // OAuth authentication is deprecated
    @Test
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

    @SuppressWarnings("deprecation") // OAuth authentication is deprecated
    @Test
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

    @SuppressWarnings("deprecation") // OAuth authentication is deprecated
    @Test
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

    @SuppressWarnings("deprecation") // OAuth authentication is deprecated
    @Test
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

    @SuppressWarnings("deprecation") // OAuth authentication is deprecated
    @Test
    public void testOAuthValidationNoUriSpecified() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaListenerAuthenticationOAuth auth = new KafkaListenerAuthenticationOAuthBuilder().build();

            ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, getListeners(auth));
        });
    }

    @SuppressWarnings("deprecation") // OAuth authentication is deprecated
    @Test
    public void testOAuthValidationWithMinimumJWKS() {
        KafkaListenerAuthenticationOAuth auth = new KafkaListenerAuthenticationOAuthBuilder()
                .withValidIssuerUri("http://valid-issuer")
                .withJwksEndpointUri("http://jwks-endpoint")
                .build();

        ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, getListeners(auth));
    }

    @SuppressWarnings("deprecation") // OAuth authentication is deprecated
    @Test
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

    @SuppressWarnings("deprecation") // OAuth authentication is deprecated
    @Test
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

    @SuppressWarnings("deprecation") // OAuth authentication is deprecated
    @Test
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

    @SuppressWarnings("deprecation") // OAuth authentication is deprecated
    @Test
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

    @SuppressWarnings("deprecation") // OAuth authentication is deprecated
    @Test
    public void testOAuthValidationIntrospectionEndpointUriWithoutClientSecret() {
        assertThrows(InvalidResourceException.class, () -> {
            KafkaListenerAuthenticationOAuth auth = new KafkaListenerAuthenticationOAuthBuilder()
                    .withClientId("my-client-id")
                    .withIntrospectionEndpointUri("http://introspection")
                    .build();

            ListenersValidator.validate(Reconciliation.DUMMY_RECONCILIATION, THREE_NODES, getListeners(auth));
        });
    }

    @SuppressWarnings("deprecation") // OAuth authentication is deprecated
    @Test
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

    @SuppressWarnings("deprecation") // OAuth authentication is deprecated
    @Test
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

    @SuppressWarnings("deprecation") // OAuth authentication is deprecated
    @Test
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

    @SuppressWarnings("deprecation") // OAuth authentication is deprecated
    @Test
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