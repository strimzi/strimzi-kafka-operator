/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityAuthenticationBuilder;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityAuthenticationType;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.AuthenticationConfiguration;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.KafkaClusterSecurityContext;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.NoneAuthenticationConfiguration;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.NoneEncryptionConfiguration;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.TlsEncryptionConfiguration;
import io.strimzi.operator.common.Reconciliation;
import org.junit.jupiter.api.Test;

import static io.strimzi.operator.cluster.TestUtils.IsEquivalent.isEquivalent;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaAgentConfigurationBuilderTest {
    private final static NodeRef NODE_REF = new NodeRef("my-cluster-kafka-2", 2, "kafka", false, true);

    @Test
    public void testTlsAndMtls()  {
        String configuration = new KafkaAgentConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withSecurity(KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT)
                .build();

        assertThat(configuration, isEquivalent(
                "namespace=namespace",
                "sslKeyStoreSecretName=my-cluster-kafka-2",
                "sslTrustStoreSecretName=name-cluster-ca-cert"
        ));
    }

    @Test
    public void testTlsWithoutAuthentication()  {
        KafkaClusterSecurityContext securityContext = mock(KafkaClusterSecurityContext.class);
        when(securityContext.encryption()).thenReturn(new TlsEncryptionConfiguration());
        when(securityContext.authentication()).thenReturn(new NoneAuthenticationConfiguration());

        String configuration = new KafkaAgentConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withSecurity(securityContext)
                .build();

        assertThat(configuration, isEquivalent(
                "namespace=namespace",
                "sslKeyStoreSecretName=my-cluster-kafka-2"
        ));
    }

    @Test
    public void testTlsAndServiceAccountAuthentication()  {
        KafkaClusterSecurityContext securityContext = mock(KafkaClusterSecurityContext.class);
        when(securityContext.encryption()).thenReturn(new TlsEncryptionConfiguration());
        when(securityContext.authentication()).thenReturn(AuthenticationConfiguration.fromCrd("namespace", "name", new ClusterSecurityAuthenticationBuilder().withType(ClusterSecurityAuthenticationType.SERVICE_ACCOUNT).build()));

        String configuration = new KafkaAgentConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withSecurity(securityContext)
                .build();

        assertThat(configuration, isEquivalent(
                "namespace=namespace",
                "sslKeyStoreSecretName=my-cluster-kafka-2",
                "tokenIssuer=https://kubernetes.default.svc.cluster.local",
                "tokenJwksUri=https://kubernetes.default.svc.cluster.local/openid/v1/jwks",
                "tokenJwksCaPath=/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
                "tokenAudience=strimzi.io/kafka/namespace/name",
                "tokenAllowedUsers=system:serviceaccount:namespace:name-cluster-operator"
        ));
    }

    @Test
    public void testServiceAccountAuthenticationWithoutTls()  {
        KafkaClusterSecurityContext securityContext = mock(KafkaClusterSecurityContext.class);
        when(securityContext.encryption()).thenReturn(new NoneEncryptionConfiguration());
        when(securityContext.authentication()).thenReturn(AuthenticationConfiguration.fromCrd("namespace", "name", new ClusterSecurityAuthenticationBuilder().withType(ClusterSecurityAuthenticationType.SERVICE_ACCOUNT).build()));

        String configuration = new KafkaAgentConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withSecurity(securityContext)
                .build();

        assertThat(configuration, isEquivalent(
                "tokenIssuer=https://kubernetes.default.svc.cluster.local",
                "tokenJwksUri=https://kubernetes.default.svc.cluster.local/openid/v1/jwks",
                "tokenJwksCaPath=/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
                "tokenAudience=strimzi.io/kafka/namespace/name",
                "tokenAllowedUsers=system:serviceaccount:namespace:name-cluster-operator"
        ));
    }

    @Test
    public void testWithoutTlsOrAuthentication()  {
        KafkaClusterSecurityContext securityContext = mock(KafkaClusterSecurityContext.class);
        when(securityContext.encryption()).thenReturn(new NoneEncryptionConfiguration());
        when(securityContext.authentication()).thenReturn(new NoneAuthenticationConfiguration());

        String configuration = new KafkaAgentConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withSecurity(securityContext)
                .build();

        assertThat(configuration, isEquivalent(""));
    }
}
