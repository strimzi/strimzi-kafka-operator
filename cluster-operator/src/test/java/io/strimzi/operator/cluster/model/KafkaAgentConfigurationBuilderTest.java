/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

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
        when(securityContext.isStrimziTlsEncryption()).thenReturn(true);
        when(securityContext.isStrimziMtlsAuthentication()).thenReturn(false);

        String configuration = new KafkaAgentConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withSecurity(securityContext)
                .build();

        assertThat(configuration, isEquivalent(
                "namespace=namespace",
                "sslKeyStoreSecretName=my-cluster-kafka-2"
        ));
    }

    @Test
    public void testWithoutTlsOrAuthentication()  {
        KafkaClusterSecurityContext securityContext = mock(KafkaClusterSecurityContext.class);
        when(securityContext.isStrimziTlsEncryption()).thenReturn(false);
        when(securityContext.isStrimziMtlsAuthentication()).thenReturn(false);

        String configuration = new KafkaAgentConfigurationBuilder(Reconciliation.DUMMY_RECONCILIATION, NODE_REF)
                .withSecurity(securityContext)
                .build();

        assertThat(configuration, isEquivalent(""));
    }
}
