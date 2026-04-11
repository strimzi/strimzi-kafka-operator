/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.auth.PemAuthIdentity;
import io.strimzi.operator.common.auth.PemTrustSet;
import io.strimzi.operator.common.operator.resource.concurrent.SecretOperator;
import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class MainTest {
    @Test
    void testAdminClientWithMTls() {
        TopicOperatorConfig config = TopicOperatorConfig.buildFromMap(Map.of(
                TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "my-kafka:9092",
                TopicOperatorConfig.WATCHED_NAMESPACE.key(), "my-namespace",
                TopicOperatorConfig.CLUSTER_NAMESPACE.key(), "my-namespace",
                TopicOperatorConfig.TRUSTSTORE_SECRET_NAME.key(), "cluster-ca-cert",
                TopicOperatorConfig.KEYSTORE_SECRET_NAME.key(), "topic-operator-cert",
                TopicOperatorConfig.KEYSTORE_KEY_NAME.key(), "user.key",
                TopicOperatorConfig.KEYSTORE_CERTIFICATE_NAME.key(), "user.crt"
        ));

        SecretOperator secretOperator = mock(SecretOperator.class);
        when(secretOperator.get("my-namespace", "cluster-ca-cert")).thenReturn(secretWithData("cluster-ca-cert", "my-namespace", Map.of(
                "ca.crt", Util.encodeToBase64("dummy-ca")
        )));
        when(secretOperator.get("my-namespace", "topic-operator-cert")).thenReturn(secretWithData("topic-operator-cert", "my-namespace", Map.of(
                "user.key", Util.encodeToBase64("dummy-key"),
                "user.crt", Util.encodeToBase64("dummy-crt")
        )));

        Admin expectedAdmin = mock(Admin.class);
        AdminClientProvider adminClientProvider = mock(AdminClientProvider.class);
        when(adminClientProvider.createAdminClient(eq("my-kafka:9092"), any(PemTrustSet.class), any(PemAuthIdentity.class), eq(config.adminClientConfig())))
                .thenReturn(expectedAdmin);

        Admin result = Main.createAdminClient(config, secretOperator, adminClientProvider);

        assertSame(expectedAdmin, result);
        ArgumentCaptor<PemTrustSet> trustSetCaptor = ArgumentCaptor.forClass(PemTrustSet.class);
        ArgumentCaptor<PemAuthIdentity> authIdentityCaptor = ArgumentCaptor.forClass(PemAuthIdentity.class);
        verify(adminClientProvider).createAdminClient(eq("my-kafka:9092"), trustSetCaptor.capture(), authIdentityCaptor.capture(), eq(config.adminClientConfig()));
        assertNotNull(trustSetCaptor.getValue());
        assertNotNull(authIdentityCaptor.getValue());
    }

    @Test
    void testAdminClientWithoutTls() {
        TopicOperatorConfig config = TopicOperatorConfig.buildFromMap(Map.of(
                TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "my-kafka:9092",
                TopicOperatorConfig.WATCHED_NAMESPACE.key(), "my-namespace"
        ));

        SecretOperator secretOperator = mock(SecretOperator.class);
        Admin expectedAdmin = mock(Admin.class);
        AdminClientProvider adminClientProvider = mock(AdminClientProvider.class);
        when(adminClientProvider.createAdminClient(eq("my-kafka:9092"), eq(null), eq(null), eq(config.adminClientConfig())))
                .thenReturn(expectedAdmin);

        Admin result = Main.createAdminClient(config, secretOperator, adminClientProvider);

        assertSame(expectedAdmin, result);
        verify(secretOperator, never()).get(any(), any());
        ArgumentCaptor<PemTrustSet> trustSetCaptor = ArgumentCaptor.forClass(PemTrustSet.class);
        ArgumentCaptor<PemAuthIdentity> authIdentityCaptor = ArgumentCaptor.forClass(PemAuthIdentity.class);
        verify(adminClientProvider).createAdminClient(eq("my-kafka:9092"), trustSetCaptor.capture(), authIdentityCaptor.capture(), eq(config.adminClientConfig()));
        assertNull(trustSetCaptor.getValue());
        assertNull(authIdentityCaptor.getValue());
    }

    @Test
    void testAdminClientWithConfiguredTlsButMissingSecrets() {
        TopicOperatorConfig config = TopicOperatorConfig.buildFromMap(Map.of(
                TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "my-kafka:9092",
                TopicOperatorConfig.WATCHED_NAMESPACE.key(), "my-namespace",
                TopicOperatorConfig.CLUSTER_NAMESPACE.key(), "my-namespace",
                TopicOperatorConfig.TRUSTSTORE_SECRET_NAME.key(), "cluster-ca-cert",
                TopicOperatorConfig.KEYSTORE_SECRET_NAME.key(), "topic-operator-cert",
                TopicOperatorConfig.KEYSTORE_KEY_NAME.key(), "user.key",
                TopicOperatorConfig.KEYSTORE_CERTIFICATE_NAME.key(), "user.crt"
        ));

        SecretOperator secretOperator = mock(SecretOperator.class);
        when(secretOperator.get("my-namespace", "cluster-ca-cert")).thenReturn(null);

        AdminClientProvider adminClientProvider = mock(AdminClientProvider.class);

        RuntimeException exception = assertThrows(RuntimeException.class, () -> Main.createAdminClient(config, secretOperator, adminClientProvider));

        assertEquals("Secret cluster-ca-cert in namespace my-namespace with certificates was configured but is missing.", exception.getMessage());
        verify(secretOperator).get("my-namespace", "cluster-ca-cert");
        verify(secretOperator, never()).get("my-namespace", "topic-operator-cert");
        verifyNoInteractions(adminClientProvider);
    }

    private static Secret secretWithData(String name, String namespace, Map<String, String> data) {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                .endMetadata()
                .withData(data)
                .build();
    }
}
