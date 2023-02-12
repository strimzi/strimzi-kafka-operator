/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.KafkaClusterSpec;
import io.strimzi.api.kafka.model.KafkaClusterSpecBuilder;
import io.strimzi.operator.cluster.model.JmxModel;
import io.strimzi.operator.cluster.model.SupportsJmx;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.vertx.core.Future;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class ReconcilerUtilsTest {
    private final static String NAME = "my-jmx-secret";
    private final static String NAMESPACE = "namespace";
    private static final OwnerReference OWNER_REFERENCE = new OwnerReferenceBuilder()
            .withApiVersion("v1")
            .withKind("my-kind")
            .withName("my-name")
            .withUid("my-uid")
            .withBlockOwnerDeletion(false)
            .withController(false)
            .build();
    private static final Labels LABELS = Labels
            .forStrimziKind("my-kind")
            .withStrimziName("my-name")
            .withStrimziCluster("my-cluster")
            .withStrimziComponentType("my-component-type");
    private static final Secret EXISTING_JMX_SECRET = new SecretBuilder()
            .withNewMetadata()
                .withName(NAME)
            .endMetadata()
            .withData(Map.of("jmx-username", "username", "jmx-password", "password"))
            .build();

    @Test
    public void testDisabledJmxWithMissingSecret(VertxTestContext context) {
        KafkaClusterSpec spec = new KafkaClusterSpecBuilder().build();
        JmxModel jmx = new JmxModel(NAMESPACE, NAME, LABELS, OWNER_REFERENCE, spec);

        SecretOperator mockSecretOps = mock(SecretOperator.class);
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture());

        Checkpoint async = context.checkpoint();
        ReconcilerUtils.reconcileJmxSecret(Reconciliation.DUMMY_RECONCILIATION, mockSecretOps, new MockJmxCluster(jmx))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    verify(mockSecretOps, never()).reconcile(any(), any(), any(), any());

                    async.flag();
                })));
    }

    @Test
    public void testDisabledJmxWithExistingSecret(VertxTestContext context) {
        KafkaClusterSpec spec = new KafkaClusterSpecBuilder().build();
        JmxModel jmx = new JmxModel(NAMESPACE, NAME, LABELS, OWNER_REFERENCE, spec);

        SecretOperator mockSecretOps = mock(SecretOperator.class);
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture(EXISTING_JMX_SECRET));
        when(mockSecretOps.reconcile(any(), any(), any(), any())).thenAnswer(i -> {
            if (i.getArgument(3) == null) {
                return Future.succeededFuture(ReconcileResult.deleted());
            } else {
                return Future.succeededFuture(ReconcileResult.patched(i.getArgument(3)));
            }
        });

        Checkpoint async = context.checkpoint();
        ReconcilerUtils.reconcileJmxSecret(Reconciliation.DUMMY_RECONCILIATION, mockSecretOps, new MockJmxCluster(jmx))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    verify(mockSecretOps, times(1)).reconcile(eq(Reconciliation.DUMMY_RECONCILIATION), eq(NAMESPACE), eq(NAME), eq(null));

                    async.flag();
                })));
    }

    @Test
    public void testEnabledJmxWithoutAuthWithMissingSecret(VertxTestContext context) {
        KafkaClusterSpec spec = new KafkaClusterSpecBuilder().withNewJmxOptions().endJmxOptions().build();
        JmxModel jmx = new JmxModel(NAMESPACE, NAME, LABELS, OWNER_REFERENCE, spec);

        SecretOperator mockSecretOps = mock(SecretOperator.class);
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture());

        Checkpoint async = context.checkpoint();
        ReconcilerUtils.reconcileJmxSecret(Reconciliation.DUMMY_RECONCILIATION, mockSecretOps, new MockJmxCluster(jmx))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    verify(mockSecretOps, never()).reconcile(any(), any(), any(), any());

                    async.flag();
                })));
    }

    @Test
    public void testEnabledJmxWithoutAuthWithExistingSecret(VertxTestContext context) {
        KafkaClusterSpec spec = new KafkaClusterSpecBuilder().withNewJmxOptions().endJmxOptions().build();
        JmxModel jmx = new JmxModel(NAMESPACE, NAME, LABELS, OWNER_REFERENCE, spec);

        SecretOperator mockSecretOps = mock(SecretOperator.class);
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture(EXISTING_JMX_SECRET));
        when(mockSecretOps.reconcile(any(), any(), any(), any())).thenAnswer(i -> {
            if (i.getArgument(3) == null) {
                return Future.succeededFuture(ReconcileResult.deleted());
            } else {
                return Future.succeededFuture(ReconcileResult.patched(i.getArgument(3)));
            }
        });

        Checkpoint async = context.checkpoint();
        ReconcilerUtils.reconcileJmxSecret(Reconciliation.DUMMY_RECONCILIATION, mockSecretOps, new MockJmxCluster(jmx))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    verify(mockSecretOps, times(1)).reconcile(eq(Reconciliation.DUMMY_RECONCILIATION), eq(NAMESPACE), eq(NAME), eq(null));

                    async.flag();
                })));
    }

    @Test
    public void testEnabledJmxWithAuthWithMissingSecret(VertxTestContext context) {
        KafkaClusterSpec spec = new KafkaClusterSpecBuilder()
                .withNewJmxOptions()
                    .withNewKafkaJmxAuthenticationPassword()
                    .endKafkaJmxAuthenticationPassword()
                .endJmxOptions()
                .build();
        JmxModel jmx = new JmxModel(NAMESPACE, NAME, LABELS, OWNER_REFERENCE, spec);

        SecretOperator mockSecretOps = mock(SecretOperator.class);
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture());
        ArgumentCaptor<Secret> secretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(any(), any(), any(), secretCaptor.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(3))));

        Checkpoint async = context.checkpoint();
        ReconcilerUtils.reconcileJmxSecret(Reconciliation.DUMMY_RECONCILIATION, mockSecretOps, new MockJmxCluster(jmx))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    verify(mockSecretOps, times(1)).reconcile(eq(Reconciliation.DUMMY_RECONCILIATION), eq(NAMESPACE), eq(NAME), any());

                    Secret secret = secretCaptor.getValue();
                    assertThat(secret, is(notNullValue()));
                    assertThat(secret.getMetadata().getName(), is(NAME));
                    assertThat(secret.getMetadata().getNamespace(), is(NAMESPACE));
                    assertThat(secret.getMetadata().getOwnerReferences(), is(List.of(OWNER_REFERENCE)));
                    assertThat(secret.getMetadata().getLabels(), is(LABELS.toMap()));
                    assertThat(secret.getMetadata().getAnnotations(), is(nullValue()));
                    assertThat(secret.getData().size(), is(2));
                    assertThat(secret.getData().get("jmx-username"), is(notNullValue()));
                    assertThat(secret.getData().get("jmx-password"), is(notNullValue()));

                    async.flag();
                })));
    }

    @Test
    public void testEnabledJmxWithAuthWithExistingSecret(VertxTestContext context) {
        KafkaClusterSpec spec = new KafkaClusterSpecBuilder()
                .withNewJmxOptions()
                    .withNewKafkaJmxAuthenticationPassword()
                    .endKafkaJmxAuthenticationPassword()
                .endJmxOptions()
                .build();
        JmxModel jmx = new JmxModel(NAMESPACE, NAME, LABELS, OWNER_REFERENCE, spec);

        SecretOperator mockSecretOps = mock(SecretOperator.class);
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture(EXISTING_JMX_SECRET));
        ArgumentCaptor<Secret> secretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(any(), any(), any(), secretCaptor.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.patched(i.getArgument(3))));

        Checkpoint async = context.checkpoint();
        ReconcilerUtils.reconcileJmxSecret(Reconciliation.DUMMY_RECONCILIATION, mockSecretOps, new MockJmxCluster(jmx))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    verify(mockSecretOps, times(1)).reconcile(eq(Reconciliation.DUMMY_RECONCILIATION), eq(NAMESPACE), eq(NAME), any());

                    Secret secret = secretCaptor.getValue();
                    assertThat(secret, is(notNullValue()));
                    assertThat(secret.getMetadata().getName(), is(NAME));
                    assertThat(secret.getMetadata().getNamespace(), is(NAMESPACE));
                    assertThat(secret.getMetadata().getOwnerReferences(), is(List.of(OWNER_REFERENCE)));
                    assertThat(secret.getMetadata().getLabels(), is(LABELS.toMap()));
                    assertThat(secret.getMetadata().getAnnotations(), is(nullValue()));
                    assertThat(secret.getData().size(), is(2));
                    assertThat(secret.getData().get("jmx-username"), is("username"));
                    assertThat(secret.getData().get("jmx-password"), is("password"));

                    async.flag();
                })));
    }

    static class MockJmxCluster implements SupportsJmx {
        private final JmxModel jmx;

        public MockJmxCluster(JmxModel jmx) {
            this.jmx = jmx;
        }

        @Override
        public JmxModel jmx() {
            return jmx;
        }
    }
}
