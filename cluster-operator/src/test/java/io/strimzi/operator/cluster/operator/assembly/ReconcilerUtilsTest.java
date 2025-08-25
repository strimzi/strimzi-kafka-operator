/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.common.CertSecretSource;
import io.strimzi.api.kafka.model.common.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.common.GenericSecretSource;
import io.strimzi.api.kafka.model.common.GenericSecretSourceBuilder;
import io.strimzi.api.kafka.model.common.PasswordSecretSource;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthentication;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationOAuthBuilder;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationPlain;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationScramSha512;
import io.strimzi.api.kafka.model.kafka.KafkaClusterSpec;
import io.strimzi.api.kafka.model.kafka.KafkaClusterSpecBuilder;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.jmx.JmxModel;
import io.strimzi.operator.cluster.model.jmx.SupportsJmx;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.vertx.core.Future;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
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
    public void testControllerNameFromPodName() {
        assertThat(ReconcilerUtils.getControllerNameFromPodName("my-cluster-brokers-2"), is("my-cluster-brokers"));
        assertThat(ReconcilerUtils.getControllerNameFromPodName("my-cluster-new-brokers-2"), is("my-cluster-new-brokers"));
        assertThat(ReconcilerUtils.getControllerNameFromPodName("my-cluster-brokers2-2"), is("my-cluster-brokers2"));
    }

    @Test
    public void testPoolNameFromPodName() {
        assertThat(ReconcilerUtils.getPoolNameFromPodName("my-cluster", "my-cluster-brokers-2"), is("brokers"));
        assertThat(ReconcilerUtils.getPoolNameFromPodName("my-cluster", "my-cluster-new-brokers-2"), is("new-brokers"));
        assertThat(ReconcilerUtils.getPoolNameFromPodName("my-cluster", "my-cluster-brokers2-2"), is("brokers2"));
    }

    @Test
    public void testClusterNameFromPodName() {
        assertThat(ReconcilerUtils.clusterNameFromLabel(new PodBuilder()
                .withNewMetadata()
                    .withName("my-cluster-new-brokers-1")
                    .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, "my-cluster"))
                .endMetadata()
                .build()), is("my-cluster"));

        RuntimeException ex = assertThrows(RuntimeException.class, () -> ReconcilerUtils.clusterNameFromLabel(new PodBuilder()
                        .withNewMetadata()
                            .withName("my-cluster-new-brokers-1")
                        .endMetadata()
                        .build()));
        assertThat(ex.getMessage(), is("Failed to extract cluster name from Pod label"));
    }

    @Test
    public void testNodeRefFromPod() {
        NodeRef node = ReconcilerUtils.nodeFromPod(new PodBuilder()
                .withNewMetadata()
                    .withName("my-cluster-new-brokers-1")
                    .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, "my-cluster"))
                .endMetadata()
                .build());

        assertThat(node.podName(), is("my-cluster-new-brokers-1"));
        assertThat(node.nodeId(), is(1));
        assertThat(node.poolName(), is("new-brokers"));
        assertThat(node.controller(), is(false));
        assertThat(node.broker(), is(false));

        node = ReconcilerUtils.nodeFromPod(new PodBuilder()
                .withNewMetadata()
                    .withName("my-cluster-new-brokers-1")
                    .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, "my-cluster", Labels.STRIMZI_CONTROLLER_ROLE_LABEL, "true", Labels.STRIMZI_BROKER_ROLE_LABEL, "false"))
                .endMetadata()
                .build());

        assertThat(node.podName(), is("my-cluster-new-brokers-1"));
        assertThat(node.nodeId(), is(1));
        assertThat(node.poolName(), is("new-brokers"));
        assertThat(node.controller(), is(true));
        assertThat(node.broker(), is(false));

        node = ReconcilerUtils.nodeFromPod(new PodBuilder()
                .withNewMetadata()
                    .withName("my-cluster-new-brokers-1")
                    .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, "my-cluster", Labels.STRIMZI_CONTROLLER_ROLE_LABEL, "false", Labels.STRIMZI_BROKER_ROLE_LABEL, "true"))
                .endMetadata()
                .build());

        assertThat(node.podName(), is("my-cluster-new-brokers-1"));
        assertThat(node.nodeId(), is(1));
        assertThat(node.poolName(), is("new-brokers"));
        assertThat(node.controller(), is(false));
        assertThat(node.broker(), is(true));
    }

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
    
    @Test
    public void testHashSecretContent() {
        Secret secret = new SecretBuilder()
            .addToData(Map.of("username", "foo"))
            .addToData(Map.of("password", "changeit"))
            .build();
        assertThat(ReconcilerUtils.hashSecretContent(secret), is("756937ae"));
    }

    @Test
    public void testHashSecretContentWithNoData() {
        Secret secret = new SecretBuilder().build();
        RuntimeException ex = assertThrows(RuntimeException.class, () -> ReconcilerUtils.hashSecretContent(secret));
        assertThat(ex.getMessage(), is("Empty secret"));
    }

    @Test
    public void testHashSecretContentWithNoSecret() {
        RuntimeException ex = assertThrows(RuntimeException.class, () -> ReconcilerUtils.hashSecretContent(null));
        assertThat(ex.getMessage(), is("Secret not found"));
    }

    @Test
    public void testMatchesSelector()   {
        Pod testResource = new PodBuilder()
                .withNewMetadata()
                .withName("test-pod")
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .build();

        // Resources without any labels
        LabelSelector selector = null;
        assertThat(ReconcilerUtils.matchesSelector(selector, testResource), is(true));

        selector = new LabelSelectorBuilder().withMatchLabels(emptyMap()).build();
        assertThat(ReconcilerUtils.matchesSelector(selector, testResource), is(true));

        selector = new LabelSelectorBuilder().withMatchLabels(Map.of("label2", "value2")).build();
        assertThat(ReconcilerUtils.matchesSelector(selector, testResource), is(false));

        // Resources with Labels
        testResource.getMetadata().setLabels(Map.of("label1", "value1", "label2", "value2"));

        selector = null;
        assertThat(ReconcilerUtils.matchesSelector(selector, testResource), is(true));

        selector = new LabelSelectorBuilder().withMatchLabels(emptyMap()).build();
        assertThat(ReconcilerUtils.matchesSelector(selector, testResource), is(true));

        selector = new LabelSelectorBuilder().withMatchLabels(Map.of("label2", "value2")).build();
        assertThat(ReconcilerUtils.matchesSelector(selector, testResource), is(true));

        selector = new LabelSelectorBuilder().withMatchLabels(Map.of("label2", "value2", "label1", "value1")).build();
        assertThat(ReconcilerUtils.matchesSelector(selector, testResource), is(true));

        selector = new LabelSelectorBuilder().withMatchLabels(Map.of("label2", "value1")).build();
        assertThat(ReconcilerUtils.matchesSelector(selector, testResource), is(false));

        selector = new LabelSelectorBuilder().withMatchLabels(Map.of("label3", "value3")).build();
        assertThat(ReconcilerUtils.matchesSelector(selector, testResource), is(false));

        selector = new LabelSelectorBuilder().withMatchLabels(Map.of("label2", "value2", "label1", "value1", "label3", "value3")).build();
        assertThat(ReconcilerUtils.matchesSelector(selector, testResource), is(false));
    }

    @Test
    void getHashOk() {
        String namespace = "ns";

        GenericSecretSource at = new GenericSecretSourceBuilder()
                .withSecretName("top-secret-at")
                .withKey("key")
                .build();

        GenericSecretSource cs = new GenericSecretSourceBuilder()
                .withSecretName("top-secret-cs")
                .withKey("key")
                .build();

        GenericSecretSource rt = new GenericSecretSourceBuilder()
                .withSecretName("top-secret-rt")
                .withKey("key")
                .build();
        KafkaClientAuthentication kcu = new KafkaClientAuthenticationOAuthBuilder()
                .withAccessToken(at)
                .withRefreshToken(rt)
                .withClientSecret(cs)
                .build();

        CertSecretSource css = new CertSecretSourceBuilder()
                .withCertificate("key")
                .withSecretName("css-secret")
                .build();

        Secret secret = new SecretBuilder()
                .withData(Map.of("key", "value"))
                .build();

        SecretOperator secretOps = mock(SecretOperator.class);
        when(secretOps.getAsync(eq(namespace), eq("top-secret-at"))).thenReturn(Future.succeededFuture(secret));
        when(secretOps.getAsync(eq(namespace), eq("top-secret-rt"))).thenReturn(Future.succeededFuture(secret));
        when(secretOps.getAsync(eq(namespace), eq("top-secret-cs"))).thenReturn(Future.succeededFuture(secret));
        when(secretOps.getAsync(eq(namespace), eq("css-secret"))).thenReturn(Future.succeededFuture(secret));
        Future<Integer> res = ReconcilerUtils.authTlsHash(secretOps, "ns", kcu, singletonList(css));
        res.onComplete(v -> {
            assertThat(v.succeeded(), is(true));
            // we are summing "value" hash four times
            assertThat(v.result(), is("value".hashCode() * 4));
        });
    }

    @Test
    void getHashFailure() {
        String namespace = "ns";

        GenericSecretSource at = new GenericSecretSourceBuilder()
                .withSecretName("top-secret-at")
                .withKey("key")
                .build();

        GenericSecretSource cs = new GenericSecretSourceBuilder()
                .withSecretName("top-secret-cs")
                .withKey("key")
                .build();

        GenericSecretSource rt = new GenericSecretSourceBuilder()
                .withSecretName("top-secret-rt")
                .withKey("key")
                .build();
        KafkaClientAuthentication kcu = new KafkaClientAuthenticationOAuthBuilder()
                .withAccessToken(at)
                .withRefreshToken(rt)
                .withClientSecret(cs)
                .build();

        CertSecretSource css = new CertSecretSourceBuilder()
                .withCertificate("key")
                .withSecretName("css-secret")
                .build();

        Secret secret = new SecretBuilder()
                .withData(Map.of("key", "value"))
                .build();

        SecretOperator secretOps = mock(SecretOperator.class);
        when(secretOps.getAsync(eq(namespace), eq("top-secret-at"))).thenReturn(Future.succeededFuture(secret));
        when(secretOps.getAsync(eq(namespace), eq("top-secret-rt"))).thenReturn(Future.succeededFuture(secret));
        when(secretOps.getAsync(eq(namespace), eq("top-secret-cs"))).thenReturn(Future.succeededFuture(null));
        when(secretOps.getAsync(eq(namespace), eq("css-secret"))).thenReturn(Future.succeededFuture(secret));
        Future<Integer> res = ReconcilerUtils.authTlsHash(secretOps, "ns", kcu, singletonList(css));
        res.onComplete(v -> {
            assertThat(v.succeeded(), is(false));
            assertThat(v.cause().getMessage(), is("Secret top-secret-cs not found"));
        });
    }

    @Test
    void getHashForPattern(VertxTestContext context) {
        String namespace = "ns";

        CertSecretSource cert1 = new CertSecretSourceBuilder()
                .withSecretName("cert-secret")
                .withPattern("*.crt")
                .build();
        CertSecretSource cert2 = new CertSecretSourceBuilder()
                .withSecretName("cert-secret2")
                .withPattern("*.crt")
                .build();
        CertSecretSource cert3 = new CertSecretSourceBuilder()
                .withSecretName("cert-secret3")
                .withCertificate("my.crt")
                .build();

        Secret secret = new SecretBuilder()
                .withData(Map.of("ca.crt", "value", "ca2.crt", "value2"))
                .build();
        Secret secret2 = new SecretBuilder()
                .withData(Map.of("ca3.crt", "value3", "ca4.crt", "value4"))
                .build();
        Secret secret3 = new SecretBuilder()
                .withData(Map.of("my.crt", "value5"))
                .build();

        SecretOperator secretOps = mock(SecretOperator.class);
        when(secretOps.getAsync(eq(namespace), eq("cert-secret"))).thenReturn(Future.succeededFuture(secret));
        when(secretOps.getAsync(eq(namespace), eq("cert-secret2"))).thenReturn(Future.succeededFuture(secret2));
        when(secretOps.getAsync(eq(namespace), eq("cert-secret3"))).thenReturn(Future.succeededFuture(secret3));

        Checkpoint async = context.checkpoint();
        ReconcilerUtils.authTlsHash(secretOps, "ns", null, List.of(cert1, cert2, cert3)).onComplete(context.succeeding(res -> {
            assertThat(res, is("valuevalue2".hashCode() + "value3value4".hashCode() + "value5".hashCode()));
            async.flag();
        }));
    }

    @Test
    void getHashPatternNotMatching(VertxTestContext context) {
        String namespace = "ns";

        CertSecretSource cert1 = new CertSecretSourceBuilder()
                .withSecretName("cert-secret")
                .withPattern("*.pem")
                .build();

        Secret secret = new SecretBuilder()
                .withData(Map.of("ca.crt", "value", "ca2.crt", "value2"))
                .build();

        SecretOperator secretOps = mock(SecretOperator.class);
        when(secretOps.getAsync(eq(namespace), eq("cert-secret"))).thenReturn(Future.succeededFuture(secret));

        Checkpoint async = context.checkpoint();
        ReconcilerUtils.authTlsHash(secretOps, "ns", null, singletonList(cert1)).onComplete(context.succeeding(res -> {
            assertThat(res, is(0));
            async.flag();
        }));
    }

    @Test
    void testAuthTlsHashScramSha512SecretFoundAndPasswordNotFound() {
        SecretOperator secretOperator = mock(SecretOperator.class);
        Map<String, String> data = new HashMap<>();
        data.put("passwordKey", "my-password");
        Secret secret = new Secret();
        secret.setData(data);
        CompletionStage<Secret> cf = CompletableFuture.supplyAsync(() ->  secret);
        when(secretOperator.getAsync(anyString(), anyString())).thenReturn(Future.fromCompletionStage(cf));
        KafkaClientAuthenticationScramSha512 auth = new KafkaClientAuthenticationScramSha512();
        PasswordSecretSource passwordSecretSource = new PasswordSecretSource();
        passwordSecretSource.setSecretName("my-secret");
        passwordSecretSource.setPassword("password1");
        auth.setPasswordSecret(passwordSecretSource);
        Future<Integer> result = ReconcilerUtils.authTlsHash(secretOperator, "anyNamespace", auth, List.of());
        result.onComplete(handler -> {
            assertTrue(handler.failed());
            assertEquals("Items with key(s) [password1] are missing in Secret my-secret", handler.cause().getMessage());
        });
    }

    @Test
    void testAuthTlsHashScramSha512SecretAndPasswordFound() {
        SecretOperator secretOperator = mock(SecretOperator.class);
        Map<String, String> data = new HashMap<>();
        data.put("passwordKey", "my-password");
        Secret secret = new Secret();
        secret.setData(data);
        CompletionStage<Secret> cf = CompletableFuture.supplyAsync(() ->  secret);
        when(secretOperator.getAsync(anyString(), anyString())).thenReturn(Future.fromCompletionStage(cf));
        KafkaClientAuthenticationScramSha512 auth = new KafkaClientAuthenticationScramSha512();
        PasswordSecretSource passwordSecretSource = new PasswordSecretSource();
        passwordSecretSource.setSecretName("my-secret");
        passwordSecretSource.setPassword("passwordKey");
        auth.setPasswordSecret(passwordSecretSource);
        Future<Integer> result = ReconcilerUtils.authTlsHash(secretOperator, "anyNamespace", auth, List.of());
        result.onComplete(handler -> {
            assertTrue(handler.succeeded());
            assertEquals("my-password".hashCode(), handler.result());
        });
    }

    @Test
    void testAuthTlsPlainSecretFoundAndPasswordNotFound() {
        SecretOperator secretOperator = mock(SecretOperator.class);
        Map<String, String> data = new HashMap<>();
        data.put("passwordKey", "my-password");
        Secret secret = new Secret();
        secret.setData(data);
        CompletionStage<Secret> cf = CompletableFuture.supplyAsync(() ->  secret);
        when(secretOperator.getAsync(anyString(), anyString())).thenReturn(Future.fromCompletionStage(cf));
        KafkaClientAuthenticationPlain auth = new KafkaClientAuthenticationPlain();
        PasswordSecretSource passwordSecretSource = new PasswordSecretSource();
        passwordSecretSource.setSecretName("my-secret");
        passwordSecretSource.setPassword("password1");
        auth.setPasswordSecret(passwordSecretSource);
        Future<Integer> result = ReconcilerUtils.authTlsHash(secretOperator, "anyNamespace", auth, List.of());
        result.onComplete(handler -> {
            assertTrue(handler.failed());
            assertEquals("Items with key(s) [password1] are missing in Secret my-secret", handler.cause().getMessage());
        });
    }

    @Test
    void testAuthTlsPlainSecretAndPasswordFound() {
        SecretOperator secretOperator = mock(SecretOperator.class);
        Map<String, String> data = new HashMap<>();
        data.put("passwordKey", "my-password");
        Secret secret = new Secret();
        secret.setData(data);
        CompletionStage<Secret> cf = CompletableFuture.supplyAsync(() ->  secret);
        when(secretOperator.getAsync(anyString(), anyString())).thenReturn(Future.fromCompletionStage(cf));
        KafkaClientAuthenticationPlain auth = new KafkaClientAuthenticationPlain();
        PasswordSecretSource passwordSecretSource = new PasswordSecretSource();
        passwordSecretSource.setSecretName("my-secret");
        passwordSecretSource.setPassword("passwordKey");
        auth.setPasswordSecret(passwordSecretSource);
        Future<Integer> result = ReconcilerUtils.authTlsHash(secretOperator, "anyNamespace", auth, List.of());
        result.onComplete(handler -> {
            assertTrue(handler.succeeded());
            assertEquals("my-password".hashCode(), handler.result());
        });
    }

    @Test
    void testGetValidateSecret() {
        String namespace = "ns";
        String secretName = "my-secret";

        Secret secret = new SecretBuilder()
                .withNewMetadata()
                .withName(secretName)
                .withNamespace(namespace)
                .endMetadata()
                .withData(Map.of("key1", "value", "key2", "value", "key3", "value"))
                .build();

        SecretOperator secretOps = mock(SecretOperator.class);
        when(secretOps.getAsync(eq(namespace), eq(secretName))).thenReturn(Future.succeededFuture(secret));

        ReconcilerUtils.getValidatedSecret(secretOps, namespace, secretName, "key1", "key2")
                .onComplete(r -> {
                    assertThat(r.succeeded(), is(true));
                    assertThat(r.result(), is(secret));
                });
    }

    @Test
    void testGetValidateSecretMissingSecret() {
        String namespace = "ns";
        String secretName = "my-secret";

        SecretOperator secretOps = mock(SecretOperator.class);
        when(secretOps.getAsync(eq(namespace), eq(secretName))).thenReturn(Future.succeededFuture(null));

        ReconcilerUtils.getValidatedSecret(secretOps, namespace, secretName, "key1", "key2")
                .onComplete(r -> {
                    assertThat(r.succeeded(), is(false));
                    assertThat(r.cause().getMessage(), is("Secret my-secret not found in namespace ns"));
                });
    }

    @Test
    void testGetValidateSecretMissingKeys() {
        String namespace = "ns";
        String secretName = "my-secret";

        Secret secret = new SecretBuilder()
                .withNewMetadata()
                .withName(secretName)
                .withNamespace(namespace)
                .endMetadata()
                .withData(Map.of("key1", "value", "key2", "value", "key3", "value"))
                .build();

        SecretOperator secretOps = mock(SecretOperator.class);
        when(secretOps.getAsync(eq(namespace), eq(secretName))).thenReturn(Future.succeededFuture(secret));

        ReconcilerUtils.getValidatedSecret(secretOps, namespace, secretName, "key1", "key4", "key5")
                .onComplete(r -> {
                    assertThat(r.succeeded(), is(false));
                    assertThat(r.cause().getMessage(), is("Items with key(s) [key4, key5] are missing in Secret my-secret"));
                });
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
