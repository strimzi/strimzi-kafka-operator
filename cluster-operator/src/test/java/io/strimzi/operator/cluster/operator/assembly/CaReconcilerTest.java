/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.api.kafka.model.podset.StrimziPodSetBuilder;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.CertIssuer;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.KafkaClusterSecurityContext;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.operator.resource.KafkaRoller;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.DeploymentOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StrimziPodSetOperator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.auth.Identity;
import io.strimzi.operator.common.ca.Ca;
import io.strimzi.operator.common.ca.CaConfig;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.operator.resource.kubernetes.SecretOperator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for actions taken by CaReconciler after the CA Secrets are reconciled,
 * particularly the rolling updates for trust.
 * The test cases use a mock CaReconciler class to capture when Kafka pods and
 * other deployment (Kafka Exporter etc) are rolled.
 * <p>
 * The tests for creating or updating the CA certs and reconciling the CA cert
 * Secrets are in the test class for each CaProvider. The CaProvider is mocked
 * here, so each test sets the state of the Cluster and Clients CA directly.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
public class CaReconcilerTest {
    private static final String NAMESPACE = "test";
    private static final String NAME = "my-cluster";
    private static final Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withName(NAME)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withNewKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName("plain")
                            .withPort(9092)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(false)
                            .build())
                .endKafka()
            .endSpec()
            .build();

    private static final String CLUSTER_CA_TRUSTED_CERTS = "cluster-ca-trusted-certs";
    private static final String CLIENTS_CA_TRUSTED_CERTS = "clients-ca-trusted-certs";
    // Never parsed by the CaReconciler, so any base64 value will do
    private static final String CURRENT_CA_CRT = Util.encodeToBase64("current-ca-crt");
    private static final String OLD_CA_CRT = Util.encodeToBase64("old-ca-crt");
    private static final String OLD_CA_CRT_ALIAS = "ca-2026-01-01T00-00-00Z.crt";

    private ResourceOperatorSupplier supplier;

    @BeforeEach
    public void setup() {
        supplier = ResourceUtils.supplierWithMocks(false);
    }

    @Test
    public void testKafkaAndDeploymentsRolledWhenClusterCaKeyReplaced() {
        // Replacing the key also renews the cert, so both generations moved on
        Ca clusterCa = mockClusterCa(1, 1, true);
        Ca clientsCa = mockClientsCa(0);
        mockKubernetesState(
                List.of(
                        controllerPodWithCaGenerations("my-cluster-controllers-3", 0, 0),
                        controllerPodWithCaGenerations("my-cluster-controllers-4", 0, 0),
                        controllerPodWithCaGenerations("my-cluster-controllers-5", 0, 0)),
                List.of(
                        brokerPodWithCaGenerations("my-cluster-brokers-0", 0, 0),
                        brokerPodWithCaGenerations("my-cluster-brokers-1", 0, 0),
                        brokerPodWithCaGenerations("my-cluster-brokers-2", 0, 0)));

        when(supplier.strimziPodSetOperator.batchReconcile(any(), eq(NAMESPACE), any(), any(Labels.class)))
                .thenAnswer(i -> CompletableFuture.completedFuture(null));

        MockCaReconciler mockCaReconciler = new MockCaReconciler(supplier, clusterCa, clientsCa);
        mockCaReconciler.reconcile(Clock.systemUTC()).toCompletableFuture().join();

        assertThat("Kafka restart reasons", mockCaReconciler.kafkaRestartReasons, aMapWithSize(6));
        mockCaReconciler.kafkaRestartReasons.forEach((podName, restartReasons) -> {
            assertThat("Restart reasons for pod " + podName, restartReasons.getReasons(), hasSize(1));
            assertThat("Restart reasons for pod " + podName, restartReasons.contains(RestartReason.CLUSTER_CA_CERT_KEY_REPLACED), is(true));
        });

        assertThat("Deployment restart reasons", mockCaReconciler.deploymentRestartReasons, aMapWithSize(3));
        mockCaReconciler.deploymentRestartReasons.forEach((deploymentName, restartReason) ->
                assertThat("Deployment restart reason for " + deploymentName, restartReason, is(RestartReason.CLUSTER_CA_CERT_KEY_REPLACED.getDefaultNote())));

        @SuppressWarnings({ "unchecked" })
        ArgumentCaptor<List<StrimziPodSet>> podSetCaptor = ArgumentCaptor.forClass(List.class);
        verify(supplier.strimziPodSetOperator).batchReconcile(any(), eq(NAMESPACE), podSetCaptor.capture(), any(Labels.class));

        assertThat(podSetCaptor.getValue(), hasSize(2));
        podSetCaptor.getValue().stream().flatMap(podSet -> PodSetUtils.podSetToPods(podSet).stream()).forEach(pod -> {
            // Expect that the CA key generation was updated. CA cert generations are updated by component reconcilers
            assertThat(pod.getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, "1"));
            assertThat(pod.getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0"));
            assertThat(pod.getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0"));
        });

        // The old CA cert has to survive, the pods have not been rolled yet
        verify(clusterCa, never()).maybeDeleteOldCerts();
    }

    // Cluster CA key replaced in a previous reconcile or by the user, and some pods already rolled
    @Test
    public void testKafkaRolledWhenPodsDoNotTrustCurrentClusterCaKey() {
        Ca clusterCa = mockClusterCa(1, 1, false);
        Ca clientsCa = mockClientsCa(0);

        List<Pod> controllerPods = List.of(
                controllerPodWithCaGenerations("my-cluster-controllers-3", 0, 0),
                controllerPodWithCaGenerations("my-cluster-controllers-4", 0, 1),
                controllerPodWithCaGenerations("my-cluster-controllers-5", 0, 0));
        List<Pod> brokerPods = List.of(
                brokerPodWithCaGenerations("my-cluster-brokers-0", 0, 1),
                brokerPodWithCaGenerations("my-cluster-brokers-1", 0, 0),
                brokerPodWithCaGenerations("my-cluster-brokers-2", 0, 0));
        mockKubernetesState(controllerPods, brokerPods);

        when(supplier.strimziPodSetOperator.batchReconcile(any(), eq(NAMESPACE), any(), any(Labels.class)))
                .thenAnswer(i -> CompletableFuture.completedFuture(null));

        MockCaReconciler mockCaReconciler = new MockCaReconciler(supplier, clusterCa, clientsCa);
        mockCaReconciler.reconcile(Clock.systemUTC()).toCompletableFuture().join();

        assertThat("Kafka restart reasons", mockCaReconciler.kafkaRestartReasons, aMapWithSize(6));
        mockCaReconciler.kafkaRestartReasons.forEach((podName, restartReasons) -> {
            if ("my-cluster-controllers-4".equals(podName) || "my-cluster-brokers-0".equals(podName)) {
                assertThat("Pod " + podName + " should not be restarted", restartReasons.getReasons(), empty());
            } else {
                assertThat("Restart reasons for pod " + podName, restartReasons.getReasons(), hasSize(1));
                assertThat("Restart reasons for pod " + podName, restartReasons.contains(RestartReason.CLUSTER_CA_CERT_KEY_REPLACED), is(true));
            }
        });

        assertThat("Deployment restart reasons", mockCaReconciler.deploymentRestartReasons, aMapWithSize(3));
        mockCaReconciler.deploymentRestartReasons.forEach((deploymentName, restartReason) ->
                assertThat("Deployment restart reason for " + deploymentName, restartReason, is(RestartReason.CLUSTER_CA_CERT_KEY_REPLACED.getDefaultNote())));

        @SuppressWarnings({ "unchecked" })
        ArgumentCaptor<List<StrimziPodSet>> podSetCaptor = ArgumentCaptor.forClass(List.class);
        verify(supplier.strimziPodSetOperator).batchReconcile(any(), eq(NAMESPACE), podSetCaptor.capture(), any(Labels.class));

        assertThat(podSetCaptor.getValue(), hasSize(2));
        podSetCaptor.getValue().stream().flatMap(podSet -> PodSetUtils.podSetToPods(podSet).stream()).forEach(pod -> {
            assertThat(pod.getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, "1"));
            assertThat(pod.getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0"));
            assertThat(pod.getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0"));
        });

        verify(clusterCa, never()).maybeDeleteOldCerts();
    }

    @Test
    public void testNothingRolledWhenClusterCaUnchanged() {
        Ca clusterCa = mockClusterCa(0, 0, false);
        Ca clientsCa = mockClientsCa(0);
        mockKubernetesState(
                List.of(
                        controllerPodWithCaGenerations("my-cluster-controllers-3", 0, 0),
                        controllerPodWithCaGenerations("my-cluster-controllers-4", 0, 0),
                        controllerPodWithCaGenerations("my-cluster-controllers-5", 0, 0)),
                List.of(
                        brokerPodWithCaGenerations("my-cluster-brokers-0", 0, 0),
                        brokerPodWithCaGenerations("my-cluster-brokers-1", 0, 0),
                        brokerPodWithCaGenerations("my-cluster-brokers-2", 0, 0)));

        MockCaReconciler mockCaReconciler = new MockCaReconciler(supplier, clusterCa, clientsCa);
        CaReconciler.CaReconciliationResult result = mockCaReconciler.reconcile(Clock.systemUTC()).toCompletableFuture().join();

        assertThat("Kafka restart reasons", mockCaReconciler.kafkaRestartReasons, anEmptyMap());
        assertThat("Deployment restart reasons", mockCaReconciler.deploymentRestartReasons, anEmptyMap());
        verify(supplier.strimziPodSetOperator, never()).batchReconcile(any(), any(), any(), any(Labels.class));

        assertThat(result.clusterCa(), is(clusterCa));
        assertThat(result.clientsCa(), is(clientsCa));
    }

    @Test
    public void testNothingRolledWhenOnlyClusterCaCertChanged() {
        Ca clusterCa = mockClusterCa(1, 0, false);
        Ca clientsCa = mockClientsCa(0);
        mockKubernetesState(
                List.of(
                        controllerPodWithCaGenerations("my-cluster-controllers-3", 0, 0),
                        controllerPodWithCaGenerations("my-cluster-controllers-4", 0, 0),
                        controllerPodWithCaGenerations("my-cluster-controllers-5", 0, 0)),
                List.of(
                        brokerPodWithCaGenerations("my-cluster-brokers-0", 0, 0),
                        brokerPodWithCaGenerations("my-cluster-brokers-1", 0, 0),
                        brokerPodWithCaGenerations("my-cluster-brokers-2", 0, 0)));

        MockCaReconciler mockCaReconciler = new MockCaReconciler(supplier, clusterCa, clientsCa);
        mockCaReconciler.reconcile(Clock.systemUTC()).toCompletableFuture().join();

        assertThat("Kafka restart reasons", mockCaReconciler.kafkaRestartReasons, anEmptyMap());
        assertThat("Deployment restart reasons", mockCaReconciler.deploymentRestartReasons, anEmptyMap());
    }

    // We rely on KafkaReconciler to roll pods for ClientsCa renewal
    @Test
    public void testNothingRolledWhenOnlyClientsCaChanged() {
        Ca clusterCa = mockClusterCa(0, 0, false);
        Ca clientsCa = mockClientsCa(1);
        mockKubernetesState(
                List.of(
                        controllerPodWithCaGenerations("my-cluster-controllers-3", 0, 0),
                        controllerPodWithCaGenerations("my-cluster-controllers-4", 0, 0),
                        controllerPodWithCaGenerations("my-cluster-controllers-5", 0, 0)),
                List.of(
                        brokerPodWithCaGenerations("my-cluster-brokers-0", 0, 0),
                        brokerPodWithCaGenerations("my-cluster-brokers-1", 0, 0),
                        brokerPodWithCaGenerations("my-cluster-brokers-2", 0, 0)));

        MockCaReconciler mockCaReconciler = new MockCaReconciler(supplier, clusterCa, clientsCa);
        mockCaReconciler.reconcile(Clock.systemUTC()).toCompletableFuture().join();

        assertThat("Kafka restart reasons", mockCaReconciler.kafkaRestartReasons, anEmptyMap());
        assertThat("Deployment restart reasons", mockCaReconciler.deploymentRestartReasons, anEmptyMap());
    }

    @Test
    public void testTerminatingPodsIgnoredWhenCheckingClusterCaTrust() {
        Ca clusterCa = mockClusterCa(1, 1, false);
        when(clusterCa.certsRemoved()).thenReturn(true);
        Ca clientsCa = mockClientsCa(0);

        // The terminating pod is the only one behind, every live pod is up to date
        Pod terminatingPod = new PodBuilder(brokerPodWithCaGenerations("my-cluster-brokers-2", 0, 0))
                .editMetadata()
                    .withDeletionTimestamp("2026-01-01T00:00:00Z")
                .endMetadata()
                .build();
        List<Pod> brokerPods = List.of(
                brokerPodWithCaGenerations("my-cluster-brokers-0", 1, 1),
                brokerPodWithCaGenerations("my-cluster-brokers-1", 1, 1),
                terminatingPod);
        mockKubernetesState(
                List.of(
                        controllerPodWithCaGenerations("my-cluster-controllers-3", 1, 1),
                        controllerPodWithCaGenerations("my-cluster-controllers-4", 1, 1),
                        controllerPodWithCaGenerations("my-cluster-controllers-5", 1, 1)),
                brokerPods);

        MockCaReconciler mockCaReconciler = new MockCaReconciler(supplier, clusterCa, clientsCa);
        mockCaReconciler.reconcile(Clock.systemUTC()).toCompletableFuture().join();

        // Its old key generation is ignored, so nothing is rolled
        assertThat("Kafka restart reasons", mockCaReconciler.kafkaRestartReasons, anEmptyMap());
        assertThat("Deployment restart reasons", mockCaReconciler.deploymentRestartReasons, anEmptyMap());

        // Its old cert generation is ignored too, so the old cert counts as unused
        verify(clusterCa).maybeDeleteOldCerts();
        verify(supplier.secretOperations).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), any());
    }

    @Test
    public void testTrustBundleSecretReconciled() {
        Ca clusterCa = mockClusterCa(1, 0, false);
        Ca clientsCa = mockClientsCa(2);
        mockKubernetesState(
                List.of(
                        controllerPodWithCaGenerations("my-cluster-controllers-3", 1, 0),
                        controllerPodWithCaGenerations("my-cluster-controllers-4", 1, 0),
                        controllerPodWithCaGenerations("my-cluster-controllers-5", 1, 0)),
                List.of(
                        brokerPodWithCaGenerations("my-cluster-brokers-0", 1, 0),
                        brokerPodWithCaGenerations("my-cluster-brokers-1", 1, 0),
                        brokerPodWithCaGenerations("my-cluster-brokers-2", 1, 0)));

        new MockCaReconciler(supplier, clusterCa, clientsCa).reconcile(Clock.systemUTC()).toCompletableFuture().join();

        ArgumentCaptor<Secret> trustBundle = ArgumentCaptor.forClass(Secret.class);
        verify(supplier.secretOperations).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.trustBundleSecretName(NAME)), trustBundle.capture());

        assertThat(trustBundle.getValue().getData(), aMapWithSize(2));
        assertThat(trustBundle.getValue().getData(), hasEntry("cluster-ca.crt", Util.encodeToBase64(CLUSTER_CA_TRUSTED_CERTS)));
        assertThat(trustBundle.getValue().getData(), hasEntry("clients-ca.crt", Util.encodeToBase64(CLIENTS_CA_TRUSTED_CERTS)));
        assertThat(trustBundle.getValue().getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "1"));
        assertThat(trustBundle.getValue().getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "2"));
    }

    @Test
    public void testClusterOperatorSecretCreatedWhenMissing() {
        Ca clusterCa = mockClusterCa(1, 0, false);
        Ca clientsCa = mockClientsCa(0);
        mockKubernetesState(
                List.of(
                        controllerPodWithCaGenerations("my-cluster-controllers-3", 1, 0),
                        controllerPodWithCaGenerations("my-cluster-controllers-4", 1, 0),
                        controllerPodWithCaGenerations("my-cluster-controllers-5", 1, 0)),
                List.of(
                        brokerPodWithCaGenerations("my-cluster-brokers-0", 1, 0),
                        brokerPodWithCaGenerations("my-cluster-brokers-1", 1, 0),
                        brokerPodWithCaGenerations("my-cluster-brokers-2", 1, 0)));

        new MockCaReconciler(supplier, clusterCa, clientsCa).reconcile(Clock.systemUTC()).toCompletableFuture().join();

        verify(clusterCa).maybeCopyOrGenerateClientCert(any(), eq("cluster-operator"), isNull(), anyBoolean());

        ArgumentCaptor<Secret> coSecret = ArgumentCaptor.forClass(Secret.class);
        verify(supplier.secretOperations).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clusterOperatorCertsSecretName(NAME)), coSecret.capture());

        assertThat(coSecret.getValue().getData(), aMapWithSize(2));
        assertThat(coSecret.getValue().getData(), hasKey("cluster-operator.crt"));
        assertThat(coSecret.getValue().getData(), hasKey("cluster-operator.key"));
        assertThat(coSecret.getValue().getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "1"));
    }

    @Test
    public void testClusterOperatorSecretRenewedWhenClusterCaFullyTrusted() {
        Ca clusterCa = mockClusterCa(2, 0, false);
        Ca clientsCa = mockClientsCa(0);
        Secret existingCoSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName(KafkaResources.clusterOperatorCertsSecretName(NAME))
                    .withNamespace(NAMESPACE)
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "1")
                .endMetadata()
                .withData(Map.of(
                        "cluster-operator.crt", Util.encodeToBase64("old-cert"),
                        "cluster-operator.key", Util.encodeToBase64("old-key")))
                .build();

        mockKubernetesState(existingCoSecret,
                List.of(
                        controllerPodWithCaGenerations("my-cluster-controllers-3", 2, 0),
                        controllerPodWithCaGenerations("my-cluster-controllers-4", 2, 0),
                        controllerPodWithCaGenerations("my-cluster-controllers-5", 2, 0)),
                List.of(
                        brokerPodWithCaGenerations("my-cluster-brokers-0", 2, 0),
                        brokerPodWithCaGenerations("my-cluster-brokers-1", 2, 0),
                        brokerPodWithCaGenerations("my-cluster-brokers-2", 2, 0)));

        new MockCaReconciler(supplier, clusterCa, clientsCa).reconcile(Clock.systemUTC()).toCompletableFuture().join();

        ArgumentCaptor<CertAndKey> oldCertAndKey = ArgumentCaptor.forClass(CertAndKey.class);
        verify(clusterCa).maybeCopyOrGenerateClientCert(any(), eq("cluster-operator"), oldCertAndKey.capture(), anyBoolean());

        assertThat(oldCertAndKey.getValue().certAsBase64String(), is(Util.encodeToBase64("old-cert")));
        assertThat(oldCertAndKey.getValue().keyAsBase64String(), is(Util.encodeToBase64("old-key")));
        assertThat(oldCertAndKey.getValue().caCertGeneration(), is(1));

        ArgumentCaptor<Secret> coSecret = ArgumentCaptor.forClass(Secret.class);
        verify(supplier.secretOperations).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clusterOperatorCertsSecretName(NAME)), coSecret.capture());
        assertThat(coSecret.getValue().getMetadata().getAnnotations(), hasEntry(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "2"));
        assertThat(coSecret.getValue().getData(), hasEntry("cluster-operator.crt", Util.encodeToBase64("new-cert")));
    }

    // The Cluster Operator keeps its old cert until every pod trusts the new key
    @Test
    public void testClusterOperatorSecretNotUpdatedWhenClusterCaNotFullyTrusted() {
        Ca clusterCa = mockClusterCa(1, 1, false);
        Ca clientsCa = mockClientsCa(0);
        Secret existingCoSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName(KafkaResources.clusterOperatorCertsSecretName(NAME))
                    .withNamespace(NAMESPACE)
                    .addToAnnotations(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, "0")
                .endMetadata()
                .withData(Map.of(
                        "cluster-operator.crt", Util.encodeToBase64("old-cert"),
                        "cluster-operator.key", Util.encodeToBase64("old-key")))
                .build();

        mockKubernetesState(existingCoSecret,
                List.of(
                        controllerPodWithCaGenerations("my-cluster-controllers-3", 0, 0),
                        controllerPodWithCaGenerations("my-cluster-controllers-4", 0, 0),
                        controllerPodWithCaGenerations("my-cluster-controllers-5", 0, 0)),
                List.of(
                        brokerPodWithCaGenerations("my-cluster-brokers-0", 0, 0),
                        brokerPodWithCaGenerations("my-cluster-brokers-1", 0, 0),
                        brokerPodWithCaGenerations("my-cluster-brokers-2", 0, 0)));
        when(supplier.strimziPodSetOperator.batchReconcile(any(), eq(NAMESPACE), any(), any(Labels.class)))
                .thenAnswer(i -> CompletableFuture.completedFuture(null));

        new MockCaReconciler(supplier, clusterCa, clientsCa).reconcile(Clock.systemUTC()).toCompletableFuture().join();

        verify(clusterCa, never()).maybeCopyOrGenerateClientCert(any(), any(), any(), anyBoolean());
        verify(supplier.secretOperations, never()).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clusterOperatorCertsSecretName(NAME)), any());
    }

    @Test
    public void testOldClusterCaCertsRemovedWhenNewCertFullyUsed() {
        // The key was replaced in a previous reconcile and every pod has been rolled since
        Ca clusterCa = mockClusterCa(1, 1, false);
        when(clusterCa.certsRemoved()).thenReturn(true);
        Ca clientsCa = mockClientsCa(0);
        mockKubernetesState(
                List.of(
                        controllerPodWithCaGenerations("my-cluster-controllers-3", 1, 1),
                        controllerPodWithCaGenerations("my-cluster-controllers-4", 1, 1),
                        controllerPodWithCaGenerations("my-cluster-controllers-5", 1, 1)),
                List.of(
                        brokerPodWithCaGenerations("my-cluster-brokers-0", 1, 1),
                        brokerPodWithCaGenerations("my-cluster-brokers-1", 1, 1),
                        brokerPodWithCaGenerations("my-cluster-brokers-2", 1, 1)));

        new MockCaReconciler(supplier, clusterCa, clientsCa).reconcile(Clock.systemUTC()).toCompletableFuture().join();

        verify(clusterCa).maybeDeleteOldCerts();

        ArgumentCaptor<Secret> clusterCaCert = ArgumentCaptor.forClass(Secret.class);
        verify(supplier.secretOperations).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), clusterCaCert.capture());

        assertThat(clusterCaCert.getValue().getData(), aMapWithSize(1));
        assertThat(clusterCaCert.getValue().getData(), hasEntry(Ca.CA_CRT, CURRENT_CA_CRT));
        assertThat(clusterCaCert.getValue().getData(), not(hasKey(OLD_CA_CRT_ALIAS)));

        // Clients CA cert doesn't have old CA certs removed automatically
        verify(supplier.secretOperations, never()).reconcile(any(), eq(NAMESPACE), eq(KafkaResources.clientsCaCertificateSecretName(NAME)), any());
    }

    @Test
    public void testOldClusterCaCertsKeptWhileOldCertStillUsed() {
        Ca clusterCa = mockClusterCa(1, 0, false);
        Ca clientsCa = mockClientsCa(0);
        // One pod still presents a certificate signed by the previous Cluster CA cert
        List<Pod> brokerPods = List.of(
                        brokerPodWithCaGenerations("my-cluster-brokers-0", 1, 0),
                        brokerPodWithCaGenerations("my-cluster-brokers-1", 1, 0),
                        brokerPodWithCaGenerations("my-cluster-brokers-2", 0, 0));
        mockKubernetesState(
                List.of(
                        controllerPodWithCaGenerations("my-cluster-controllers-3", 1, 0),
                        controllerPodWithCaGenerations("my-cluster-controllers-4", 1, 0),
                        controllerPodWithCaGenerations("my-cluster-controllers-5", 1, 0)),
                brokerPods);

        new MockCaReconciler(supplier, clusterCa, clientsCa).reconcile(Clock.systemUTC()).toCompletableFuture().join();

        verify(clusterCa, never()).maybeDeleteOldCerts();
        verify(supplier.secretOperations, never()).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), any());
    }

    @Test
    public void testOldClusterCaCertsKeptWhenThereAreNoPods() {
        // The key was replaced in a previous reconcile, so there is an old cert, but no Pod can prove it is unused
        Ca clusterCa = mockClusterCa(1, 1, false);
        Ca clientsCa = mockClientsCa(0);
        // still no Pods, a new Kafka cluster is under creation
        mockKubernetesState(List.of(), List.of());

        new MockCaReconciler(supplier, clusterCa, clientsCa).reconcile(Clock.systemUTC()).toCompletableFuture().join();

        verify(clusterCa, never()).maybeDeleteOldCerts();
        verify(supplier.secretOperations, never()).reconcile(any(), eq(NAMESPACE), eq(AbstractModel.clusterCaCertSecretName(NAME)), any());
    }

    private static Ca mockClusterCa(int caCertGeneration, int caKeyGeneration, boolean keyReplaced) {
        Ca clusterCa = mock(Ca.class);
        when(clusterCa.caCertGeneration()).thenReturn(caCertGeneration);
        when(clusterCa.caKeyGeneration()).thenReturn(caKeyGeneration);
        when(clusterCa.keyReplaced()).thenReturn(keyReplaced);
        when(clusterCa.caCertGenerationAnnotation()).thenReturn(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION);
        when(clusterCa.trustedCaCerts()).thenReturn(CLUSTER_CA_TRUSTED_CERTS);
        // The CA cert data as it is after any old certificates were deleted
        when(clusterCa.caCertData()).thenReturn(Map.of(Ca.CA_CRT, CURRENT_CA_CRT));
        when(clusterCa.maybeCopyOrGenerateClientCert(any(), any(), any(), anyBoolean()))
                .thenReturn(CompletableFuture.completedStage(new CertAndKey(
                        "new-key".getBytes(StandardCharsets.US_ASCII),
                        "new-cert".getBytes(StandardCharsets.US_ASCII),
                        caCertGeneration)));
        return clusterCa;
    }

    private static Ca mockClientsCa(int caCertGeneration) {
        Ca clientsCa = mock(Ca.class);
        when(clientsCa.caCertGeneration()).thenReturn(caCertGeneration);
        when(clientsCa.caCertGenerationAnnotation()).thenReturn(Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION);
        when(clientsCa.trustedCaCerts()).thenReturn(CLIENTS_CA_TRUSTED_CERTS);
        return clientsCa;
    }

    private void mockKubernetesState(List<Pod> controllerPods, List<Pod> brokerPods) {
        mockKubernetesState(null, controllerPods, brokerPods);
    }

    private void mockKubernetesState(Secret existingClusterOperatorSecret, List<Pod> controllerPods, List<Pod> brokerPods) {
        List<Secret> secrets = existingClusterOperatorSecret == null ? List.of() : List.of(existingClusterOperatorSecret);

        SecretOperator secretOps = supplier.secretOperations;
        when(secretOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(CompletableFuture.completedFuture(secrets));
        when(secretOps.reconcile(any(), eq(NAMESPACE), any(), any(Secret.class))).thenReturn(CompletableFuture.completedFuture(null));

        PodOperator mockPodOps = supplier.podOperations;
        List<Pod> pods = new ArrayList<>(controllerPods);
        pods.addAll(brokerPods);
        when(mockPodOps.listAsync(any(), any(Labels.class))).thenReturn(CompletableFuture.completedFuture(pods));

        StrimziPodSetOperator spsOps = supplier.strimziPodSetOperator;
        when(spsOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(CompletableFuture.completedFuture(
                List.of(new StrimziPodSetBuilder()
                                .withNewMetadata().withName(NAME + "-controller").endMetadata()
                                .withNewSpec().withPods(PodSetUtils.podsToMaps(controllerPods)).endSpec()
                                .build(),
                        new StrimziPodSetBuilder()
                                .withNewMetadata().withName(NAME + "-broker").endMetadata()
                                .withNewSpec().withPods(PodSetUtils.podsToMaps(brokerPods)).endSpec()
                                .build())));

        Map<String, Deployment> deps = new HashMap<>();
        deps.put("my-cluster-entity-operator", deploymentWithName("my-cluster-entity-operator"));
        deps.put("my-cluster-cruise-control", deploymentWithName("my-cluster-cruise-control"));
        deps.put("my-cluster-kafka-exporter", deploymentWithName("my-cluster-kafka-exporter"));
        DeploymentOperator depsOperator = supplier.deploymentOperations;
        when(depsOperator.getAsync(any(), any())).thenAnswer(i -> CompletableFuture.completedFuture(deps.get(i.getArgument(1, String.class))));
    }

    static class MockCaReconciler extends CaReconciler {
        private final Ca clusterCa;
        private final Ca clientsCa;
        private final Secret providedClusterCaCertSecret = caCertSecret(AbstractModel.clusterCaCertSecretName(NAME));
        private final Secret providedClientsCaCertSecret = caCertSecret(KafkaResources.clientsCaCertificateSecretName(NAME));

        Map<String, RestartReasons> kafkaRestartReasons = new HashMap<>();
        Map<String, String> deploymentRestartReasons = new HashMap<>();

        MockCaReconciler(ResourceOperatorSupplier supplier, Ca clusterCa, Ca clientsCa) {
            super(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME),
                    KAFKA,
                    new ClusterOperatorConfig.ClusterOperatorConfigBuilder(ResourceUtils.dummyClusterOperatorConfig(), KafkaVersionTestUtils.getKafkaVersionLookup()).with(ClusterOperatorConfig.OPERATION_TIMEOUT_MS.key(), "1").build(),
                    supplier,
                    mock(CertIssuer.class),
                    mock(PasswordGenerator.class),
                    KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT
            );
            this.clusterCa = clusterCa;
            this.clientsCa = clientsCa;
        }

        @Override
        CaProvider createCaProvider(Ca.CaRole caRole, CaConfig caConfig, Secret existingCaCertSecret, Secret coSecret, Secret existingCaKeySecret, Clock clock) {
            CaProviderResult result = caRole == Ca.CaRole.CLUSTER_CA
                    ? new CaProviderResult(clusterCa, providedClusterCaCertSecret)
                    : new CaProviderResult(clientsCa, providedClientsCaCertSecret);

            CaProvider mockCaProvider = mock(CaProvider.class);
            when(mockCaProvider.createAndReconcileCa()).thenReturn(CompletableFuture.completedStage(result));
            return mockCaProvider;
        }

        @Override
        KafkaRoller createKafkaRoller(Set<NodeRef> nodes, Identity coIdentity) {
            KafkaRoller mockKafkaRoller = mock(KafkaRoller.class);
            when(mockKafkaRoller.rollingRestart(any())).thenAnswer(i -> podOperator.listAsync(NAMESPACE, Labels.EMPTY)
                    .thenAccept(pods -> kafkaRestartReasons = pods.stream().collect(Collectors.toMap(
                            pod -> pod.getMetadata().getName(),
                            pod -> (RestartReasons) i.getArgument(0, Function.class).apply(pod))))
                    .thenApply(v -> (Void) null));
            return mockKafkaRoller;
        }

        @Override
        CompletionStage<Void> rollDeploymentIfExists(String deploymentName, RestartReason reason) {
            return deploymentOperator.getAsync(reconciliation.namespace(), deploymentName)
                    .thenApply(dep -> {
                        if (dep != null) {
                            this.deploymentRestartReasons.put(deploymentName, reason.getDefaultNote());
                        }
                        return null;
                    });
        }
    }



    private static Pod brokerPodWithCaGenerations(String podName, int caCertGeneration, int caKeyGeneration) {
        return podWithCaGenerations(podName, true, false, caCertGeneration, caKeyGeneration);
    }

    private static Pod controllerPodWithCaGenerations(String podName, int caCertGeneration, int caKeyGeneration) {
        return podWithCaGenerations(podName, false, true, caCertGeneration, caKeyGeneration);
    }

    private static Pod podWithCaGenerations(String name, boolean broker, boolean controller, int clusterCaCertGeneration, int clusterCaKeyGeneration) {
        return new PodBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withAnnotations(Map.of(
                            Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(clusterCaCertGeneration),
                            Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, String.valueOf(clusterCaKeyGeneration),
                            Ca.ANNO_STRIMZI_IO_CLIENTS_CA_CERT_GENERATION, "0"))
                    .withLabels(Map.of(
                            Labels.STRIMZI_CLUSTER_LABEL, NAME,
                            Labels.STRIMZI_CONTROLLER_ROLE_LABEL, Boolean.toString(controller),
                            Labels.STRIMZI_BROKER_ROLE_LABEL, Boolean.toString(broker)
                            ))
                .endMetadata()
                .build();
    }



    private static Deployment deploymentWithName(String name) {
        return new DeploymentBuilder()
                .withNewMetadata()
                    .withName(name)
                .endMetadata()
                .build();
    }

    // The CA cert Secret from the mocked CaProvider, with an old cert next to the current one
    private static Secret caCertSecret(String name) {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withData(Map.of(Ca.CA_CRT, CURRENT_CA_CRT, OLD_CA_CRT_ALIAS, OLD_CA_CRT))
                .build();
    }

}
