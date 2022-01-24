/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.StrimziPodSetList;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerConfigurationBrokerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.certs.CertManager;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.StatefulSetOperator;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.IngressOperator;
import io.strimzi.operator.common.operator.resource.IngressV1Beta1Operator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

import static java.util.Collections.emptyList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaAssemblyOperatorIngressKafkaListenerTest {

    public static final String NAMESPACE = "test";
    public static final String NAME = "my-kafka";
    private static Vertx vertx;
    private final OpenSslCertManager certManager = new OpenSslCertManager();
    private final PasswordGenerator passwordGenerator = new PasswordGenerator(12,
            "abcdefghijklmnopqrstuvwxyz" +
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
            "abcdefghijklmnopqrstuvwxyz" +
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
                    "0123456789");

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    @Test
    public void testIngressV1Beta1(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName(NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("ingress")
                                .withPort(9094)
                                .withTls(true)
                                .withType(KafkaListenerType.INGRESS)
                                .withNewConfiguration()
                                    .withNewBootstrap()
                                        .withHost("bootstrap.mydomain.tld")
                                    .endBootstrap()
                                    .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder().withBroker(0).withHost("broker-0.mydomain.tld").build(),
                                            new GenericKafkaListenerConfigurationBrokerBuilder().withBroker(1).withHost("broker-1.mydomain.tld").build(),
                                            new GenericKafkaListenerConfigurationBrokerBuilder().withBroker(2).withHost("broker-2.mydomain.tld").build())
                                .endConfiguration()
                                .build())
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                    .withNewEntityOperator()
                        .withNewUserOperator()
                        .endUserOperator()
                        .withNewTopicOperator()
                        .endTopicOperator()
                    .endEntityOperator()
                .endSpec()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the CRD Operator for Kafka resources
        CrdOperator mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture(kafka));
        when(mockKafkaOps.get(eq(NAMESPACE), eq(NAME))).thenReturn(kafka);
        when(mockKafkaOps.updateStatusAsync(any(), any(Kafka.class))).thenReturn(Future.succeededFuture());

        // Mock the KafkaSet operations
        StatefulSetOperator mockStsOps = supplier.stsOperations;
        when(mockStsOps.getAsync(eq(NAMESPACE), eq(KafkaCluster.kafkaClusterName(NAME)))).thenReturn(Future.succeededFuture());

        // Mock the StrimziPodSet operator
        CrdOperator<KubernetesClient, StrimziPodSet, StrimziPodSetList> mockPodSetOps = supplier.strimziPodSetOperator;
        when(mockPodSetOps.getAsync(any(), any())).thenReturn(Future.succeededFuture(null));

        // Mock the Pod operations
        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(emptyList()));

        // Mock ingress v1beta1 ops
        IngressV1Beta1Operator mockIngressV1Beta1ops = supplier.ingressV1Beta1Operations;
        ArgumentCaptor<io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress> ingressV1Beta1Captor = ArgumentCaptor.forClass(io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress.class);
        when(mockIngressV1Beta1ops.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockIngressV1Beta1ops.reconcile(any(), anyString(), anyString(), ingressV1Beta1Captor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress())));
        when(mockIngressV1Beta1ops.hasIngressAddress(any(), eq(NAMESPACE), any(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        // Mock ingress v1 ops
        IngressOperator mockIngressOps = supplier.ingressOperations;
        ArgumentCaptor<Ingress> ingressCaptor = ArgumentCaptor.forClass(Ingress.class);
        when(mockIngressOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockIngressOps.reconcile(any(), anyString(), anyString(), ingressCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new Ingress())));
        when(mockIngressOps.hasIngressAddress(any(), eq(NAMESPACE), any(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        KafkaAssemblyOperator op = new MockKafkaAssemblyOperatorForIngressTests(vertx, new PlatformFeaturesAvailability(false, KubernetesVersion.V1_16), certManager, passwordGenerator,
                supplier, ResourceUtils.dummyClusterOperatorConfig(KafkaVersionTestUtils.getKafkaVersionLookup()));
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);

        Checkpoint async = context.checkpoint();
        op.reconcile(reconciliation)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(ingressCaptor.getAllValues().size(), is(0));
                    assertThat(ingressV1Beta1Captor.getAllValues().size(), is(4));

                    verify(mockIngressOps, never()).list(any(), any());
                    verify(mockIngressOps, never()).reconcile(any(), any(), any(), any());
                    verify(mockIngressOps, never()).hasIngressAddress(any(), any(), any(), anyLong(), anyLong());

                    async.flag();
                })));
    }

    @Test
    public void testIngressV1(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName(NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("ingress")
                                .withPort(9094)
                                .withTls(true)
                                .withType(KafkaListenerType.INGRESS)
                                .withNewConfiguration()
                                    .withNewBootstrap()
                                        .withHost("bootstrap.mydomain.tld")
                                    .endBootstrap()
                                    .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder().withBroker(0).withHost("broker-0.mydomain.tld").build(),
                                            new GenericKafkaListenerConfigurationBrokerBuilder().withBroker(1).withHost("broker-1.mydomain.tld").build(),
                                            new GenericKafkaListenerConfigurationBrokerBuilder().withBroker(2).withHost("broker-2.mydomain.tld").build())
                                .endConfiguration()
                                .build())
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                    .withNewEntityOperator()
                        .withNewUserOperator()
                        .endUserOperator()
                        .withNewTopicOperator()
                        .endTopicOperator()
                    .endEntityOperator()
                .endSpec()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the CRD Operator for Kafka resources
        CrdOperator mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture(kafka));
        when(mockKafkaOps.get(eq(NAMESPACE), eq(NAME))).thenReturn(kafka);
        when(mockKafkaOps.updateStatusAsync(any(), any(Kafka.class))).thenReturn(Future.succeededFuture());

        // Mock the KafkaSet operations
        StatefulSetOperator mockStsOps = supplier.stsOperations;
        when(mockStsOps.getAsync(eq(NAMESPACE), eq(KafkaCluster.kafkaClusterName(NAME)))).thenReturn(Future.succeededFuture());

        // Mock the StrimziPodSet operator
        CrdOperator<KubernetesClient, StrimziPodSet, StrimziPodSetList> mockPodSetOps = supplier.strimziPodSetOperator;
        when(mockPodSetOps.getAsync(any(), any())).thenReturn(Future.succeededFuture(null));

        // Mock the Pod operations
        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(emptyList()));

        // Mock ingress v1beta1 ops
        IngressV1Beta1Operator mockIngressV1Beta1ops = supplier.ingressV1Beta1Operations;
        ArgumentCaptor<io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress> ingressV1Beta1Captor = ArgumentCaptor.forClass(io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress.class);
        when(mockIngressV1Beta1ops.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockIngressV1Beta1ops.reconcile(any(), anyString(), anyString(), ingressV1Beta1Captor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress())));
        when(mockIngressV1Beta1ops.hasIngressAddress(any(), eq(NAMESPACE), any(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        // Mock ingress v1 ops
        IngressOperator mockIngressOps = supplier.ingressOperations;
        ArgumentCaptor<Ingress> ingressCaptor = ArgumentCaptor.forClass(Ingress.class);
        when(mockIngressOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(emptyList()));
        when(mockIngressOps.reconcile(any(), anyString(), anyString(), ingressCaptor.capture())).thenReturn(Future.succeededFuture(ReconcileResult.created(new Ingress())));
        when(mockIngressOps.hasIngressAddress(any(), eq(NAMESPACE), any(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        KafkaAssemblyOperator op = new MockKafkaAssemblyOperatorForIngressTests(vertx, new PlatformFeaturesAvailability(false, KubernetesVersion.V1_19), certManager, passwordGenerator,
                supplier, ResourceUtils.dummyClusterOperatorConfig(KafkaVersionTestUtils.getKafkaVersionLookup()));
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);

        Checkpoint async = context.checkpoint();
        op.reconcile(reconciliation)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(ingressCaptor.getAllValues().size(), is(4));
                    assertThat(ingressV1Beta1Captor.getAllValues().size(), is(0));

                    verify(mockIngressV1Beta1ops, never()).list(any(), any());
                    verify(mockIngressV1Beta1ops, never()).reconcile(any(), any(), any(), any());
                    verify(mockIngressV1Beta1ops, never()).hasIngressAddress(any(), any(), any(), anyLong(), anyLong());

                    async.flag();
                })));
    }

    /**
     * Override KafkaAssemblyOperator to only run reconciliation steps that concern the Ingress resources feature
     */
    class MockKafkaAssemblyOperatorForIngressTests extends KafkaAssemblyOperator {
        public MockKafkaAssemblyOperatorForIngressTests(
                Vertx vertx,
                PlatformFeaturesAvailability pfa,
                CertManager certManager,
                PasswordGenerator passwordGenerator,
                ResourceOperatorSupplier supplier,
                ClusterOperatorConfig config
        ) {
            super(vertx, pfa, certManager, passwordGenerator, supplier, config);
        }

        @Override
        Future<Void> reconcile(ReconciliationState reconcileState)  {
            return reconcileState.getKafkaClusterDescription()
                    .compose(state -> state.kafkaIngresses())
                    .compose(state -> state.kafkaIngressesV1Beta1())
                    .compose(state -> state.kafkaIngressesReady())
                    .compose(state -> state.kafkaIngressesV1Beta1Ready())
                    .map((Void) null);
        }
    }
}