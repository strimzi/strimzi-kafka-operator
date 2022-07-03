/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerConfigurationBrokerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.IngressOperator;
import io.strimzi.operator.common.operator.resource.IngressV1Beta1Operator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.RouteOperator;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;
import io.vertx.core.Future;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
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
public class KafkaListenerReconcilerIngressV1Beta1Test {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    public static final String NAMESPACE = "test";
    public static final String NAME = "my-kafka";

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
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

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

        MockKafkaListenersReconciler reconciler = new MockKafkaListenersReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME),
                kafkaCluster,
                new PlatformFeaturesAvailability(false, KubernetesVersion.V1_16),
                supplier.secretOperations,
                supplier.serviceOperations,
                supplier.routeOperations,
                supplier.ingressOperations,
                supplier.ingressV1Beta1Operations
        );

        Checkpoint async = context.checkpoint();
        reconciler.reconcile()
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
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

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

        MockKafkaListenersReconciler reconciler = new MockKafkaListenersReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME),
                kafkaCluster,
                new PlatformFeaturesAvailability(false, KubernetesVersion.V1_22),
                supplier.secretOperations,
                supplier.serviceOperations,
                supplier.routeOperations,
                supplier.ingressOperations,
                supplier.ingressV1Beta1Operations
        );

        Checkpoint async = context.checkpoint();
        reconciler.reconcile()
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
     * Override KafkaListenersReconciler to only run reconciliation steps that concern the Ingress resources feature
     */
    static class MockKafkaListenersReconciler extends KafkaListenersReconciler {
        public MockKafkaListenersReconciler(
                Reconciliation reconciliation,
                KafkaCluster kafka,
                PlatformFeaturesAvailability pfa,
                SecretOperator secretOperator,
                ServiceOperator serviceOperator,
                RouteOperator routeOperator,
                IngressOperator ingressOperator,
                IngressV1Beta1Operator ingressV1Beta1Operator) {
            super(reconciliation, kafka, null, pfa, 300_000L, secretOperator, serviceOperator, routeOperator, ingressOperator, ingressV1Beta1Operator);
        }

        @Override
        public Future<ReconciliationResult> reconcile()  {
            return ingresses()
                    .compose(i -> ingressesV1Beta1())
                    .compose(i -> ingressesReady())
                    .compose(i -> ingressesV1Beta1Ready())
                    .compose(i -> Future.succeededFuture(result));
        }
    }
}