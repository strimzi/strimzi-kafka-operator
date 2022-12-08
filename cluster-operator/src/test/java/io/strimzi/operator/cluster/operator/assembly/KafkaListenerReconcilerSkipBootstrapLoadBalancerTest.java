/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Service;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.ListenerStatus;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.IngressOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.RouteOperator;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;
import io.strimzi.platform.KubernetesVersion;
import io.vertx.core.Future;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaListenerReconcilerSkipBootstrapLoadBalancerTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    public static final String NAMESPACE = "test";
    public static final String CLUSTER_NAME = "my-kafka";
    public static final String DNS_NAME_FOR_BROKER_0 = "broker-0.test.dns.name";
    public static final String DNS_NAME_FOR_BROKER_1 = "broker-1.test.dns.name";
    public static final String DNS_NAME_FOR_BROKER_2 = "broker-2.test.dns.name";
    public static final String DNS_NAME_FOR_BOOTSTRAP_SERVICE = "bootstrap-broker.test.dns.name";
    public static final int LISTENER_PORT = 9094;

    @Test
    public void testLoadBalancerSkipBootstrapService(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(LISTENER_PORT)
                                .withTls(true)
                                .withType(KafkaListenerType.LOADBALANCER)
                                .withNewConfiguration()
                                    .withCreateBootstrapService(false)
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

        ResourceOperatorSupplier supplier = prepareResourceOperatorSupplier();

        MockKafkaListenersReconciler reconciler = new MockKafkaListenersReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                kafkaCluster,
                new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier.secretOperations,
                supplier.serviceOperations,
                supplier.routeOperations,
                supplier.ingressOperations
        );

        Checkpoint async = context.checkpoint();
        reconciler.reconcile()
                .onComplete(context.succeeding(res -> context.verify(() -> {
                    // Check status
                    assertThat(res.listenerStatuses.size(), is(1));
                    ListenerStatus listenerStatus = res.listenerStatuses.get(0);
                    assertThat(listenerStatus.getBootstrapServers(), is("broker-0.test.dns.name:9094,broker-1.test.dns.name:9094,broker-2.test.dns.name:9094"));
                    assertThat(listenerStatus.getAddresses().size(), is(3));
                    assertThat(listenerStatus.getAddresses().get(0).getHost(), is(DNS_NAME_FOR_BROKER_0));
                    assertThat(listenerStatus.getAddresses().get(1).getHost(), is(DNS_NAME_FOR_BROKER_1));
                    assertThat(listenerStatus.getAddresses().get(2).getHost(), is(DNS_NAME_FOR_BROKER_2));
                    assertThat(listenerStatus.getAddresses().get(0).getPort(), is(LISTENER_PORT));
                    assertThat(listenerStatus.getAddresses().get(1).getPort(), is(LISTENER_PORT));
                    assertThat(listenerStatus.getAddresses().get(2).getPort(), is(LISTENER_PORT));

                    // Check creation of services
                    verify(supplier.serviceOperations, never()).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-kafka-external-bootstrap"), notNull());
                    verify(supplier.serviceOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-kafka-0"), notNull());
                    verify(supplier.serviceOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-kafka-1"), notNull());
                    verify(supplier.serviceOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-kafka-2"), notNull());

                    async.flag();
                })));
    }

    @Test
    public void testLoadBalancerWithBootstrapService(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(LISTENER_PORT)
                                .withTls(true)
                                .withType(KafkaListenerType.LOADBALANCER)
                                .withNewConfiguration()
                                    .withCreateBootstrapService(true)
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

        ResourceOperatorSupplier supplier = prepareResourceOperatorSupplier();

        MockKafkaListenersReconciler reconciler = new MockKafkaListenersReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                kafkaCluster,
                new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier.secretOperations,
                supplier.serviceOperations,
                supplier.routeOperations,
                supplier.ingressOperations
        );

        Checkpoint async = context.checkpoint();
        reconciler.reconcile()
                .onComplete(context.succeeding(res -> context.verify(() -> {
                    // Check status
                    assertThat(res.listenerStatuses.size(), is(1));
                    ListenerStatus listenerStatus = res.listenerStatuses.get(0);
                    assertThat(listenerStatus.getBootstrapServers(), is("bootstrap-broker.test.dns.name:9094"));
                    assertThat(listenerStatus.getAddresses().size(), is(1));
                    assertThat(listenerStatus.getAddresses().get(0).getHost(), is(DNS_NAME_FOR_BOOTSTRAP_SERVICE));
                    assertThat(listenerStatus.getAddresses().get(0).getPort(), is(LISTENER_PORT));

                    // Check creation of services
                    verify(supplier.serviceOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-kafka-external-bootstrap"), notNull());
                    verify(supplier.serviceOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-kafka-0"), notNull());
                    verify(supplier.serviceOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-kafka-1"), notNull());
                    verify(supplier.serviceOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-kafka-2"), notNull());

                    async.flag();
                })));
    }


    @Test
    public void testLoadBalancer(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(LISTENER_PORT)
                                .withTls(true)
                                .withType(KafkaListenerType.LOADBALANCER)
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

        ResourceOperatorSupplier supplier = prepareResourceOperatorSupplier();

        MockKafkaListenersReconciler reconciler = new MockKafkaListenersReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                kafkaCluster,
                new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier.secretOperations,
                supplier.serviceOperations,
                supplier.routeOperations,
                supplier.ingressOperations
        );

        Checkpoint async = context.checkpoint();
        reconciler.reconcile()
                .onComplete(context.succeeding(res -> context.verify(() -> {
                    // Check status
                    assertThat(res.listenerStatuses.size(), is(1));
                    ListenerStatus listenerStatus = res.listenerStatuses.get(0);
                    assertThat(listenerStatus.getBootstrapServers(), is("bootstrap-broker.test.dns.name:9094"));
                    assertThat(listenerStatus.getAddresses().size(), is(1));
                    assertThat(listenerStatus.getAddresses().get(0).getHost(), is(DNS_NAME_FOR_BOOTSTRAP_SERVICE));
                    assertThat(listenerStatus.getAddresses().get(0).getPort(), is(LISTENER_PORT));

                    // Check creation of services
                    verify(supplier.serviceOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-kafka-external-bootstrap"), notNull());
                    verify(supplier.serviceOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-kafka-0"), notNull());
                    verify(supplier.serviceOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-kafka-1"), notNull());
                    verify(supplier.serviceOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-kafka-2"), notNull());

                    async.flag();
                })));
    }

    private ResourceOperatorSupplier prepareResourceOperatorSupplier() {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the Services for the kafka.
        Service mockServiceBootstrap = mock(Service.class, RETURNS_DEEP_STUBS);
        Service mockServiceBroker0 = mock(Service.class, RETURNS_DEEP_STUBS);
        Service mockServiceBroker1 = mock(Service.class, RETURNS_DEEP_STUBS);
        Service mockServiceBroker2 = mock(Service.class, RETURNS_DEEP_STUBS);
        when(mockServiceBootstrap.getStatus().getLoadBalancer().getIngress().get(0).getHostname()).thenReturn(DNS_NAME_FOR_BOOTSTRAP_SERVICE);
        when(mockServiceBroker0.getStatus().getLoadBalancer().getIngress().get(0).getHostname()).thenReturn(DNS_NAME_FOR_BROKER_0);
        when(mockServiceBroker1.getStatus().getLoadBalancer().getIngress().get(0).getHostname()).thenReturn(DNS_NAME_FOR_BROKER_1);
        when(mockServiceBroker2.getStatus().getLoadBalancer().getIngress().get(0).getHostname()).thenReturn(DNS_NAME_FOR_BROKER_2);

        // Mock the ServiceOperator for the kafka services.
        ServiceOperator mockServiceOperator = supplier.serviceOperations;

        // Delegate the batchReconcile call to the real method which calls the other mocked methods. This allows us to better test the exact behavior.
        when(mockServiceOperator.batchReconcile(any(), eq(NAMESPACE), any(), any())).thenCallRealMethod();

        // Mock getting of services and their readiness
        when(mockServiceOperator.getAsync(eq(NAMESPACE), eq(CLUSTER_NAME + "-kafka-external-bootstrap"))).thenReturn(Future.succeededFuture(mockServiceBootstrap));
        when(mockServiceOperator.getAsync(eq(NAMESPACE), eq(CLUSTER_NAME + "-kafka-0"))).thenReturn(Future.succeededFuture(mockServiceBroker0));
        when(mockServiceOperator.getAsync(eq(NAMESPACE), eq(CLUSTER_NAME + "-kafka-1"))).thenReturn(Future.succeededFuture(mockServiceBroker1));
        when(mockServiceOperator.getAsync(eq(NAMESPACE), eq(CLUSTER_NAME + "-kafka-2"))).thenReturn(Future.succeededFuture(mockServiceBroker2));

        // Mock listing of services
        when(mockServiceOperator.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));

        // Mock service creation / update
        when(mockServiceOperator.reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-kafka-external-bootstrap"), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(mockServiceBootstrap)));
        when(mockServiceOperator.reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-kafka-0"), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(mockServiceBroker0)));
        when(mockServiceOperator.reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-kafka-1"), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(mockServiceBroker1)));
        when(mockServiceOperator.reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-kafka-2"), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(mockServiceBroker2)));
        when(mockServiceOperator.reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-kafka-brokers"), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new Service())));
        when(mockServiceOperator.reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-kafka-bootstrap"), any())).thenReturn(Future.succeededFuture(ReconcileResult.created(new Service())));

        return supplier;
    }

    /**
     * Override KafkaListenersReconciler to only run reconciliation steps that concern the Load balancer resources feature
     */
    static class MockKafkaListenersReconciler extends KafkaListenersReconciler {
        public MockKafkaListenersReconciler(
                Reconciliation reconciliation,
                KafkaCluster kafka,
                PlatformFeaturesAvailability pfa,
                SecretOperator secretOperator,
                ServiceOperator serviceOperator,
                RouteOperator routeOperator,
                IngressOperator ingressOperator) {
            super(reconciliation, kafka, null, pfa, 300_000L, secretOperator, serviceOperator, routeOperator, ingressOperator);
        }

        @Override
        public Future<ReconciliationResult> reconcile()  {
            return services()
                    .compose(i -> loadBalancerServicesReady())
                    .compose(i -> Future.succeededFuture(result));
        }
    }
}