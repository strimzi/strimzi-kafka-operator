/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.openshift.api.model.Route;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.kafka.listener.ListenerStatus;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaMetadataConfigurationState;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.IngressOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.RouteOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.platform.KubernetesVersion;
import io.vertx.core.Future;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaListenerReconcilerRoutesTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    public static final String NAMESPACE = "test";
    public static final String CLUSTER_NAME = "my-kafka";
    public static final String DNS_NAME_FOR_BROKER_10 = "broker-10-route.test.dns.name";
    public static final String DNS_NAME_FOR_BROKER_11 = "broker-11-route.test.dns.name";
    public static final String DNS_NAME_FOR_BROKER_12 = "broker-12-reout.test.dns.name";
    public static final String DNS_NAME_FOR_BOOTSTRAP_SERVICE = "bootstrap-route.test.dns.name";
    public static final int LISTENER_PORT = 9094;
    private static final Kafka KAFKA = new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withAnnotations(Map.of(
                            Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled",
                            Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled"
                    ))
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
                    .withNewEntityOperator()
                        .withNewTopicOperator()
                        .endTopicOperator()
                        .withNewUserOperator()
                        .endUserOperator()
                    .endEntityOperator()
                .endSpec()
                .build();
    private static final KafkaNodePool POOL_CONTROLLERS = new KafkaNodePoolBuilder()
                .withNewMetadata()
                    .withName("controllers")
                    .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME))
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[0-9]"))
                .endMetadata()
                .withNewSpec()
                    .withReplicas(3)
                    .withNewJbodStorage()
                        .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").withStorageClass("gp99").build())
                    .endJbodStorage()
                    .withRoles(ProcessRoles.CONTROLLER)
                .endSpec()
                .build();
    private static final KafkaNodePool POOL_BROKERS = new KafkaNodePoolBuilder()
                .withNewMetadata()
                    .withName("brokers")
                    .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME))
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[10-99]"))
                .endMetadata()
                .withNewSpec()
                    .withReplicas(3)
                    .withNewJbodStorage()
                        .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").withStorageClass("gp99").build())
                    .endJbodStorage()
                    .withRoles(ProcessRoles.BROKER)
                .endSpec()
                .build();

    @Test
    public void testRoutesNotSupported(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(LISTENER_PORT)
                                .withTls(true)
                                .withType(KafkaListenerType.ROUTE)
                                .build())
                    .endKafka()
                .endSpec()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the ServiceOperator for the kafka services.
        ServiceOperator mockServiceOperator = supplier.serviceOperations;
        // Delegate the batchReconcile call to the real method which calls the other mocked methods. This allows us to better test the exact behavior.
        when(mockServiceOperator.batchReconcile(any(), eq(NAMESPACE), any(), any())).thenCallRealMethod();
        // Mock listing of services
        when(mockServiceOperator.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));
        // Mock service creation / update
        when(mockServiceOperator.reconcile(any(), eq(NAMESPACE), any(), any())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(3))));

        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME);

        KafkaCluster kafkaCluster = KafkaClusterCreator.createKafkaCluster(
                reconciliation,
                kafka,
                List.of(POOL_CONTROLLERS, POOL_BROKERS),
                Map.of(),
                Map.of(),
                KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE,
                KafkaMetadataConfigurationState.KRAFT,
                VERSIONS,
                supplier.sharedEnvironmentProvider
        );

        MockKafkaListenersReconciler reconciler = new MockKafkaListenersReconciler(
                reconciliation,
                kafkaCluster,
                new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier.secretOperations,
                supplier.serviceOperations,
                supplier.routeOperations,
                supplier.ingressOperations
        );

        Checkpoint async = context.checkpoint();
        reconciler.reconcile()
                .onComplete(context.failing(res -> context.verify(() -> {
                    assertThat(res.getMessage(), is("The OpenShift route API is not available in this Kubernetes cluster. Exposing Kafka cluster my-kafka using routes is not possible."));
                    async.flag();
                })));
    }

    @Test
    public void testRoutes(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(LISTENER_PORT)
                                .withTls(true)
                                .withType(KafkaListenerType.ROUTE)
                                .build())
                    .endKafka()
                .endSpec()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);

        // Mock the ServiceOperator for the kafka services.
        ServiceOperator mockServiceOperator = supplier.serviceOperations;
        // Delegate the batchReconcile call to the real method which calls the other mocked methods. This allows us to better test the exact behavior.
        when(mockServiceOperator.batchReconcile(any(), eq(NAMESPACE), any(), any())).thenCallRealMethod();
        // Mock listing of services
        when(mockServiceOperator.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));
        // Mock service creation / update
        when(mockServiceOperator.reconcile(any(), eq(NAMESPACE), any(), any())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(3))));

        // Mock the RouteOperator for the OpenShift routes
        RouteOperator mockRouteOperator = supplier.routeOperations;
        // Delegate the batchReconcile call to the real method which calls the other mocked methods. This allows us to better test the exact behavior.
        when(mockRouteOperator.batchReconcile(any(), eq(NAMESPACE), any(), any())).thenCallRealMethod();
        // Mock getting of routes and their readiness
        Route mockRouteBootstrap = mock(Route.class, RETURNS_DEEP_STUBS);
        Route mockRouteBroker0 = mock(Route.class, RETURNS_DEEP_STUBS);
        Route mockRouteBroker1 = mock(Route.class, RETURNS_DEEP_STUBS);
        Route mockRouteBroker2 = mock(Route.class, RETURNS_DEEP_STUBS);
        when(mockRouteBootstrap.getStatus().getIngress().get(0).getHost()).thenReturn(DNS_NAME_FOR_BOOTSTRAP_SERVICE);
        when(mockRouteBroker0.getStatus().getIngress().get(0).getHost()).thenReturn(DNS_NAME_FOR_BROKER_10);
        when(mockRouteBroker1.getStatus().getIngress().get(0).getHost()).thenReturn(DNS_NAME_FOR_BROKER_11);
        when(mockRouteBroker2.getStatus().getIngress().get(0).getHost()).thenReturn(DNS_NAME_FOR_BROKER_12);
        when(mockRouteOperator.getAsync(eq(NAMESPACE), eq(CLUSTER_NAME + "-kafka-bootstrap"))).thenReturn(Future.succeededFuture(mockRouteBootstrap));
        when(mockRouteOperator.getAsync(eq(NAMESPACE), eq(CLUSTER_NAME + "-brokers-10"))).thenReturn(Future.succeededFuture(mockRouteBroker0));
        when(mockRouteOperator.getAsync(eq(NAMESPACE), eq(CLUSTER_NAME + "-brokers-11"))).thenReturn(Future.succeededFuture(mockRouteBroker1));
        when(mockRouteOperator.getAsync(eq(NAMESPACE), eq(CLUSTER_NAME + "-brokers-12"))).thenReturn(Future.succeededFuture(mockRouteBroker2));
        // Mock listing of routes
        when(mockRouteOperator.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of()));
        // Mock route creation / update
        when(mockRouteOperator.reconcile(any(), eq(NAMESPACE), any(), any())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.created(i.getArgument(3))));

        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME);

        KafkaCluster kafkaCluster = KafkaClusterCreator.createKafkaCluster(
                reconciliation,
                kafka,
                List.of(POOL_CONTROLLERS, POOL_BROKERS),
                Map.of(),
                Map.of(),
                KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE,
                KafkaMetadataConfigurationState.KRAFT,
                VERSIONS,
                supplier.sharedEnvironmentProvider
        );

        MockKafkaListenersReconciler reconciler = new MockKafkaListenersReconciler(
                reconciliation,
                kafkaCluster,
                new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
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
                    assertThat(listenerStatus.getBootstrapServers(), is(DNS_NAME_FOR_BOOTSTRAP_SERVICE + ":443"));
                    assertThat(listenerStatus.getAddresses().size(), is(1));
                    assertThat(listenerStatus.getAddresses().get(0).getHost(), is(DNS_NAME_FOR_BOOTSTRAP_SERVICE));
                    assertThat(listenerStatus.getAddresses().get(0).getPort(), is(443));

                    // Check creation of services
                    verify(supplier.serviceOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-kafka-external-bootstrap"), notNull());
                    verify(supplier.serviceOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-brokers-10"), notNull());
                    verify(supplier.serviceOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-brokers-11"), notNull());
                    verify(supplier.serviceOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-brokers-12"), notNull());

                    // Check creation of routes
                    verify(supplier.routeOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-kafka-bootstrap"), notNull());
                    verify(supplier.routeOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-brokers-10"), notNull());
                    verify(supplier.routeOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-brokers-11"), notNull());
                    verify(supplier.routeOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-brokers-12"), notNull());

                    async.flag();
                })));
    }

    @Test
    public void testRouteDeletion(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("internal")
                                .withPort(9092)
                                .withTls(false)
                                .withType(KafkaListenerType.INTERNAL)
                                .build())
                    .endKafka()
                .endSpec()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);

        // Mock the ServiceOperator for the kafka services.
        ServiceOperator mockServiceOperator = supplier.serviceOperations;
        // Delegate the batchReconcile call to the real method which calls the other mocked methods. This allows us to better test the exact behavior.
        when(mockServiceOperator.batchReconcile(any(), eq(NAMESPACE), any(), any())).thenCallRealMethod();
        // Mock listing of services
        Service mockServiceLocalBootstrap = mock(Service.class, RETURNS_DEEP_STUBS);
        Service mockServiceBrokers = mock(Service.class, RETURNS_DEEP_STUBS);
        Service mockServiceBootstrap = mock(Service.class, RETURNS_DEEP_STUBS);
        Service mockServiceBroker0 = mock(Service.class, RETURNS_DEEP_STUBS);
        Service mockServiceBroker1 = mock(Service.class, RETURNS_DEEP_STUBS);
        Service mockServiceBroker2 = mock(Service.class, RETURNS_DEEP_STUBS);
        when(mockServiceLocalBootstrap.getMetadata().getName()).thenReturn(CLUSTER_NAME + "-kafka-brokers");
        when(mockServiceBrokers.getMetadata().getName()).thenReturn(CLUSTER_NAME + "-kafka-bootstrap");
        when(mockServiceBootstrap.getMetadata().getName()).thenReturn(CLUSTER_NAME + "-kafka-external-bootstrap");
        when(mockServiceBroker0.getMetadata().getName()).thenReturn(CLUSTER_NAME + "-brokers-10");
        when(mockServiceBroker1.getMetadata().getName()).thenReturn(CLUSTER_NAME + "-brokers-11");
        when(mockServiceBroker2.getMetadata().getName()).thenReturn(CLUSTER_NAME + "-brokers-12");
        when(mockServiceOperator.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of(mockServiceLocalBootstrap, mockServiceBrokers, mockServiceBootstrap, mockServiceBroker0, mockServiceBroker1, mockServiceBroker2)));
        // Mock service creation / update
        when(mockServiceOperator.reconcile(any(), eq(NAMESPACE), any(), any())).thenAnswer(i -> {
            if (i.getArgument(3) != null) {
                return Future.succeededFuture(ReconcileResult.created(i.getArgument(3)));
            } else {
                return Future.succeededFuture(ReconcileResult.deleted());
            }
        });

        // Mock the RouteOperator for the OpenShift routes
        RouteOperator mockRouteOperator = supplier.routeOperations;
        // Delegate the batchReconcile call to the real method which calls the other mocked methods. This allows us to better test the exact behavior.
        when(mockRouteOperator.batchReconcile(any(), eq(NAMESPACE), any(), any())).thenCallRealMethod();
        // Mock listing of routes
        Route mockRouteBootstrap = mock(Route.class, RETURNS_DEEP_STUBS);
        Route mockRouteBroker0 = mock(Route.class, RETURNS_DEEP_STUBS);
        Route mockRouteBroker1 = mock(Route.class, RETURNS_DEEP_STUBS);
        Route mockRouteBroker2 = mock(Route.class, RETURNS_DEEP_STUBS);
        when(mockRouteBootstrap.getMetadata().getName()).thenReturn(CLUSTER_NAME + "-kafka-bootstrap");
        when(mockRouteBroker0.getMetadata().getName()).thenReturn(CLUSTER_NAME + "-brokers-10");
        when(mockRouteBroker1.getMetadata().getName()).thenReturn(CLUSTER_NAME + "-brokers-11");
        when(mockRouteBroker2.getMetadata().getName()).thenReturn(CLUSTER_NAME + "-brokers-12");
        when(mockRouteOperator.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(List.of(mockRouteBootstrap, mockRouteBroker0, mockRouteBroker1, mockRouteBroker2)));
        // Mock route creation / update
        when(mockRouteOperator.reconcile(any(), eq(NAMESPACE), any(), any())).thenReturn(Future.succeededFuture(ReconcileResult.deleted()));

        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME);

        KafkaCluster kafkaCluster = KafkaClusterCreator.createKafkaCluster(
                reconciliation,
                kafka,
                List.of(POOL_CONTROLLERS, POOL_BROKERS),
                Map.of(),
                Map.of(),
                KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE,
                KafkaMetadataConfigurationState.KRAFT,
                VERSIONS,
                supplier.sharedEnvironmentProvider
        );

        MockKafkaListenersReconciler reconciler = new MockKafkaListenersReconciler(
                reconciliation,
                kafkaCluster,
                new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier.secretOperations,
                supplier.serviceOperations,
                supplier.routeOperations,
                supplier.ingressOperations
        );

        Checkpoint async = context.checkpoint();
        reconciler.reconcile()
                .onComplete(context.succeeding(res -> context.verify(() -> {
                    // Check status
                    assertThat(res.listenerStatuses.size(), is(0));

                    // Check creation of services
                    verify(supplier.serviceOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-kafka-bootstrap"), notNull());
                    verify(supplier.serviceOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-kafka-brokers"), notNull());
                    verify(supplier.serviceOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-kafka-external-bootstrap"), isNull());
                    verify(supplier.serviceOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-brokers-10"), isNull());
                    verify(supplier.serviceOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-brokers-11"), isNull());
                    verify(supplier.serviceOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-brokers-12"), isNull());

                    // Check creation of routes
                    verify(supplier.routeOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-kafka-bootstrap"), isNull());
                    verify(supplier.routeOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-brokers-10"), isNull());
                    verify(supplier.routeOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-brokers-11"), isNull());
                    verify(supplier.routeOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-brokers-12"), isNull());

                    async.flag();
                })));
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
                    .compose(i -> routes())
                    .compose(i -> clusterIPServicesReady())
                    .compose(i -> loadBalancerServicesReady())
                    .compose(i -> routesReady())
                    .compose(i -> Future.succeededFuture(result));
        }
    }
}