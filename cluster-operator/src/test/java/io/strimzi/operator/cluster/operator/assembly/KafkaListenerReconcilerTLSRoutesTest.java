/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.gatewayapi.v1.ParentReferenceBuilder;
import io.fabric8.kubernetes.api.model.gatewayapi.v1.RouteParentStatus;
import io.fabric8.kubernetes.api.model.gatewayapi.v1.TLSRoute;
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
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.IngressOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.RouteOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.TLSRouteOperator;
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
import java.util.function.BiPredicate;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaListenerReconcilerTLSRoutesTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    public static final String NAMESPACE = "test";
    public static final String CLUSTER_NAME = "my-kafka";
    public static final String DNS_NAME_FOR_BOOTSTRAP_SERVICE = "my-kafka-bootstrap.com";
    public static final int LISTENER_PORT = 9094;
    private static final Kafka KAFKA = new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
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
    public void testTLSRoutesNotSupported(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(LISTENER_PORT)
                                .withType(KafkaListenerType.TLSROUTE)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withNewBootstrap()
                                        .withHost("my-kafka-bootstrap.com")
                                        .withAnnotations(Map.of("dns-annotation", "my-kafka-bootstrap.com"))
                                        .withLabels(Map.of("label", "label-value"))
                                    .endBootstrap()
                                    .withHostTemplate("my-kafka-broker-{nodeId}.com")
                                    .withPerBrokerLabelsTemplate(Map.of("label", "label-value-{nodeId}"))
                                    .withPerBrokerAnnotationsTemplate(Map.of("dns-annotation", "my-kafka-broker-{nodeId}.com"))
                                    .withParentRefs(new ParentReferenceBuilder()
                                            .withKind("Gateway")
                                            .withGroup("gateway.networking.k8s.io")
                                            .withName("envoy-gateway")
                                            .withNamespace("envoy-gateway-system")
                                            .build())
                                .endConfiguration()
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
                KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE,
                VERSIONS,
                supplier.sharedEnvironmentProvider
        );

        MockKafkaListenersReconciler reconciler = new MockKafkaListenersReconciler(
                reconciliation,
                kafkaCluster,
                new PlatformFeaturesAvailability(true, false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier.secretOperations,
                supplier.serviceOperations,
                supplier.routeOperations,
                supplier.tlsRouteOperations,
                supplier.ingressOperations
        );

        Checkpoint async = context.checkpoint();
        reconciler.reconcile()
                .onComplete(context.failing(res -> context.verify(() -> {
                    assertThat(res.getMessage(), is("The Gateway API TLSRoute resource is not available in this Kubernetes cluster. Exposing Kafka cluster my-kafka using TLSRoutes is not possible."));
                    async.flag();
                })));
    }

    @Test
    public void testTLSRoutes(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(LISTENER_PORT)
                                .withType(KafkaListenerType.TLSROUTE)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withNewBootstrap()
                                        .withHost("my-kafka-bootstrap.com")
                                        .withAnnotations(Map.of("dns-annotation", "my-kafka-bootstrap.com"))
                                        .withLabels(Map.of("label", "label-value"))
                                    .endBootstrap()
                                    .withHostTemplate("my-kafka-broker-{nodeId}.com")
                                    .withPerBrokerLabelsTemplate(Map.of("label", "label-value-{nodeId}"))
                                    .withPerBrokerAnnotationsTemplate(Map.of("dns-annotation", "my-kafka-broker-{nodeId}.com"))
                                    .withParentRefs(new ParentReferenceBuilder()
                                            .withKind("Gateway")
                                            .withGroup("gateway.networking.k8s.io")
                                            .withName("envoy-gateway")
                                            .withNamespace("envoy-gateway-system")
                                            .build())
                                .endConfiguration()
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
        TLSRouteOperator mockRouteOperator = supplier.tlsRouteOperations;
        // Delegate the batchReconcile call to the real method which calls the other mocked methods. This allows us to better test the exact behavior.
        when(mockRouteOperator.batchReconcile(any(), eq(NAMESPACE), any(), any())).thenCallRealMethod();
        // Mock getting of routes
        TLSRoute mockRouteBootstrap = mock(TLSRoute.class, RETURNS_DEEP_STUBS);
        TLSRoute mockRouteBroker0 = mock(TLSRoute.class, RETURNS_DEEP_STUBS);
        TLSRoute mockRouteBroker1 = mock(TLSRoute.class, RETURNS_DEEP_STUBS);
        TLSRoute mockRouteBroker2 = mock(TLSRoute.class, RETURNS_DEEP_STUBS);
        when(mockRouteBootstrap.getStatus().getParents()).thenReturn(List.of(new RouteParentStatus()));
        when(mockRouteBroker0.getStatus().getParents()).thenReturn(List.of(new RouteParentStatus()));
        when(mockRouteBroker1.getStatus().getParents()).thenReturn(List.of(new RouteParentStatus()));
        when(mockRouteBroker2.getStatus().getParents()).thenReturn(List.of(new RouteParentStatus()));
        when(mockRouteOperator.get(eq(NAMESPACE), eq(CLUSTER_NAME + "-kafka-bootstrap"))).thenReturn(mockRouteBootstrap);
        when(mockRouteOperator.get(eq(NAMESPACE), eq(CLUSTER_NAME + "-brokers-10"))).thenReturn(mockRouteBroker0);
        when(mockRouteOperator.get(eq(NAMESPACE), eq(CLUSTER_NAME + "-brokers-11"))).thenReturn(mockRouteBroker1);
        when(mockRouteOperator.get(eq(NAMESPACE), eq(CLUSTER_NAME + "-brokers-12"))).thenReturn(mockRouteBroker2);
        when(mockRouteOperator.hasParents(any(), eq(NAMESPACE), any(), anyLong(), anyLong())).thenCallRealMethod();
        when(mockRouteOperator.waitFor(any(), eq(NAMESPACE), any(), eq("addressable"), anyLong(), anyLong(), any())).then(i -> {
            BiPredicate<String, String> predicate = i.getArgument(6);
            boolean result = predicate.test(i.getArgument(1), i.getArgument(2));
            return result ? Future.succeededFuture() : Future.failedFuture(new RuntimeException("failed"));
        });
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
                KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE,
                VERSIONS,
                supplier.sharedEnvironmentProvider
        );

        MockKafkaListenersReconciler reconciler = new MockKafkaListenersReconciler(
                reconciliation,
                kafkaCluster,
                new PlatformFeaturesAvailability(false, true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier.secretOperations,
                supplier.serviceOperations,
                supplier.routeOperations,
                supplier.tlsRouteOperations,
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
                    verify(supplier.tlsRouteOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-kafka-bootstrap"), notNull());
                    verify(supplier.tlsRouteOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-brokers-10"), notNull());
                    verify(supplier.tlsRouteOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-brokers-11"), notNull());
                    verify(supplier.tlsRouteOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-brokers-12"), notNull());

                    async.flag();
                })));
    }

    @Test
    public void testTLSRouteDeletion(VertxTestContext context) {
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
        TLSRouteOperator mockRouteOperator = supplier.tlsRouteOperations;
        // Delegate the batchReconcile call to the real method which calls the other mocked methods. This allows us to better test the exact behavior.
        when(mockRouteOperator.batchReconcile(any(), eq(NAMESPACE), any(), any())).thenCallRealMethod();
        // Mock the check if the TLSRoute has been accepted or not
        when(mockRouteOperator.hasParents(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-kafka-bootstrap"), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockRouteOperator.hasParents(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-brokers-10"), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockRouteOperator.hasParents(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-brokers-11"), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        when(mockRouteOperator.hasParents(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-brokers-12"), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
        // Mock listing of routes
        TLSRoute mockRouteBootstrap = mock(TLSRoute.class, RETURNS_DEEP_STUBS);
        TLSRoute mockRouteBroker0 = mock(TLSRoute.class, RETURNS_DEEP_STUBS);
        TLSRoute mockRouteBroker1 = mock(TLSRoute.class, RETURNS_DEEP_STUBS);
        TLSRoute mockRouteBroker2 = mock(TLSRoute.class, RETURNS_DEEP_STUBS);
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
                KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE,
                VERSIONS,
                supplier.sharedEnvironmentProvider
        );

        MockKafkaListenersReconciler reconciler = new MockKafkaListenersReconciler(
                reconciliation,
                kafkaCluster,
                new PlatformFeaturesAvailability(false, true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier.secretOperations,
                supplier.serviceOperations,
                supplier.routeOperations,
                supplier.tlsRouteOperations,
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
                    verify(supplier.tlsRouteOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-kafka-bootstrap"), isNull());
                    verify(supplier.tlsRouteOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-brokers-10"), isNull());
                    verify(supplier.tlsRouteOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-brokers-11"), isNull());
                    verify(supplier.tlsRouteOperations, times(1)).reconcile(any(), eq(NAMESPACE), eq(CLUSTER_NAME + "-brokers-12"), isNull());

                    async.flag();
                })));
    }

    @Test
    public void testTLSRoutesNotReady(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(LISTENER_PORT)
                                .withType(KafkaListenerType.TLSROUTE)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withNewBootstrap()
                                        .withHost("my-kafka-bootstrap.com")
                                        .withAnnotations(Map.of("dns-annotation", "my-kafka-bootstrap.com"))
                                        .withLabels(Map.of("label", "label-value"))
                                    .endBootstrap()
                                    .withHostTemplate("my-kafka-broker-{nodeId}.com")
                                    .withPerBrokerLabelsTemplate(Map.of("label", "label-value-{nodeId}"))
                                    .withPerBrokerAnnotationsTemplate(Map.of("dns-annotation", "my-kafka-broker-{nodeId}.com"))
                                    .withParentRefs(new ParentReferenceBuilder()
                                            .withKind("Gateway")
                                            .withGroup("gateway.networking.k8s.io")
                                            .withName("envoy-gateway")
                                            .withNamespace("envoy-gateway-system")
                                            .build())
                                .endConfiguration()
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
        TLSRouteOperator mockRouteOperator = supplier.tlsRouteOperations;
        // Delegate the batchReconcile call to the real method which calls the other mocked methods. This allows us to better test the exact behavior.
        when(mockRouteOperator.batchReconcile(any(), eq(NAMESPACE), any(), any())).thenCallRealMethod();
        // Mock getting of routes
        TLSRoute mockRouteBootstrap = mock(TLSRoute.class, RETURNS_DEEP_STUBS);
        TLSRoute mockRouteBroker0 = mock(TLSRoute.class, RETURNS_DEEP_STUBS);
        TLSRoute mockRouteBroker1 = mock(TLSRoute.class, RETURNS_DEEP_STUBS);
        TLSRoute mockRouteBroker2 = mock(TLSRoute.class, RETURNS_DEEP_STUBS);
        when(mockRouteBootstrap.getStatus().getParents()).thenReturn(List.of(new RouteParentStatus()));
        when(mockRouteBroker0.getStatus().getParents()).thenReturn(null);
        when(mockRouteBroker1.getStatus().getParents()).thenReturn(List.of());
        when(mockRouteBroker2.getStatus().getParents()).thenReturn(List.of(new RouteParentStatus()));
        when(mockRouteOperator.get(eq(NAMESPACE), eq(CLUSTER_NAME + "-kafka-bootstrap"))).thenReturn(mockRouteBootstrap);
        when(mockRouteOperator.get(eq(NAMESPACE), eq(CLUSTER_NAME + "-brokers-10"))).thenReturn(mockRouteBroker0);
        when(mockRouteOperator.get(eq(NAMESPACE), eq(CLUSTER_NAME + "-brokers-11"))).thenReturn(mockRouteBroker1);
        when(mockRouteOperator.get(eq(NAMESPACE), eq(CLUSTER_NAME + "-brokers-12"))).thenReturn(mockRouteBroker2);
        when(mockRouteOperator.hasParents(any(), eq(NAMESPACE), any(), anyLong(), anyLong())).thenCallRealMethod();
        when(mockRouteOperator.waitFor(any(), eq(NAMESPACE), any(), eq("addressable"), anyLong(), anyLong(), any())).then(i -> {
            BiPredicate<String, String> predicate = i.getArgument(6);
            boolean result = predicate.test(i.getArgument(1), i.getArgument(2));
            return result ? Future.succeededFuture() : Future.failedFuture(new RuntimeException(i.getArgument(2) + " is not addressable"));
        });
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
                KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE,
                VERSIONS,
                supplier.sharedEnvironmentProvider
        );

        MockKafkaListenersReconciler reconciler = new MockKafkaListenersReconciler(
                reconciliation,
                kafkaCluster,
                new PlatformFeaturesAvailability(false, true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                supplier.secretOperations,
                supplier.serviceOperations,
                supplier.routeOperations,
                supplier.tlsRouteOperations,
                supplier.ingressOperations
        );

        Checkpoint async = context.checkpoint();
        reconciler.reconcile()
                .onComplete(context.failing(e -> context.verify(() -> {
                    // Check status
                    assertThat(e.getMessage(), is("my-kafka-brokers-10 is not addressable"));

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
                TLSRouteOperator tlsRouteOperator,
                IngressOperator ingressOperator) {
            super(reconciliation, kafka, null, pfa, 300_000L, secretOperator, serviceOperator, routeOperator, tlsRouteOperator, ingressOperator);
        }

        @Override
        public Future<ReconciliationResult> reconcile()  {
            return services()
                    .compose(i -> tlsRoutes())
                    .compose(i -> clusterIPServicesReady())
                    .compose(i -> loadBalancerServicesReady())
                    .compose(i -> tlsRoutesReady())
                    .compose(i -> Future.succeededFuture(result));
        }
    }
}