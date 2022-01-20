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
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static java.util.Collections.emptyList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaAssemblyOperatorLoadBalancerKafkaListenerTest {
    public static final String NAMESPACE = "test";
    public static final String NAME = "my-kafka";
    public static final String DNS_NAME_FOR_BROKER_0 = "broker-0.test.dns.name";
    public static final String DNS_NAME_FOR_BROKER_1 = "broker-1.test.dns.name";
    public static final String DNS_NAME_FOR_BROKER_2 = "broker-2.test.dns.name";
    public static final String DNS_NAME_FOR_BOOTSTRAP_SERVICE = "bootstrap-broker.test.dns.name";
    public static final int LISTENER_PORT = 9094;

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
    public void testLoadBalancerSkipBootstrapService(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName(NAME)
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

        ResourceOperatorSupplier supplier = prepareResourceOperatorSupplier(kafka);

        KafkaAssemblyOperator op = new MockKafkaAssemblyOperatorForLoadBalancerTests(vertx, new PlatformFeaturesAvailability(false, KubernetesVersion.V1_16), certManager, passwordGenerator,
                supplier, ResourceUtils.dummyClusterOperatorConfig(KafkaVersionTestUtils.getKafkaVersionLookup()));
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);

        Checkpoint async = context.checkpoint();
        op.reconcile(reconciliation)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(kafka.getStatus().getListeners().size(), is(1));
                    ListenerStatus listenerStatus = kafka.getStatus().getListeners().get(0);
                    assertThat(listenerStatus.getBootstrapServers(), is("broker-0.test.dns.name:9094,broker-1.test.dns.name:9094,broker-2.test.dns.name:9094"));
                    assertThat(listenerStatus.getAddresses().size(), is(3));
                    assertThat(listenerStatus.getAddresses().get(0).getHost(), is(DNS_NAME_FOR_BROKER_0));
                    assertThat(listenerStatus.getAddresses().get(1).getHost(), is(DNS_NAME_FOR_BROKER_1));
                    assertThat(listenerStatus.getAddresses().get(2).getHost(), is(DNS_NAME_FOR_BROKER_2));
                    assertThat(listenerStatus.getAddresses().get(0).getPort(), is(LISTENER_PORT));
                    assertThat(listenerStatus.getAddresses().get(1).getPort(), is(LISTENER_PORT));
                    assertThat(listenerStatus.getAddresses().get(2).getPort(), is(LISTENER_PORT));
                    async.flag();
                })));
    }

    @Test
    public void testLoadBalancerWithBootstrapService(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                .withName(NAME)
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

        ResourceOperatorSupplier supplier = prepareResourceOperatorSupplier(kafka);

        KafkaAssemblyOperator op = new MockKafkaAssemblyOperatorForLoadBalancerTests(vertx, new PlatformFeaturesAvailability(false, KubernetesVersion.V1_16), certManager, passwordGenerator,
                supplier, ResourceUtils.dummyClusterOperatorConfig(KafkaVersionTestUtils.getKafkaVersionLookup()));
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);

        Checkpoint async = context.checkpoint();
        op.reconcile(reconciliation)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(kafka.getStatus().getListeners().size(), is(1));
                    ListenerStatus listenerStatus = kafka.getStatus().getListeners().get(0);
                    assertThat(listenerStatus.getBootstrapServers(), is("bootstrap-broker.test.dns.name:9094"));
                    assertThat(listenerStatus.getAddresses().size(), is(1));
                    assertThat(listenerStatus.getAddresses().get(0).getHost(), is(DNS_NAME_FOR_BOOTSTRAP_SERVICE));
                    assertThat(listenerStatus.getAddresses().get(0).getPort(), is(LISTENER_PORT));
                    async.flag();
                })));
    }


    @Test
    public void testLoadBalancer(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                .withName(NAME)
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

        ResourceOperatorSupplier supplier = prepareResourceOperatorSupplier(kafka);

        KafkaAssemblyOperator op = new MockKafkaAssemblyOperatorForLoadBalancerTests(vertx, new PlatformFeaturesAvailability(false, KubernetesVersion.V1_16), certManager, passwordGenerator,
                supplier, ResourceUtils.dummyClusterOperatorConfig(KafkaVersionTestUtils.getKafkaVersionLookup()));
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);

        Checkpoint async = context.checkpoint();
        op.reconcile(reconciliation)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(kafka.getStatus().getListeners().size(), is(1));
                    ListenerStatus listenerStatus = kafka.getStatus().getListeners().get(0);
                    assertThat(listenerStatus.getBootstrapServers(), is("bootstrap-broker.test.dns.name:9094"));
                    assertThat(listenerStatus.getAddresses().size(), is(1));
                    assertThat(listenerStatus.getAddresses().get(0).getHost(), is(DNS_NAME_FOR_BOOTSTRAP_SERVICE));
                    assertThat(listenerStatus.getAddresses().get(0).getPort(), is(LISTENER_PORT));
                    async.flag();
                })));
    }

    /**
     * Override KafkaAssemblyOperator to only run reconciliation steps that concern the load balancer resources feature
     */
    class MockKafkaAssemblyOperatorForLoadBalancerTests extends KafkaAssemblyOperator {
        public MockKafkaAssemblyOperatorForLoadBalancerTests(
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
                    .compose(state -> state.kafkaLoadBalancerServicesReady())
                    .map((Void) null);
        }
    }

    private ResourceOperatorSupplier prepareResourceOperatorSupplier(Kafka kafka) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the CRD Operator for Kafka resources
        CrdOperator mockKafkaOps = supplier.kafkaOperator;
        when(mockKafkaOps.getAsync(eq(NAMESPACE), eq(NAME))).thenReturn(Future.succeededFuture(kafka));
        when(mockKafkaOps.get(eq(NAMESPACE), eq(NAME))).thenReturn(kafka);
        when(mockKafkaOps.updateStatusAsync(any(), any(Kafka.class))).thenReturn(Future.succeededFuture());

        // Mock the KafkaSet operations
        StatefulSetOperator mockStsOps = supplier.stsOperations;
        when(mockStsOps.getAsync(eq(NAMESPACE), eq(KafkaCluster.kafkaClusterName(NAME)))).thenReturn(Future.succeededFuture());

        // Mock the Pod operations
        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(emptyList()));

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
        when(mockServiceOperator.getAsync(NAMESPACE, NAME + "-kafka-external-bootstrap")).thenReturn(Future.succeededFuture(mockServiceBootstrap));
        when(mockServiceOperator.getAsync(NAMESPACE, NAME + "-kafka-0")).thenReturn(Future.succeededFuture(mockServiceBroker0));
        when(mockServiceOperator.getAsync(NAMESPACE, NAME + "-kafka-1")).thenReturn(Future.succeededFuture(mockServiceBroker1));
        when(mockServiceOperator.getAsync(NAMESPACE, NAME + "-kafka-2")).thenReturn(Future.succeededFuture(mockServiceBroker2));

        return supplier;
    }
}