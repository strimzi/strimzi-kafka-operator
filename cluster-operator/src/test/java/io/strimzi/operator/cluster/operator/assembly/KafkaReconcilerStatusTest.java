/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeAddress;
import io.fabric8.kubernetes.api.model.NodeBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.listener.NodeAddressType;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerConfigurationBroker;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerConfigurationBrokerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.KafkaStatus;
import io.strimzi.api.kafka.model.status.ListenerStatusBuilder;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperator;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.ClientsCa;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.KafkaVersionChange;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.NodeOperator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.platform.KubernetesVersion;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaReconcilerStatusTest {
    private final static String NAMESPACE = "testns";
    private final static String CLUSTER_NAME = "testkafka";
    private final static KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private final static PlatformFeaturesAvailability PFA = new PlatformFeaturesAvailability(true, KubernetesVersion.V1_22);
    private final static KafkaVersionChange VERSION_CHANGE = new KafkaVersionChange(
            VERSIONS.defaultVersion(),
            VERSIONS.defaultVersion(),
            VERSIONS.defaultVersion().protocolVersion(),
            VERSIONS.defaultVersion().messageVersion()
    );
    private final static ClusterOperatorConfig CO_CONFIG = ResourceUtils.dummyClusterOperatorConfig();
    private final static ClusterCa CLUSTER_CA = new ClusterCa(
            Reconciliation.DUMMY_RECONCILIATION,
            new OpenSslCertManager(),
            new PasswordGenerator(10, "a", "a"),
            CLUSTER_NAME,
            ResourceUtils.createInitialCaCertSecret(NAMESPACE, CLUSTER_NAME, AbstractModel.clusterCaCertSecretName(CLUSTER_NAME), MockCertManager.clusterCaCert(), MockCertManager.clusterCaCertStore(), "123456"),
            ResourceUtils.createInitialCaKeySecret(NAMESPACE, CLUSTER_NAME, AbstractModel.clusterCaKeySecretName(CLUSTER_NAME), MockCertManager.clusterCaKey())
    );
    private final static ClientsCa CLIENTS_CA = new ClientsCa(
            Reconciliation.DUMMY_RECONCILIATION,
            new OpenSslCertManager(),
            new PasswordGenerator(10, "a", "a"),
            KafkaResources.clientsCaCertificateSecretName(CLUSTER_NAME),
            ResourceUtils.createInitialCaCertSecret(NAMESPACE, CLUSTER_NAME, AbstractModel.clusterCaCertSecretName(CLUSTER_NAME), MockCertManager.clusterCaCert(), MockCertManager.clusterCaCertStore(), "123456"),
            KafkaResources.clientsCaKeySecretName(CLUSTER_NAME),
            ResourceUtils.createInitialCaKeySecret(NAMESPACE, CLUSTER_NAME, AbstractModel.clusterCaKeySecretName(CLUSTER_NAME), MockCertManager.clusterCaKey()),
            365,
            30,
            true,
            null
    );
    private final static Kafka KAFKA = new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("tls")
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(true)
                                .build())
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                .endSpec()
                .build();

    private static Vertx vertx;
    private static WorkerExecutor sharedWorkerExecutor;

    @BeforeAll
    public static void beforeAll()  {
        vertx = Vertx.vertx();
        sharedWorkerExecutor = vertx.createSharedWorkerExecutor("kubernetes-ops-pool");
    }

    @AfterAll
    public static void afterAll()    {
        sharedWorkerExecutor.close();
        vertx.close();
    }

    @Test
    public void testKafkaReconcilerStatus(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editOrNewSpec()
                    .editOrNewKafka()
                        .withReplicas(1)
                    .endKafka()
                .endSpec()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the secrets needed for Kafka client
        SecretOperator mockSecretOps = supplier.secretOperations;
        Secret secret = new Secret();
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(ClusterOperator.secretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));

        // Run the test
        KafkaReconciler reconciler = new MockKafkaReconcilerStatusTasks(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                supplier,
                kafka
        );

        KafkaStatus status = new KafkaStatus();

        Checkpoint async = context.checkpoint();
        reconciler.reconcile(status, Clock.systemUTC()).onComplete(res -> context.verify(() -> {
            assertThat(res.succeeded(), is(true));

            // Check ClusterID
            assertThat(status.getClusterId(), is("CLUSTERID"));

            // Check model warning conditions
            assertThat(status.getConditions().size(), is(1));
            assertThat(status.getConditions().get(0).getType(), is("Warning"));
            assertThat(status.getConditions().get(0).getReason(), is("KafkaStorage"));

            async.flag();
        }));
    }

    @Test
    public void testKafkaReconcilerStatusWithSpecCheckerWarnings(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the secrets needed for Kafka client
        SecretOperator mockSecretOps = supplier.secretOperations;
        Secret secret = new Secret();
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(ClusterOperator.secretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));

        // Run the test
        KafkaReconciler reconciler = new MockKafkaReconcilerStatusTasks(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                supplier,
                KAFKA
        );

        KafkaStatus status = new KafkaStatus();

        Checkpoint async = context.checkpoint();
        reconciler.reconcile(status, Clock.systemUTC()).onComplete(res -> context.verify(() -> {
            assertThat(res.succeeded(), is(true));

            // Check model warning conditions
            assertThat(status.getConditions().size(), is(2));
            assertThat(status.getConditions().get(0).getType(), is("Warning"));
            assertThat(status.getConditions().get(0).getReason(), is("KafkaDefaultReplicationFactor"));
            assertThat(status.getConditions().get(1).getType(), is("Warning"));
            assertThat(status.getConditions().get(1).getReason(), is("KafkaMinInsyncReplicas"));

            async.flag();
        }));
    }

    @Test
    public void testKafkaReconcilerStatusWithNodePorts(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editOrNewSpec()
                    .editOrNewKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .build())
                    .endKafka()
                .endSpec()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the secrets needed for Kafka client
        SecretOperator mockSecretOps = supplier.secretOperations;
        Secret secret = new Secret();
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(ClusterOperator.secretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));

        // Mock Kafka broker pods
        Pod pod0 = new PodBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME + "-kafka-" + 0)
                .endMetadata()
                .withNewStatus()
                    .withHostIP("10.0.0.1")
                .endStatus()
                .build();

        Pod pod1 = new PodBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME + "-kafka-" + 1)
                .endMetadata()
                .withNewStatus()
                    .withHostIP("10.0.0.25")
                .endStatus()
                .build();

        Pod pod2 = new PodBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME + "-kafka-" + 2)
                .endMetadata()
                .withNewStatus()
                    .withHostIP("10.0.0.13")
                .endStatus()
                .build();

        List<Pod> pods = new ArrayList<>();
        pods.add(pod0);
        pods.add(pod1);
        pods.add(pod2);

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(pods));

        // Mock Kubernetes worker nodes
        NodeOperator mockNodeOps = supplier.nodeOperator;
        when(mockNodeOps.listAsync(any(Labels.class))).thenReturn(Future.succeededFuture(kubernetesWorkerNodes()));

        // Run the test
        KafkaReconciler reconciler = new MockKafkaReconcilerStatusTasks(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                supplier,
                kafka
        );

        KafkaStatus status = new KafkaStatus();

        Checkpoint async = context.checkpoint();
        reconciler.reconcile(status, Clock.systemUTC()).onComplete(res -> context.verify(() -> {
            assertThat(res.succeeded(), is(true));

            // Check listener status
            assertThat(status.getListeners().size(), is(1));
            assertThat(status.getListeners().get(0).getName(), is("external"));
            assertThat(status.getListeners().get(0).getType(), is("external"));
            assertThat(status.getListeners().get(0).getBootstrapServers(), is("5.124.16.8:31234,50.35.18.119:31234,55.36.78.115:31234"));
            assertThat(status.getListeners().get(0).getAddresses().size(), is(3));

            // Assert the listener addresses independently on their order
            assertThat(status.getListeners().get(0).getAddresses().stream().anyMatch(a -> a.getPort() == 31234 && "5.124.16.8".equals(a.getHost())), is(true));
            assertThat(status.getListeners().get(0).getAddresses().stream().anyMatch(a -> a.getPort() == 31234 && "55.36.78.115".equals(a.getHost())), is(true));
            assertThat(status.getListeners().get(0).getAddresses().stream().anyMatch(a -> a.getPort() == 31234 && "50.35.18.119".equals(a.getHost())), is(true));

            async.flag();
        }));
    }

    @Test
    public void testKafkaReconcilerStatusWithNodePortsAndOverrides(VertxTestContext context) {
        GenericKafkaListenerConfigurationBroker broker0 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withBroker(0)
                .withAdvertisedHost("my-address-0")
                .build();

        GenericKafkaListenerConfigurationBroker broker1 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withBroker(1)
                .withAdvertisedHost("my-address-1")
                .build();

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editOrNewSpec()
                    .editOrNewKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withBrokers(broker0, broker1)
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the secrets needed for Kafka client
        SecretOperator mockSecretOps = supplier.secretOperations;
        Secret secret = new Secret();
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(ClusterOperator.secretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));

        // Mock Kafka broker pods
        Pod pod0 = new PodBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME + "-kafka-" + 0)
                .endMetadata()
                .withNewStatus()
                    .withHostIP("10.0.0.1")
                .endStatus()
                .build();

        Pod pod1 = new PodBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME + "-kafka-" + 1)
                .endMetadata()
                .withNewStatus()
                    .withHostIP("10.0.0.25")
                .endStatus()
                .build();

        Pod pod2 = new PodBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME + "-kafka-" + 2)
                .endMetadata()
                .withNewStatus()
                    .withHostIP("10.0.0.13")
                .endStatus()
                .build();

        List<Pod> pods = new ArrayList<>();
        pods.add(pod0);
        pods.add(pod1);
        pods.add(pod2);

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(pods));

        // Mock Kubernetes worker nodes
        NodeOperator mockNodeOps = supplier.nodeOperator;
        when(mockNodeOps.listAsync(any(Labels.class))).thenReturn(Future.succeededFuture(kubernetesWorkerNodes()));

        // Run the test
        KafkaReconciler reconciler = new MockKafkaReconcilerStatusTasks(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                supplier,
                kafka
        );

        KafkaStatus status = new KafkaStatus();

        Checkpoint async = context.checkpoint();
        reconciler.reconcile(status, Clock.systemUTC()).onComplete(res -> context.verify(() -> {
            assertThat(res.succeeded(), is(true));

            // Check listener status
            assertThat(status.getListeners().size(), is(1));
            assertThat(status.getListeners().get(0).getName(), is("external"));
            assertThat(status.getListeners().get(0).getType(), is("external"));
            assertThat(status.getListeners().get(0).getBootstrapServers(), is("5.124.16.8:31234,my-address-0:31234,my-address-1:31234"));
            assertThat(status.getListeners().get(0).getAddresses().size(), is(3));

            // Assert the listener addresses independently on their order
            assertThat(status.getListeners().get(0).getAddresses().stream().anyMatch(a -> a.getPort() == 31234 && "my-address-0".equals(a.getHost())), is(true));
            assertThat(status.getListeners().get(0).getAddresses().stream().anyMatch(a -> a.getPort() == 31234 && "my-address-1".equals(a.getHost())), is(true));
            assertThat(status.getListeners().get(0).getAddresses().stream().anyMatch(a -> a.getPort() == 31234 && "5.124.16.8".equals(a.getHost())), is(true));

            async.flag();
        }));
    }

    @Test
    public void testKafkaReconcilerStatusWithNodePortsWithPreferredAddressType(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editOrNewSpec()
                    .editOrNewKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withPreferredNodePortAddressType(NodeAddressType.INTERNAL_DNS)
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the secrets needed for Kafka client
        SecretOperator mockSecretOps = supplier.secretOperations;
        Secret secret = new Secret();
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(ClusterOperator.secretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));

        // Mock Kafka broker pods
        Pod pod0 = new PodBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME + "-kafka-" + 0)
                .endMetadata()
                .withNewStatus()
                    .withHostIP("10.0.0.1")
                .endStatus()
                .build();

        Pod pod1 = new PodBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME + "-kafka-" + 1)
                .endMetadata()
                .withNewStatus()
                    .withHostIP("10.0.0.25")
                .endStatus()
                .build();

        Pod pod2 = new PodBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME + "-kafka-" + 2)
                .endMetadata()
                .withNewStatus()
                    .withHostIP("10.0.0.13")
                .endStatus()
                .build();

        List<Pod> pods = new ArrayList<>();
        pods.add(pod0);
        pods.add(pod1);
        pods.add(pod2);

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(pods));

        // Mock Kubernetes worker nodes
        NodeOperator mockNodeOps = supplier.nodeOperator;
        when(mockNodeOps.listAsync(any(Labels.class))).thenReturn(Future.succeededFuture(kubernetesWorkerNodes()));

        // Run the test
        KafkaReconciler reconciler = new MockKafkaReconcilerStatusTasks(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                supplier,
                kafka
        );

        KafkaStatus status = new KafkaStatus();

        Checkpoint async = context.checkpoint();
        reconciler.reconcile(status, Clock.systemUTC()).onComplete(res -> context.verify(() -> {
            assertThat(res.succeeded(), is(true));

            // Check listener status
            assertThat(status.getListeners().size(), is(1));
            assertThat(status.getListeners().get(0).getName(), is("external"));
            assertThat(status.getListeners().get(0).getType(), is("external"));
            assertThat(status.getListeners().get(0).getBootstrapServers(), is("node-0.my-kube:31234,node-1.my-kube:31234,node-3.my-kube:31234"));
            assertThat(status.getListeners().get(0).getAddresses().size(), is(3));

            // Assert the listener addresses independently on their order
            assertThat(status.getListeners().get(0).getAddresses().stream().anyMatch(a -> a.getPort() == 31234 && "node-0.my-kube".equals(a.getHost())), is(true));
            assertThat(status.getListeners().get(0).getAddresses().stream().anyMatch(a -> a.getPort() == 31234 && "node-1.my-kube".equals(a.getHost())), is(true));
            assertThat(status.getListeners().get(0).getAddresses().stream().anyMatch(a -> a.getPort() == 31234 && "node-3.my-kube".equals(a.getHost())), is(true));

            async.flag();
        }));
    }

    @Test
    public void testKafkaReconcilerStatusWithNodePortsOnSameNode(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editOrNewSpec()
                    .editOrNewKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .build())
                    .endKafka()
                .endSpec()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the secrets needed for Kafka client
        SecretOperator mockSecretOps = supplier.secretOperations;
        Secret secret = new Secret();
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(ClusterOperator.secretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));

        // Mock Kafka broker pods
        Pod pod0 = new PodBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME + "-kafka-" + 0)
                .endMetadata()
                .withNewStatus()
                    .withHostIP("10.0.0.1")
                .endStatus()
                .build();

        Pod pod1 = new PodBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME + "-kafka-" + 1)
                .endMetadata()
                .withNewStatus()
                    .withHostIP("10.0.0.1")
                .endStatus()
                .build();

        Pod pod2 = new PodBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME + "-kafka-" + 2)
                .endMetadata()
                .withNewStatus()
                    .withHostIP("10.0.0.1")
                .endStatus()
                .build();

        List<Pod> pods = new ArrayList<>();
        pods.add(pod0);
        pods.add(pod1);
        pods.add(pod2);

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(pods));

        // Mock Kubernetes worker nodes
        NodeOperator mockNodeOps = supplier.nodeOperator;
        when(mockNodeOps.listAsync(any(Labels.class))).thenReturn(Future.succeededFuture(kubernetesWorkerNodes()));

        // Run the test
        KafkaReconciler reconciler = new MockKafkaReconcilerStatusTasks(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                supplier,
                kafka
        );

        KafkaStatus status = new KafkaStatus();

        Checkpoint async = context.checkpoint();
        reconciler.reconcile(status, Clock.systemUTC()).onComplete(res -> context.verify(() -> {
            assertThat(res.succeeded(), is(true));

            // Check listener status
            assertThat(status.getListeners().size(), is(1));
            assertThat(status.getListeners().get(0).getName(), is("external"));
            assertThat(status.getListeners().get(0).getType(), is("external"));
            assertThat(status.getListeners().get(0).getBootstrapServers(), is("50.35.18.119:31234"));
            assertThat(status.getListeners().get(0).getAddresses().size(), is(1));
            assertThat(status.getListeners().get(0).getAddresses().get(0).getPort(), is(31234));
            assertThat(status.getListeners().get(0).getAddresses().get(0).getHost(), is("50.35.18.119"));

            async.flag();
        }));
    }

    @Test
    public void testKafkaReconcilerStatusWithNodePortsAndMissingNode(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editOrNewSpec()
                    .editOrNewKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .build())
                    .endKafka()
                .endSpec()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the secrets needed for Kafka client
        SecretOperator mockSecretOps = supplier.secretOperations;
        Secret secret = new Secret();
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(ClusterOperator.secretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));

        // Mock Kafka broker pods
        Pod pod0 = new PodBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME + "-kafka-" + 0)
                .endMetadata()
                .withNewStatus()
                    .withHostIP("10.0.0.5")
                .endStatus()
                .build();

        Pod pod1 = new PodBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME + "-kafka-" + 1)
                .endMetadata()
                .withNewStatus()
                    .withHostIP("10.0.0.5")
                .endStatus()
                .build();

        Pod pod2 = new PodBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME + "-kafka-" + 2)
                .endMetadata()
                .withNewStatus()
                    .withHostIP("10.0.0.5")
                .endStatus()
                .build();

        List<Pod> pods = new ArrayList<>();
        pods.add(pod0);
        pods.add(pod1);
        pods.add(pod2);

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(eq(NAMESPACE), any(Labels.class))).thenReturn(Future.succeededFuture(pods));

        // Mock Kubernetes worker nodes
        NodeOperator mockNodeOps = supplier.nodeOperator;
        when(mockNodeOps.listAsync(any(Labels.class))).thenReturn(Future.succeededFuture(kubernetesWorkerNodes()));

        // Run the test
        KafkaReconciler reconciler = new MockKafkaReconcilerStatusTasks(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                supplier,
                kafka
        );

        KafkaStatus status = new KafkaStatus();

        Checkpoint async = context.checkpoint();
        reconciler.reconcile(status, Clock.systemUTC()).onComplete(res -> context.verify(() -> {
            assertThat(res.succeeded(), is(true));

            // Check listener status
            assertThat(status.getListeners().size(), is(1));
            assertThat(status.getListeners().get(0).getName(), is("external"));
            assertThat(status.getListeners().get(0).getType(), is("external"));
            assertThat(status.getListeners().get(0).getBootstrapServers(), is(nullValue()));
            assertThat(status.getListeners().get(0).getAddresses(), is(List.of()));

            async.flag();
        }));
    }

    private static List<Node> kubernetesWorkerNodes()    {
        Node node0 = new NodeBuilder()
                .withNewMetadata()
                    .withName("node-0")
                .endMetadata()
                .withNewStatus()
                    .withAddresses(new NodeAddress("50.35.18.119", "ExternalIP"),
                            new NodeAddress("node-0.my-kube", "InternalDNS"),
                            new NodeAddress("10.0.0.1", "InternalIP"),
                            new NodeAddress("node-0", "Hostname"))
                .endStatus()
                .build();

        Node node1 = new NodeBuilder()
                .withNewMetadata()
                    .withName("node-1")
                .endMetadata()
                .withNewStatus()
                    .withAddresses(new NodeAddress("55.36.78.115", "ExternalIP"),
                            new NodeAddress("node-1.my-kube", "InternalDNS"),
                            new NodeAddress("10.0.0.25", "InternalIP"),
                            new NodeAddress("node-1", "Hostname"))
                .endStatus()
                .build();

        Node node2 = new NodeBuilder()
                .withNewMetadata()
                    .withName("node-2")
                .endMetadata()
                .withNewStatus()
                    .withAddresses(new NodeAddress("35.15.152.9", "ExternalIP"),
                            new NodeAddress("node-2.my-kube", "InternalDNS"),
                            new NodeAddress("10.0.0.16", "InternalIP"),
                            new NodeAddress("node-2", "Hostname"))
                .endStatus()
                .build();

        Node node3 = new NodeBuilder()
                .withNewMetadata()
                    .withName("node-3")
                .endMetadata()
                .withNewStatus()
                    .withAddresses(new NodeAddress("5.124.16.8", "ExternalIP"),
                            new NodeAddress("node-3.my-kube", "InternalDNS"),
                            new NodeAddress("10.0.0.13", "InternalIP"),
                            new NodeAddress("node-3", "Hostname"))
                .endStatus()
                .build();

        List<Node> nodes = new ArrayList<>();
        nodes.add(node0);
        nodes.add(node1);
        nodes.add(node2);
        nodes.add(node3);

        return nodes;
    }

    static class MockKafkaReconcilerStatusTasks extends KafkaReconciler {
        private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(MockKafkaReconcilerStatusTasks.class.getName());

        public MockKafkaReconcilerStatusTasks(Reconciliation reconciliation, ResourceOperatorSupplier supplier, Kafka kafkaCr) {
            super(reconciliation, kafkaCr, null, 0, CLUSTER_CA, CLIENTS_CA, VERSION_CHANGE, CO_CONFIG, supplier, PFA, vertx);
        }

        @Override
        public Future<Void> reconcile(KafkaStatus kafkaStatus, Clock clock)    {
            return modelWarnings(kafkaStatus)
                    .compose(i -> listeners())
                    .compose(i -> clusterId(kafkaStatus))
                    .compose(i -> nodePortExternalListenerStatus())
                    .compose(i -> addListenersToKafkaStatus(kafkaStatus))
                    .recover(error -> {
                        LOGGER.errorCr(reconciliation, "Reconciliation failed", error);
                        return Future.failedFuture(error);
                    });
        }

        @Override
        protected Future<Void> listeners()  {
            listenerReconciliationResults = new KafkaListenersReconciler.ReconciliationResult();
            listenerReconciliationResults.bootstrapNodePorts.put("external-9094", 31234);
            listenerReconciliationResults.listenerStatuses.add(new ListenerStatusBuilder().withName("external").build());

            return Future.succeededFuture();
        }
    }
}
