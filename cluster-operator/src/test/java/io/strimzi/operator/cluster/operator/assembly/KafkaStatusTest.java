/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.status.ConditionBuilder;
import io.strimzi.api.kafka.model.status.KafkaStatus;
import io.strimzi.api.kafka.model.status.ListenerAddressBuilder;
import io.strimzi.api.kafka.model.status.ListenerStatus;
import io.strimzi.api.kafka.model.status.ListenerStatusBuilder;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.ResourceType;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.StringReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import static io.strimzi.test.TestUtils.map;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

public class KafkaStatusTest {
    private final KubernetesVersion kubernetesVersion = KubernetesVersion.V1_11;
    private final MockCertManager certManager = new MockCertManager();
    private final ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig();
    private static final KafkaVersion.Lookup VERSIONS = new KafkaVersion.Lookup(
            new StringReader(
                    "2.0.0  default  2.0  2.0  1234567890abcdef\n" +
                            "2.0.1           2.0  2.0  1234567890abcdef\n" +
                            "2.1.0           2.1  2.1  1234567890abcdef\n"),
            map("2.0.0", "strimzi/kafka:0.8.0-kafka-2.0.0",
                    "2.0.1", "strimzi/kafka:0.8.0-kafka-2.0.1",
                    "2.1.0", "strimzi/kafka:0.8.0-kafka-2.1.0"),
            singletonMap("2.0.0", "kafka-connect"),
            singletonMap("2.0.0", "kafka-connect-s2i"),
            singletonMap("2.0.0", "kafka-mirror-maker-s2i")) { };
    private final String namespace = "testns";
    private final String clusterName = "testkafka";
    protected static Vertx vertx;

    @BeforeClass
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterClass
    public static void after() {
        vertx.close();
    }

    public Kafka getKafkaCrd() throws ParseException {
        return new KafkaBuilder()
                .withNewMetadata()
                    .withName(clusterName)
                    .withNamespace(namespace)
                    .withGeneration(2L)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withNewListeners()
                            .withNewPlain()
                            .endPlain()
                        .endListeners()
                .withNewEphemeralStorage()
                .endEphemeralStorage()
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                .endSpec()
                .withNewStatus()
                    .withObservedGeneration(1L)
                    .withConditions(new ConditionBuilder()
                            .withNewLastTransitionTime(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse("2011-01-01 00:00:00")))
                            .withNewType("NotReady")
                            .withNewStatus("True")
                            .build())
                .endStatus()
                .build();
    }

    @Test
    public void testStatusAfterSuccessfulReconciliationWithPreviousFailure() throws ParseException {
        Kafka kafka = getKafkaCrd();
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the Kafka Operator
        CrdOperator mockKafkaOps = supplier.kafkaOperator;

        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(getKafkaCrd()));

        ArgumentCaptor<Kafka> kafkaCaptor = ArgumentCaptor.forClass(Kafka.class);
        when(mockKafkaOps.updateStatusAsync(kafkaCaptor.capture())).thenReturn(Future.succeededFuture());

        MockWorkingKafkaAssemblyOperator kao = new MockWorkingKafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                supplier,
                config);

        kao.createOrUpdate(new Reconciliation("test-trigger", ResourceType.KAFKA, namespace, clusterName), kafka).setHandler(res -> {
            assertTrue(res.succeeded());

            assertNotNull(kafkaCaptor.getValue());
            assertNotNull(kafkaCaptor.getValue().getStatus());
            KafkaStatus status = kafkaCaptor.getValue().getStatus();

            assertEquals(2, status.getListeners().size());
            assertEquals("plain", status.getListeners().get(0).getType());
            assertEquals("my-service.my-namespace.svc", status.getListeners().get(0).getAddresses().get(0).getHost());
            assertEquals(new Integer(9092), status.getListeners().get(0).getAddresses().get(0).getPort());
            assertEquals("external", status.getListeners().get(1).getType());
            assertEquals("my-route-address.domain.tld", status.getListeners().get(1).getAddresses().get(0).getHost());
            assertEquals(new Integer(443), status.getListeners().get(1).getAddresses().get(0).getPort());

            assertEquals(1, status.getConditions().size());
            assertEquals("Ready", status.getConditions().get(0).getType());
            assertEquals("True", status.getConditions().get(0).getStatus());

            assertEquals(2L, status.getObservedGeneration());
        });
    }

    @Test
    public void testStatusAfterSuccessfulReconciliationWithPreviousSuccess() throws ParseException {
        Kafka kafka = getKafkaCrd();
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the Kafka Operator
        CrdOperator mockKafkaOps = supplier.kafkaOperator;

        Kafka readyKafka = new KafkaBuilder(kafka)
                .editStatus()
                    .withObservedGeneration(2L)
                    .editCondition(0)
                        .withType("Ready")
                    .endCondition()
                    .withListeners(new ListenerStatusBuilder()
                            .withNewType("plain")
                            .withAddresses(new ListenerAddressBuilder()
                                    .withHost("my-service.my-namespace.svc")
                                    .withPort(9092)
                                    .build())
                            .build(),
                            new ListenerStatusBuilder()
                                    .withNewType("external")
                                    .withAddresses(new ListenerAddressBuilder()
                                            .withHost("my-route-address.domain.tld")
                                            .withPort(443)
                                            .build())
                                    .build())
                .endStatus()
                .build();

        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(readyKafka));

        ArgumentCaptor<Kafka> kafkaCaptor = ArgumentCaptor.forClass(Kafka.class);
        when(mockKafkaOps.updateStatusAsync(kafkaCaptor.capture())).thenReturn(Future.succeededFuture());

        MockWorkingKafkaAssemblyOperator kao = new MockWorkingKafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                supplier,
                config);

        kao.createOrUpdate(new Reconciliation("test-trigger", ResourceType.KAFKA, namespace, clusterName), kafka).setHandler(res -> {
            assertTrue(res.succeeded());
            // The status should not change => we test that updateStatusAsync was not called
            assertEquals(0, kafkaCaptor.getAllValues().size());
        });
    }

    @Test
    public void testStatusAfterFailedReconciliationWithPreviousFailure() throws ParseException {
        testStatusAfterFailedReconciliationWithPreviousFailure(new RuntimeException("Something went wrong"));
    }

    @Test
    public void testStatusAfterFailedReconciliationWithPreviousFailure_NPE() throws ParseException {
        testStatusAfterFailedReconciliationWithPreviousFailure(new NullPointerException());
    }

    public void testStatusAfterFailedReconciliationWithPreviousFailure(Throwable exception) throws ParseException {
        Kafka kafka = getKafkaCrd();
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the Kafka Operator
        CrdOperator mockKafkaOps = supplier.kafkaOperator;

        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(getKafkaCrd()));

        ArgumentCaptor<Kafka> kafkaCaptor = ArgumentCaptor.forClass(Kafka.class);
        when(mockKafkaOps.updateStatusAsync(kafkaCaptor.capture())).thenReturn(Future.succeededFuture());

        MockFailingKafkaAssemblyOperator kao = new MockFailingKafkaAssemblyOperator(
                exception,
                vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                supplier,
                config);

        kao.createOrUpdate(new Reconciliation("test-trigger", ResourceType.KAFKA, namespace, clusterName), kafka).setHandler(res -> {
            assertFalse(res.succeeded());

            assertNotNull(kafkaCaptor.getValue());
            assertNotNull(kafkaCaptor.getValue().getStatus());
            KafkaStatus status = kafkaCaptor.getValue().getStatus();

            assertEquals(1, status.getListeners().size());
            assertEquals("plain", status.getListeners().get(0).getType());
            assertEquals("my-service.my-namespace.svc", status.getListeners().get(0).getAddresses().get(0).getHost());
            assertEquals(new Integer(9092), status.getListeners().get(0).getAddresses().get(0).getPort());

            assertEquals(1, status.getConditions().size());
            assertEquals("NotReady", status.getConditions().get(0).getType());
            assertEquals("True", status.getConditions().get(0).getStatus());
            assertEquals(exception.getClass().getSimpleName(), status.getConditions().get(0).getReason());
            assertEquals(exception.getMessage(), status.getConditions().get(0).getMessage());

            assertEquals(2L, status.getObservedGeneration());
        });
    }

    @Test
    public void testStatusAfterFailedReconciliationWithPreviousSuccess() throws ParseException {
        Kafka kafka = getKafkaCrd();
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the Kafka Operator
        CrdOperator mockKafkaOps = supplier.kafkaOperator;

        Kafka readyKafka = new KafkaBuilder(kafka)
                .editStatus()
                    .withObservedGeneration(1L)
                    .editCondition(0)
                        .withType("Ready")
                    .endCondition()
                    .withListeners(new ListenerStatusBuilder()
                                    .withNewType("plain")
                                    .withAddresses(new ListenerAddressBuilder()
                                            .withHost("my-service.my-namespace.svc")
                                            .withPort(9092)
                                            .build())
                                    .build(),
                            new ListenerStatusBuilder()
                                    .withNewType("external")
                                    .withAddresses(new ListenerAddressBuilder()
                                            .withHost("my-route-address.domain.tld")
                                            .withPort(443)
                                            .build())
                                    .build())
                .endStatus()
                .build();

        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(readyKafka));

        ArgumentCaptor<Kafka> kafkaCaptor = ArgumentCaptor.forClass(Kafka.class);
        when(mockKafkaOps.updateStatusAsync(kafkaCaptor.capture())).thenReturn(Future.succeededFuture());

        MockFailingKafkaAssemblyOperator kao = new MockFailingKafkaAssemblyOperator(
                new RuntimeException("Something went wrong"),
                vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                supplier,
                config);

        kao.createOrUpdate(new Reconciliation("test-trigger", ResourceType.KAFKA, namespace, clusterName), kafka).setHandler(res -> {
            assertFalse(res.succeeded());

            assertNotNull(kafkaCaptor.getValue());
            assertNotNull(kafkaCaptor.getValue().getStatus());
            KafkaStatus status = kafkaCaptor.getValue().getStatus();

            assertEquals(1, status.getListeners().size());
            assertEquals("plain", status.getListeners().get(0).getType());
            assertEquals("my-service.my-namespace.svc", status.getListeners().get(0).getAddresses().get(0).getHost());
            assertEquals(new Integer(9092), status.getListeners().get(0).getAddresses().get(0).getPort());

            assertEquals(1, status.getConditions().size());
            assertEquals("NotReady", status.getConditions().get(0).getType());
            assertEquals("True", status.getConditions().get(0).getStatus());
            assertEquals("RuntimeException", status.getConditions().get(0).getReason());
            assertEquals("Something went wrong", status.getConditions().get(0).getMessage());

            assertEquals(2L, status.getObservedGeneration());
        });
    }

    // This allows to test the status handling when reconciliation succeeds
    class MockWorkingKafkaAssemblyOperator extends KafkaAssemblyOperator  {
        public MockWorkingKafkaAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa, CertManager certManager, ResourceOperatorSupplier supplier, ClusterOperatorConfig config) {
            super(vertx, pfa, certManager, supplier, config);
        }

        @Override
        Future<Void> reconcile(ReconciliationState reconcileState)  {
            ListenerStatus ls = new ListenerStatusBuilder()
                    .withNewType("plain")
                    .withAddresses(new ListenerAddressBuilder()
                            .withHost("my-service.my-namespace.svc")
                            .withPort(9092)
                            .build())
                    .build();

            ListenerStatus ls2 = new ListenerStatusBuilder()
                    .withNewType("external")
                    .withAddresses(new ListenerAddressBuilder()
                            .withHost("my-route-address.domain.tld")
                            .withPort(443)
                            .build())
                    .build();

            List<ListenerStatus> listeners = new ArrayList<>(2);
            listeners.add(ls);
            listeners.add(ls2);

            reconcileState.kafkaStatus.setListeners(listeners);

            return Future.succeededFuture();
        }
    }

    // This allows to test the status handling when reconciliation succeeds
    class MockFailingKafkaAssemblyOperator extends KafkaAssemblyOperator  {
        private final Throwable exception;

        public MockFailingKafkaAssemblyOperator(Throwable exception, Vertx vertx, PlatformFeaturesAvailability pfa, CertManager certManager, ResourceOperatorSupplier supplier, ClusterOperatorConfig config) {
            super(vertx, pfa, certManager, supplier, config);
            this.exception = exception;
        }

        @Override
        Future<Void> reconcile(ReconciliationState reconcileState)  {
            ListenerStatus ls = new ListenerStatusBuilder()
                    .withNewType("plain")
                    .withAddresses(new ListenerAddressBuilder()
                            .withHost("my-service.my-namespace.svc")
                            .withPort(9092)
                            .build())
                    .build();

            reconcileState.kafkaStatus.setListeners(singletonList(ls));

            return Future.failedFuture(exception);
        }
    }
}
