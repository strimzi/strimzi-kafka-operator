/*
 * Copyright Strimzi authors.
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
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

public class KafkaStatusTest {
    private final KubernetesVersion kubernetesVersion = KubernetesVersion.V1_11;
    private final MockCertManager certManager = new MockCertManager();
    private final ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig();
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private final String namespace = "testns";
    private final String clusterName = "testkafka";
    protected static Vertx vertx;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
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

        kao.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName), kafka).setHandler(res -> {
            assertThat(res.succeeded(), is(true));

            assertThat(kafkaCaptor.getValue(), is(notNullValue()));
            assertThat(kafkaCaptor.getValue().getStatus(), is(notNullValue()));
            KafkaStatus status = kafkaCaptor.getValue().getStatus();

            assertThat(status.getListeners().size(), is(2));
            assertThat(status.getListeners().get(0).getType(), is("plain"));
            assertThat(status.getListeners().get(0).getAddresses().get(0).getHost(), is("my-service.my-namespace.svc"));
            assertThat(status.getListeners().get(0).getAddresses().get(0).getPort(), is(new Integer(9092)));
            assertThat(status.getListeners().get(1).getType(), is("external"));
            assertThat(status.getListeners().get(1).getAddresses().get(0).getHost(),  is("my-route-address.domain.tld"));
            assertThat(status.getListeners().get(1).getAddresses().get(0).getPort(), is(new Integer(443)));

            assertThat(status.getConditions().size(), is(1));
            assertThat(status.getConditions().get(0).getType(), is("Ready"));
            assertThat(status.getConditions().get(0).getStatus(), is("True"));

            assertThat(status.getObservedGeneration(), is(2L));
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

        kao.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName), kafka).setHandler(res -> {
            assertThat(res.succeeded(), is(true));
            // The status should not change => we test that updateStatusAsync was not called
            assertThat(kafkaCaptor.getAllValues().size(), is(0));
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

        kao.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName), kafka).setHandler(res -> {
            assertThat(res.succeeded(), is(false));

            assertThat(kafkaCaptor.getValue(), is(notNullValue()));
            assertThat(kafkaCaptor.getValue().getStatus(), is(notNullValue()));
            KafkaStatus status = kafkaCaptor.getValue().getStatus();

            assertThat(status.getListeners().size(), is(1));
            assertThat(status.getListeners().get(0).getType(), is("plain"));
            assertThat(status.getListeners().get(0).getAddresses().get(0).getHost(), is("my-service.my-namespace.svc"));
            assertThat(status.getListeners().get(0).getAddresses().get(0).getPort(), is(new Integer(9092)));

            assertThat(status.getConditions().size(), is(1));
            assertThat(status.getConditions().get(0).getType(), is("NotReady"));
            assertThat(status.getConditions().get(0).getStatus(), is("True"));
            assertThat(status.getConditions().get(0).getReason(), is(exception.getClass().getSimpleName()));
            assertThat(status.getConditions().get(0).getMessage(), is(exception.getMessage()));

            assertThat(status.getObservedGeneration(), is(2L));
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

        kao.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName), kafka).setHandler(res -> {
            assertThat(res.succeeded(), is(false));

            assertThat(kafkaCaptor.getValue(), is(notNullValue()));
            assertThat(kafkaCaptor.getValue().getStatus(), is(notNullValue()));
            KafkaStatus status = kafkaCaptor.getValue().getStatus();

            assertThat(status.getListeners().size(), is(1));
            assertThat(status.getListeners().get(0).getType(), is("plain"));
            assertThat(status.getListeners().get(0).getAddresses().get(0).getHost(), is("my-service.my-namespace.svc"));
            assertThat(status.getListeners().get(0).getAddresses().get(0).getPort(), is(new Integer(9092)));

            assertThat(status.getConditions().size(), is(1));
            assertThat(status.getConditions().get(0).getType(), is("NotReady"));
            assertThat(status.getConditions().get(0).getStatus(), is("True"));
            assertThat(status.getConditions().get(0).getReason(), is("RuntimeException"));
            assertThat(status.getConditions().get(0).getMessage(), is("Something went wrong"));

            assertThat(status.getObservedGeneration(), is(2L));
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
