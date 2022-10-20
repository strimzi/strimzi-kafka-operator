/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.ConditionBuilder;
import io.strimzi.api.kafka.model.status.KafkaStatus;
import io.strimzi.api.kafka.model.status.ListenerAddressBuilder;
import io.strimzi.api.kafka.model.status.ListenerStatus;
import io.strimzi.api.kafka.model.status.ListenerStatusBuilder;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.StatusUtils;
import io.strimzi.platform.KubernetesVersion;
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

import java.text.ParseException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@SuppressWarnings({"checkstyle:ClassFanOutComplexity", "checkstyle:ClassDataAbstractionCoupling"})
@ExtendWith(VertxExtension.class)
public class KafkaStatusTest {
    private final KubernetesVersion kubernetesVersion = KubernetesVersion.MINIMAL_SUPPORTED_VERSION;
    private final MockCertManager certManager = new MockCertManager();
    private final PasswordGenerator passwordGenerator = new PasswordGenerator(10, "a", "a");
    private final ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);
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
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("plain")
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
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
                .withNewStatus()
                    .withObservedGeneration(1L)
                    .withConditions(new ConditionBuilder()
                            .withLastTransitionTime(StatusUtils.iso8601(Instant.parse("2011-01-01T00:00:00Z")))
                            .withType("NotReady")
                            .withStatus("True")
                            .build())
                .endStatus()
                .build();
    }

    @Test
    public void testStatusAfterSuccessfulReconciliationWithPreviousFailure(VertxTestContext context) throws ParseException {
        Kafka kafka = getKafkaCrd();
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the Kafka Operator
        CrdOperator<KubernetesClient, Kafka, KafkaList> mockKafkaOps = supplier.kafkaOperator;

        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(getKafkaCrd()));
        when(mockKafkaOps.get(eq(namespace), eq(clusterName))).thenReturn(kafka);

        ArgumentCaptor<Kafka> kafkaCaptor = ArgumentCaptor.forClass(Kafka.class);
        when(mockKafkaOps.updateStatusAsync(any(), kafkaCaptor.capture())).thenReturn(Future.succeededFuture());

        MockWorkingKafkaAssemblyOperator kao = new MockWorkingKafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config);

        Checkpoint async = context.checkpoint();
        kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName)).onComplete(res -> {
            assertThat(res.succeeded(), is(true));

            assertThat(kafkaCaptor.getValue(), is(notNullValue()));
            assertThat(kafkaCaptor.getValue().getStatus(), is(notNullValue()));
            KafkaStatus status = kafkaCaptor.getValue().getStatus();

            assertThat(status.getListeners().size(), is(2));
            assertThat(status.getListeners().get(0).getType(), is("plain"));
            assertThat(status.getListeners().get(0).getName(), is("plain"));
            assertThat(status.getListeners().get(0).getAddresses().get(0).getHost(), is("my-service.my-namespace.svc"));
            assertThat(status.getListeners().get(0).getAddresses().get(0).getPort(), is(9092));
            assertThat(status.getListeners().get(0).getBootstrapServers(), is("my-service.my-namespace.svc:9092"));
            assertThat(status.getListeners().get(1).getType(), is("external"));
            assertThat(status.getListeners().get(1).getName(), is("external"));
            assertThat(status.getListeners().get(1).getAddresses().get(0).getHost(), is("my-route-address.domain.tld"));
            assertThat(status.getListeners().get(1).getAddresses().get(0).getPort(), is(443));
            assertThat(status.getListeners().get(1).getBootstrapServers(), is("my-route-address.domain.tld:443"));

            assertThat(status.getConditions().size(), is(1));
            assertThat(status.getConditions().get(0).getType(), is("Ready"));
            assertThat(status.getConditions().get(0).getStatus(), is("True"));

            assertThat(status.getObservedGeneration(), is(2L));

            async.flag();
        });
    }

    @Test
    public void testPauseReconciliationsStatus(VertxTestContext context) throws ParseException {
        Kafka kafka = getKafkaCrd();
        kafka.getMetadata().setAnnotations(singletonMap("strimzi.io/pause-reconciliation", Boolean.toString(true)));
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the Kafka Operator
        CrdOperator<KubernetesClient, Kafka, KafkaList> mockKafkaOps = supplier.kafkaOperator;

        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(getKafkaCrd()));
        when(mockKafkaOps.get(eq(namespace), eq(clusterName))).thenReturn(kafka);

        ArgumentCaptor<Kafka> kafkaCaptor = ArgumentCaptor.forClass(Kafka.class);
        when(mockKafkaOps.updateStatusAsync(any(), kafkaCaptor.capture())).thenReturn(Future.succeededFuture());

        MockWorkingKafkaAssemblyOperator kao = new MockWorkingKafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config);

        Checkpoint async = context.checkpoint();
        kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName)).onComplete(res -> {
            assertThat(res.succeeded(), is(true));

            assertThat(kafkaCaptor.getValue(), is(notNullValue()));
            assertThat(kafkaCaptor.getValue().getStatus(), is(notNullValue()));
            KafkaStatus status = kafkaCaptor.getValue().getStatus();

            assertThat(status.getConditions().size(), is(1));
            assertThat(status.getConditions().get(0).getStatus(), is("True"));
            assertThat(status.getConditions().get(0).getType(), is("ReconciliationPaused"));
            assertThat(status.getObservedGeneration(), is(1L));
            async.flag();
        });
    }

    @Test
    public void testStatusAfterSuccessfulReconciliationWithPreviousSuccess(VertxTestContext context) throws ParseException {
        Kafka kafka = getKafkaCrd();
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the Kafka Operator
        CrdOperator<KubernetesClient, Kafka, KafkaList> mockKafkaOps = supplier.kafkaOperator;

        Kafka readyKafka = new KafkaBuilder(kafka)
                .editStatus()
                    .withObservedGeneration(2L)
                    .editCondition(0)
                        .withType("Ready")
                    .endCondition()
                    .withListeners(new ListenerStatusBuilder()
                            .withName("plain")
                            .withAddresses(new ListenerAddressBuilder()
                                    .withHost("my-service.my-namespace.svc")
                                    .withPort(9092)
                                    .build())
                            .build(),
                            new ListenerStatusBuilder()
                                    .withName("external")
                                    .withAddresses(new ListenerAddressBuilder()
                                            .withHost("my-route-address.domain.tld")
                                            .withPort(443)
                                            .build())
                                    .build())
                .endStatus()
                .build();

        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(readyKafka));

        ArgumentCaptor<Kafka> kafkaCaptor = ArgumentCaptor.forClass(Kafka.class);
        when(mockKafkaOps.updateStatusAsync(any(), kafkaCaptor.capture())).thenReturn(Future.succeededFuture());

        MockWorkingKafkaAssemblyOperator kao = new MockWorkingKafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config);

        Checkpoint async = context.checkpoint();
        kao.createOrUpdate(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName), kafka).onComplete(res -> {
            assertThat(res.succeeded(), is(true));
            // The status should not change => we test that updateStatusAsync was not called
            assertThat(kafkaCaptor.getAllValues().size(), is(0));

            async.flag();
        });
    }

    @Test
    public void testStatusAfterFailedReconciliationWithPreviousFailure(VertxTestContext context) throws ParseException {
        testStatusAfterFailedReconciliationWithPreviousFailure(context, new RuntimeException("Something went wrong"));
    }

    @Test
    public void testStatusAfterFailedReconciliationWithPreviousFailure_NPE(VertxTestContext context) throws ParseException {
        testStatusAfterFailedReconciliationWithPreviousFailure(context, new NullPointerException());
    }

    public void testStatusAfterFailedReconciliationWithPreviousFailure(VertxTestContext context, Throwable exception) throws ParseException {
        Kafka kafka = getKafkaCrd();
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the Kafka Operator
        CrdOperator<KubernetesClient, Kafka, KafkaList> mockKafkaOps = supplier.kafkaOperator;

        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(getKafkaCrd()));
        when(mockKafkaOps.get(eq(namespace), eq(clusterName))).thenReturn(kafka);

        ArgumentCaptor<Kafka> kafkaCaptor = ArgumentCaptor.forClass(Kafka.class);
        when(mockKafkaOps.updateStatusAsync(any(), kafkaCaptor.capture())).thenReturn(Future.succeededFuture());

        MockFailingKafkaAssemblyOperator kao = new MockFailingKafkaAssemblyOperator(
                exception,
                vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config);

        Checkpoint async = context.checkpoint();
        kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName)).onComplete(res -> {
            assertThat(res.succeeded(), is(false));

            assertThat(kafkaCaptor.getValue(), is(notNullValue()));
            assertThat(kafkaCaptor.getValue().getStatus(), is(notNullValue()));
            KafkaStatus status = kafkaCaptor.getValue().getStatus();

            assertThat(status.getListeners().size(), is(1));
            assertThat(status.getListeners().get(0).getType(), is("plain"));
            assertThat(status.getListeners().get(0).getName(), is("plain"));
            assertThat(status.getListeners().get(0).getAddresses().get(0).getHost(), is("my-service.my-namespace.svc"));
            assertThat(status.getListeners().get(0).getAddresses().get(0).getPort(), is(9092));
            assertThat(status.getListeners().get(0).getBootstrapServers(), is("my-service.my-namespace.svc:9092"));

            assertThat(status.getConditions().size(), is(1));
            assertThat(status.getConditions().get(0).getType(), is("NotReady"));
            assertThat(status.getConditions().get(0).getStatus(), is("True"));
            assertThat(status.getConditions().get(0).getReason(), is(exception.getClass().getSimpleName()));
            assertThat(status.getConditions().get(0).getMessage(), is(exception.getMessage()));

            assertThat(status.getObservedGeneration(), is(2L));

            async.flag();
        });
    }

    @Test
    public void testStatusAfterFailedReconciliationWithPreviousSuccess(VertxTestContext context) throws ParseException {
        Kafka kafka = getKafkaCrd();
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the Kafka Operator
        CrdOperator<KubernetesClient, Kafka, KafkaList> mockKafkaOps = supplier.kafkaOperator;

        Kafka readyKafka = new KafkaBuilder(kafka)
                .editStatus()
                    .withObservedGeneration(1L)
                    .editCondition(0)
                        .withType("Ready")
                    .endCondition()
                    .withListeners(new ListenerStatusBuilder()
                                    .withName("plain")
                                    .withAddresses(new ListenerAddressBuilder()
                                            .withHost("my-service.my-namespace.svc")
                                            .withPort(9092)
                                            .build())
                                    .build(),
                            new ListenerStatusBuilder()
                                    .withName("external")
                                    .withAddresses(new ListenerAddressBuilder()
                                            .withHost("my-route-address.domain.tld")
                                            .withPort(443)
                                            .build())
                                    .build())
                .endStatus()
                .build();

        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(readyKafka));
        when(mockKafkaOps.get(eq(namespace), eq(clusterName))).thenReturn(readyKafka);

        ArgumentCaptor<Kafka> kafkaCaptor = ArgumentCaptor.forClass(Kafka.class);
        when(mockKafkaOps.updateStatusAsync(any(), kafkaCaptor.capture())).thenReturn(Future.succeededFuture());

        MockFailingKafkaAssemblyOperator kao = new MockFailingKafkaAssemblyOperator(
                new RuntimeException("Something went wrong"),
                vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config);

        Checkpoint async = context.checkpoint();
        kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName)).onComplete(res -> {
            assertThat(res.succeeded(), is(false));

            assertThat(kafkaCaptor.getValue(), is(notNullValue()));
            assertThat(kafkaCaptor.getValue().getStatus(), is(notNullValue()));
            KafkaStatus status = kafkaCaptor.getValue().getStatus();

            assertThat(status.getListeners().size(), is(1));
            assertThat(status.getListeners().get(0).getType(), is("plain"));
            assertThat(status.getListeners().get(0).getName(), is("plain"));
            assertThat(status.getListeners().get(0).getAddresses().get(0).getHost(), is("my-service.my-namespace.svc"));
            assertThat(status.getListeners().get(0).getAddresses().get(0).getPort(), is(9092));
            assertThat(status.getListeners().get(0).getBootstrapServers(), is("my-service.my-namespace.svc:9092"));

            assertThat(status.getConditions().size(), is(1));
            assertThat(status.getConditions().get(0).getType(), is("NotReady"));
            assertThat(status.getConditions().get(0).getStatus(), is("True"));
            assertThat(status.getConditions().get(0).getReason(), is("RuntimeException"));
            assertThat(status.getConditions().get(0).getMessage(), is("Something went wrong"));

            assertThat(status.getObservedGeneration(), is(2L));

            async.flag();
        });
    }

    @Test
    public void testInitialStatusOnNewResource() throws ParseException {
        Kafka kafka = getKafkaCrd();
        kafka.setStatus(null);

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the Kafka Operator
        CrdOperator<KubernetesClient, Kafka, KafkaList> mockKafkaOps = supplier.kafkaOperator;

        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(kafka));
        when(mockKafkaOps.get(eq(namespace), eq(clusterName))).thenReturn(kafka);

        ArgumentCaptor<Kafka> kafkaCaptor = ArgumentCaptor.forClass(Kafka.class);
        when(mockKafkaOps.updateStatusAsync(any(), kafkaCaptor.capture())).thenReturn(Future.succeededFuture());

        MockInitialStatusKafkaAssemblyOperator kao = new MockInitialStatusKafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config);

        kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName)).onComplete(res -> {
            assertThat(res.succeeded(), is(true));

            assertThat(kafkaCaptor.getAllValues().size(), is(2));
            assertThat(kafkaCaptor.getAllValues().get(0).getStatus(), is(notNullValue()));
            KafkaStatus status = kafkaCaptor.getAllValues().get(0).getStatus();

            assertThat(status.getListeners(), is(nullValue()));

            assertThat(status.getConditions().size(), is(1));
            assertThat(status.getConditions().get(0).getType(), is("NotReady"));
            assertThat(status.getConditions().get(0).getStatus(), is("True"));
            assertThat(status.getConditions().get(0).getReason(), is("Creating"));
            assertThat(status.getConditions().get(0).getMessage(), is("Kafka cluster is being deployed"));
        });
    }

    @Test
    public void testInitialStatusOnOldResource() throws ParseException {
        Kafka kafka = getKafkaCrd();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the Kafka Operator
        CrdOperator<KubernetesClient, Kafka, KafkaList> mockKafkaOps = supplier.kafkaOperator;

        when(mockKafkaOps.getAsync(eq(namespace), eq(clusterName))).thenReturn(Future.succeededFuture(kafka));
        when(mockKafkaOps.get(eq(namespace), eq(clusterName))).thenReturn(kafka);

        ArgumentCaptor<Kafka> kafkaCaptor = ArgumentCaptor.forClass(Kafka.class);
        when(mockKafkaOps.updateStatusAsync(any(), kafkaCaptor.capture())).thenReturn(Future.succeededFuture());

        MockInitialStatusKafkaAssemblyOperator kao = new MockInitialStatusKafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                passwordGenerator,
                supplier,
                config);

        kao.reconcile(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName)).onComplete(res -> {
            assertThat(res.succeeded(), is(true));

            assertThat(kafkaCaptor.getAllValues().size(), is(1));
        });
    }

    // This allows to test the status handling when reconciliation succeeds
    static class MockWorkingKafkaAssemblyOperator extends KafkaAssemblyOperator  {
        public MockWorkingKafkaAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa, CertManager certManager, PasswordGenerator passwordGenerator, ResourceOperatorSupplier supplier, ClusterOperatorConfig config) {
            super(vertx, pfa, certManager, passwordGenerator, supplier, config);
        }

        @Override
        Future<Void> reconcile(ReconciliationState reconcileState)  {
            ListenerStatus ls = new ListenerStatusBuilder()
                    .withName("plain")
                    .withAddresses(new ListenerAddressBuilder()
                            .withHost("my-service.my-namespace.svc")
                            .withPort(9092)
                            .build())
                    .build();

            ListenerStatus ls2 = new ListenerStatusBuilder()
                    .withName("external")
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
    static class MockFailingKafkaAssemblyOperator extends KafkaAssemblyOperator  {
        private final Throwable exception;

        public MockFailingKafkaAssemblyOperator(Throwable exception, Vertx vertx, PlatformFeaturesAvailability pfa, CertManager certManager, PasswordGenerator passwordGenerator, ResourceOperatorSupplier supplier, ClusterOperatorConfig config) {
            super(vertx, pfa, certManager, passwordGenerator, supplier, config);
            this.exception = exception;
        }

        @Override
        Future<Void> reconcile(ReconciliationState reconcileState)  {
            ListenerStatus ls = new ListenerStatusBuilder()
                    .withName("plain")
                    .withAddresses(new ListenerAddressBuilder()
                            .withHost("my-service.my-namespace.svc")
                            .withPort(9092)
                            .build())
                    .build();

            reconcileState.kafkaStatus.setListeners(singletonList(ls));

            return Future.failedFuture(exception);
        }
    }

    // This allows to test the initial status handling when new resource is created
    static class MockInitialStatusKafkaAssemblyOperator extends KafkaAssemblyOperator  {
        public MockInitialStatusKafkaAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa, CertManager certManager, PasswordGenerator passwordGenerator, ResourceOperatorSupplier supplier, ClusterOperatorConfig config) {
            super(vertx, pfa, certManager, passwordGenerator, supplier, config);
        }

        @Override
        Future<Void> reconcile(ReconciliationState reconcileState)  {
            return reconcileState.initialStatus()
                    .map((Void) null);
        }
    }
}
