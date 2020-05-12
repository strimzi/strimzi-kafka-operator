/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.KafkaRebalanceList;
import io.strimzi.api.kafka.model.CruiseControlSpec;
import io.strimzi.api.kafka.model.CruiseControlSpecBuilder;
import io.strimzi.api.kafka.model.DoneableKafka;
import io.strimzi.api.kafka.model.DoneableKafkaRebalance;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaRebalance;
import io.strimzi.api.kafka.model.KafkaRebalanceBuilder;
import io.strimzi.api.kafka.model.KafkaRebalanceSpec;
import io.strimzi.api.kafka.model.KafkaRebalanceSpecBuilder;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.CruiseControl;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.cluster.model.NoSuchResourceException;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.MockCruiseControl;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.test.mockkube.MockKube;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockserver.integration.ClientAndServer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaRebalanceAssemblyOperatorTest {

    private static final String HOST = "localhost";
    private static final String RESOURCE_NAME = "my-rebalance";
    private static final String CLUSTER_NAMESPACE = "cruise-control-namespace";
    private static final String CLUSTER_NAME = "kafka-cruise-control-test-cluster";

    private final KubernetesVersion kubernetesVersion = KubernetesVersion.V1_11;

    private static final Logger log = LogManager.getLogger(KafkaRebalanceAssemblyOperatorTest.class.getName());

    private static ClientAndServer ccServer;

    private final int replicas = 1;
    private final String image = "my-kafka-image";
    private final int healthDelay = 120;
    private final int healthTimeout = 30;

    private final String version = KafkaVersionTestUtils.DEFAULT_KAFKA_VERSION;
    private final String ccImage = "my-cruise-control-image";

    private final CruiseControlSpec cruiseControlSpec = new CruiseControlSpecBuilder()
            .withImage(ccImage)
            .build();

    private final Kafka kafka =
            new KafkaBuilder(ResourceUtils.createKafkaCluster(CLUSTER_NAMESPACE, CLUSTER_NAME, replicas, image, healthDelay, healthTimeout))
                    .editSpec()
                        .editKafka()
                            .withVersion(version)
                        .endKafka()
                        .withCruiseControl(cruiseControlSpec)
                    .endSpec()
                    .build();

    @BeforeAll
    public static void before() throws IOException, URISyntaxException {
        ccServer = MockCruiseControl.getCCServer(CruiseControl.REST_API_PORT);
    }

    @AfterAll
    public static void after() {
        ccServer.stop();
    }

    @BeforeEach
    public void resetServer() {
        ccServer.reset();
    }

    @Test
    public void testNewRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        // Setup the rebalance endpoint with the number of pending calls before a response is received.
        MockCruiseControl.setupCCRebalanceResponse(ccServer, 0);

        KubernetesClient client = new MockKube()
                .withCustomResourceDefinition(Crds.kafkaRebalance(), KafkaRebalance.class, KafkaRebalanceList.class, DoneableKafkaRebalance.class)
                .end()
                .build();

        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, new KafkaRebalanceSpecBuilder().build());

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, kubernetesVersion);
        KafkaRebalanceAssemblyOperator kcrao = new KafkaRebalanceAssemblyOperator(vertx, pfa, supplier, HOST);

        CrdOperator<KubernetesClient,
                KafkaRebalance,
                KafkaRebalanceList,
                DoneableKafkaRebalance> mockRebalanceOps = supplier.kafkaRebalanceOperator;

        CrdOperator<KubernetesClient,
                Kafka,
                KafkaList,
                DoneableKafka> mockKafkaOps = supplier.kafkaOperator;

        Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).create(kr);

        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        when(mockRebalanceOps.getAsync(CLUSTER_NAMESPACE, RESOURCE_NAME)).thenAnswer(invocation -> {
            try {
                return Future.succeededFuture(Crds.kafkaRebalanceOperation(client)
                        .inNamespace(CLUSTER_NAMESPACE)
                        .withName(RESOURCE_NAME)
                        .get());
            } catch (Exception e) {
                return Future.failedFuture(e);
            }
        });
        when(mockRebalanceOps.updateStatusAsync(any(KafkaRebalance.class))).thenAnswer(invocation -> {
            try {
                return Future.succeededFuture(Crds.kafkaRebalanceOperation(client)
                        .inNamespace(CLUSTER_NAMESPACE)
                        .withName(RESOURCE_NAME)
                        .patch(invocation.getArgument(0)));
            } catch (Exception e) {
                return Future.failedFuture(e);
            }
        });

        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                kr).setHandler(context.succeeding(v -> context.verify(() -> {
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertState(kr1, KafkaRebalanceAssemblyOperator.State.ProposalReady);
                    context.completeNow();
                })));
    }

    @Test
    public void testApproveRebalance(Vertx vertx, VertxTestContext context) throws IOException, URISyntaxException {

        // Setup the rebalance user tasks endpoints with the number of pending calls before a response is received.
        MockCruiseControl.setupCCRebalanceResponse(ccServer, 0);
        MockCruiseControl.setupCCUserTasksResponseNoGoals(ccServer, 0, 0);

        KubernetesClient client = new MockKube()
                .withCustomResourceDefinition(Crds.kafkaRebalance(), KafkaRebalance.class, KafkaRebalanceList.class, DoneableKafkaRebalance.class)
                .end()
                .build();

        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, new KafkaRebalanceSpecBuilder().build());

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, kubernetesVersion);
        KafkaRebalanceAssemblyOperator kcrao = new KafkaRebalanceAssemblyOperator(vertx, pfa, supplier, HOST);

        CrdOperator<KubernetesClient,
                KafkaRebalance,
                KafkaRebalanceList,
                DoneableKafkaRebalance> mockRebalanceOps = supplier.kafkaRebalanceOperator;

        CrdOperator<KubernetesClient,
                Kafka,
                KafkaList,
                DoneableKafka> mockKafkaOps = supplier.kafkaOperator;

        Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).create(kr);

        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        when(mockRebalanceOps.getAsync(CLUSTER_NAMESPACE, RESOURCE_NAME)).thenAnswer(invocation -> {
            try {
                return Future.succeededFuture(Crds.kafkaRebalanceOperation(client)
                        .inNamespace(CLUSTER_NAMESPACE)
                        .withName(RESOURCE_NAME)
                        .get());
            } catch (Exception e) {
                return Future.failedFuture(e);
            }
        });
        when(mockRebalanceOps.updateStatusAsync(any(KafkaRebalance.class))).thenAnswer(invocation -> {
            try {
                return Future.succeededFuture(Crds.kafkaRebalanceOperation(client)
                        .inNamespace(CLUSTER_NAMESPACE)
                        .withName(RESOURCE_NAME)
                        .patch(invocation.getArgument(0)));
            } catch (Exception e) {
                return Future.failedFuture(e);
            }
        });
        when(mockRebalanceOps.patchAsync(any(KafkaRebalance.class))).thenAnswer(invocation -> {
            try {
                return Future.succeededFuture(Crds.kafkaRebalanceOperation(client)
                        .inNamespace(CLUSTER_NAMESPACE)
                        .withName(RESOURCE_NAME)
                        .patch(invocation.getArgument(0)));
            } catch (Exception e) {
                return Future.failedFuture(e);
            }
        });

        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                kr).setHandler(context.succeeding(v -> context.verify(() -> {
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertState(kr1, KafkaRebalanceAssemblyOperator.State.ProposalReady);
                }))).compose(v -> {

                    KafkaRebalance kr2 = Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();

                    KafkaRebalance approvedKr = new KafkaRebalanceBuilder(kr2)
                            .editMetadata()
                                .addToAnnotations(KafkaRebalanceAssemblyOperator.ANNO_STRIMZI_IO_REBALANCE, "approve")
                            .endMetadata()
                            .build();

                    Crds.kafkaRebalanceOperation(client)
                            .inNamespace(CLUSTER_NAMESPACE)
                            .withName(RESOURCE_NAME)
                            .patch(approvedKr);

                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                            approvedKr);

                }).compose(v -> {
                    KafkaRebalance kr3 = Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertState(kr3, KafkaRebalanceAssemblyOperator.State.Rebalancing);

                    return kcrao.reconcileRebalance(
                            new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                            kr3);

                }).setHandler(ar -> {
                    KafkaRebalance kr4 = Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertState(kr4, KafkaRebalanceAssemblyOperator.State.Ready);
                    context.completeNow();
                });
    }

    @Test
    public void testNoKafkaCluster(Vertx vertx, VertxTestContext context) {

        KubernetesClient client = new MockKube()
                .withCustomResourceDefinition(Crds.kafkaRebalance(), KafkaRebalance.class, KafkaRebalanceList.class, DoneableKafkaRebalance.class)
                .end()
                .build();

        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, new KafkaRebalanceSpecBuilder().build());

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, kubernetesVersion);
        KafkaRebalanceAssemblyOperator kcrao = new KafkaRebalanceAssemblyOperator(vertx, pfa, supplier, HOST);

        CrdOperator<KubernetesClient,
                KafkaRebalance,
                KafkaRebalanceList,
                DoneableKafkaRebalance> mockRebalanceOps = supplier.kafkaRebalanceOperator;

        CrdOperator<KubernetesClient,
                Kafka,
                KafkaList,
                DoneableKafka> mockKafkaOps = supplier.kafkaOperator;

        Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).create(kr);

        // the Kafka cluster isn't deployed in the namespace
        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(null));
        when(mockRebalanceOps.updateStatusAsync(any(KafkaRebalance.class))).thenAnswer(invocation -> {
            try {
                return Future.succeededFuture(Crds.kafkaRebalanceOperation(client)
                        .inNamespace(CLUSTER_NAMESPACE)
                        .withName(RESOURCE_NAME)
                        .patch(invocation.getArgument(0)));
            } catch (Exception e) {
                return Future.failedFuture(e);
            }
        });

        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                kr).setHandler(context.succeeding(v -> context.verify(() -> {
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertState(kr1, KafkaRebalanceAssemblyOperator.State.NotReady, NoSuchResourceException.class,
                            "Kafka resource '" + CLUSTER_NAME + "' identified by label '" + Labels.STRIMZI_CLUSTER_LABEL + "' does not exist in namespace " + CLUSTER_NAMESPACE  + ".");
                    context.completeNow();
                })));
    }

    @Test
    public void testNoCruiseControl(Vertx vertx, VertxTestContext context) {

        // build a Kafka cluster without the cruiseControl definition
        Kafka kafka =
                new KafkaBuilder(ResourceUtils.createKafkaCluster(CLUSTER_NAMESPACE, CLUSTER_NAME, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                            .editKafka()
                                .withVersion(version)
                            .endKafka()
                        .endSpec()
                        .build();

        KubernetesClient client = new MockKube()
                .withCustomResourceDefinition(Crds.kafkaRebalance(), KafkaRebalance.class, KafkaRebalanceList.class, DoneableKafkaRebalance.class)
                .end()
                .build();

        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, new KafkaRebalanceSpecBuilder().build());

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, kubernetesVersion);
        KafkaRebalanceAssemblyOperator kcrao = new KafkaRebalanceAssemblyOperator(vertx, pfa, supplier, HOST);

        CrdOperator<KubernetesClient,
                KafkaRebalance,
                KafkaRebalanceList,
                DoneableKafkaRebalance> mockRebalanceOps = supplier.kafkaRebalanceOperator;

        CrdOperator<KubernetesClient,
                Kafka,
                KafkaList,
                DoneableKafka> mockKafkaOps = supplier.kafkaOperator;

        Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).create(kr);

        // the Kafka cluster doesn't have the Cruise Control deployment
        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        when(mockRebalanceOps.updateStatusAsync(any(KafkaRebalance.class))).thenAnswer(invocation -> {
            try {
                return Future.succeededFuture(Crds.kafkaRebalanceOperation(client)
                        .inNamespace(CLUSTER_NAMESPACE)
                        .withName(RESOURCE_NAME)
                        .patch(invocation.getArgument(0)));
            } catch (Exception e) {
                return Future.failedFuture(e);
            }
        });

        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                kr).setHandler(context.succeeding(v -> context.verify(() -> {
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertState(kr1, KafkaRebalanceAssemblyOperator.State.NotReady, InvalidResourceException.class,
                            "Kafka resouce lacks 'cruiseControl' declaration : No deployed Cruise Control for doing a rebalance.");
                    context.completeNow();
                })));
    }

    @Test
    public void testNoKafkaClusterInKafkaRebalanceLabel(Vertx vertx, VertxTestContext context) {

        KubernetesClient client = new MockKube()
                .withCustomResourceDefinition(Crds.kafkaRebalance(), KafkaRebalance.class, KafkaRebalanceList.class, DoneableKafkaRebalance.class)
                .end()
                .build();

        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, null, RESOURCE_NAME, new KafkaRebalanceSpecBuilder().build());

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, kubernetesVersion);
        KafkaRebalanceAssemblyOperator kcrao = new KafkaRebalanceAssemblyOperator(vertx, pfa, supplier, HOST);

        CrdOperator<KubernetesClient,
                KafkaRebalance,
                KafkaRebalanceList,
                DoneableKafkaRebalance> mockRebalanceOps = supplier.kafkaRebalanceOperator;

        CrdOperator<KubernetesClient,
                Kafka,
                KafkaList,
                DoneableKafka> mockKafkaOps = supplier.kafkaOperator;

        Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).create(kr);

        when(mockKafkaOps.getAsync(CLUSTER_NAMESPACE, CLUSTER_NAME)).thenReturn(Future.succeededFuture(kafka));
        when(mockRebalanceOps.updateStatusAsync(any(KafkaRebalance.class))).thenAnswer(invocation -> {
            try {
                return Future.succeededFuture(Crds.kafkaRebalanceOperation(client)
                        .inNamespace(CLUSTER_NAMESPACE)
                        .withName(RESOURCE_NAME)
                        .patch(invocation.getArgument(0)));
            } catch (Exception e) {
                return Future.failedFuture(e);
            }
        });

        kcrao.reconcileRebalance(
                new Reconciliation("test-trigger", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME),
                kr).setHandler(context.succeeding(v -> context.verify(() -> {
                    KafkaRebalance kr1 = Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertState(kr1, KafkaRebalanceAssemblyOperator.State.NotReady, InvalidResourceException.class,
                            "Resource lacks label '" + Labels.STRIMZI_CLUSTER_LABEL + "': No cluster related to a possible rebalance.");
                    context.completeNow();
                })));
    }

    private void assertState(KafkaRebalance kafkaRebalance, KafkaRebalanceAssemblyOperator.State state) {
        assertThat(kafkaRebalance, notNullValue());
        assertThat(kafkaRebalance.getStatus(), notNullValue());
        assertThat(kafkaRebalance.getStatus().getConditions(), notNullValue());
        assertThat(kafkaRebalance.getStatus().getConditions().get(0).getStatus(), is(state.toString()));
    }

    private void assertState(KafkaRebalance kafkaRebalance, KafkaRebalanceAssemblyOperator.State state,
                             Class reason, String message) {
        assertState(kafkaRebalance, state);
        assertThat(kafkaRebalance.getStatus().getConditions().get(0).getReason(), is(reason.getSimpleName()));
        assertThat(kafkaRebalance.getStatus().getConditions().get(0).getMessage(), is(message));
    }

    private KafkaRebalance createKafkaRebalance(String namespace, String clusterName, String resourceName,
                                                     KafkaRebalanceSpec kafkaRebalanceSpec) {
        KafkaRebalance kcRebalance = new KafkaRebalanceBuilder()
                .withNewMetadata()
                    .withNamespace(namespace)
                    .withName(resourceName)
                    .withLabels(clusterName != null ? Collections.singletonMap(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME) : null)
                .endMetadata()
                .withSpec(kafkaRebalanceSpec)
                .build();

        return kcRebalance;
    }

}
