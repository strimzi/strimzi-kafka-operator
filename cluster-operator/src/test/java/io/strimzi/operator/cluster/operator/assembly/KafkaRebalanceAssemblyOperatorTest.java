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
import io.strimzi.api.kafka.model.status.KafkaRebalanceStatus;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.CruiseControl;
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

        // Setup the rebalance user tasks endpoints with the number of pending calls before a response is received.
        MockCruiseControl.setupCCRebalanceResponse(ccServer);
        MockCruiseControl.setupCCUserTasksResponse(ccServer, 0);

        KubernetesClient client = new MockKube()
                .withCustomResourceDefinition(Crds.kafkaRebalance(), KafkaRebalance.class, KafkaRebalanceList.class, DoneableKafkaRebalance.class)
                .end()
                .build();

        KafkaRebalanceSpec rebalanceSpec = new KafkaRebalanceSpecBuilder().build();

        KafkaRebalance kcRebalance = new KafkaRebalanceBuilder()
                .withNewMetadata()
                    .withNamespace(CLUSTER_NAMESPACE)
                    .withName(RESOURCE_NAME)
                    .withLabels(Collections.singletonMap(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME))
                .endMetadata()
                .withSpec(rebalanceSpec)
                .build();

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

        Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).create(kcRebalance);

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
                kcRebalance).setHandler(context.succeeding(v -> context.verify(() -> {
                    KafkaRebalance kr = Crds.kafkaRebalanceOperation(client).inNamespace(CLUSTER_NAMESPACE).withName(RESOURCE_NAME).get();
                    assertThat(kr, notNullValue());
                    assertThat(kr.getStatus(), notNullValue());
                    assertThat(kr.getStatus().getConditions(), notNullValue());
                    assertThat(kr.getStatus().getConditions().get(0).getType(), is(KafkaRebalanceStatus.REBALANCE_STATUS_CONDITION_TYPE));
                    assertThat(kr.getStatus().getConditions().get(0).getStatus(), is(KafkaRebalanceAssemblyOperator.State.ProposalReady.toString()));
                    context.completeNow();
                })));
    }

}
