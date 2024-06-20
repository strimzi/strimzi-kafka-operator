/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaMirrorMaker2List;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Builder;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.FeatureGates;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.DefaultZookeeperScalerProvider;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.ZookeeperLeaderFinder;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.DefaultAdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.OrderedProperties;
import io.strimzi.test.TestUtils;
import io.strimzi.test.mockkube.MockKube;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaMirrorMaker2AssemblyOperatorMockTest {

    private static final Logger LOGGER = LogManager.getLogger(KafkaMirrorMaker2AssemblyOperatorMockTest.class);

    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();

    private static final String NAMESPACE = "my-namespace";
    private static final String CLUSTER_NAME = "my-mm2-cluster";

    private final int replicas = 3;

    private KubernetesClient mockClient;

    private static Vertx vertx;
    private KafkaMirrorMaker2AssemblyOperator kco;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    private void setMirrorMaker2Resource(KafkaMirrorMaker2 mirrorMaker2Resource) {
        if (mockClient != null) {
            mockClient.close();
        }
        MockKube mockKube = new MockKube();
        mockClient = mockKube
                .withCustomResourceDefinition(Crds.kafkaMirrorMaker2(), KafkaMirrorMaker2.class, KafkaMirrorMaker2List.class, KafkaMirrorMaker2::getStatus, KafkaMirrorMaker2::setStatus)
                    .withInitialInstances(Collections.singleton(mirrorMaker2Resource))
                .end()
                .build();
    }

    @AfterEach
    public void afterEach() {
        if (mockClient != null) {
            mockClient.close();
        }
    }


    private Future<Void> createMirrorMaker2Cluster(VertxTestContext context, KafkaConnectApi kafkaConnectApi, boolean reconciliationPaused) {
        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, KubernetesVersion.V1_16);
        ResourceOperatorSupplier supplier = new ResourceOperatorSupplier(vertx, this.mockClient,
                new ZookeeperLeaderFinder(vertx,
                    // Retry up to 3 times (4 attempts), with overall max delay of 35000ms
                    () -> new BackOff(5_000, 2, 4)),
                new DefaultAdminClientProvider(),
                new DefaultZookeeperScalerProvider(),
                ResourceUtils.metricsProvider(),
                pfa, FeatureGates.NONE, 60_000L);

        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);
        kco = new KafkaMirrorMaker2AssemblyOperator(vertx, pfa,
            supplier,
            config,
            foo -> kafkaConnectApi);

        LOGGER.info("Reconciling initially -> create");
        Promise<Void> created = Promise.promise();

        kco.reconcile(new Reconciliation("test-trigger", KafkaMirrorMaker2.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME))
            .onComplete(context.succeeding(ar -> context.verify(() -> {
                if (!reconciliationPaused) {
                    assertThat(mockClient.apps().deployments().inNamespace(NAMESPACE).withName(KafkaMirrorMaker2Resources.deploymentName(CLUSTER_NAME)).get(), is(notNullValue()));
                    assertThat(mockClient.configMaps().inNamespace(NAMESPACE).withName(KafkaMirrorMaker2Resources.metricsAndLogConfigMapName(CLUSTER_NAME)).get(), is(notNullValue()));
                    assertThat(mockClient.services().inNamespace(NAMESPACE).withName(KafkaMirrorMaker2Resources.serviceName(CLUSTER_NAME)).get(), is(notNullValue()));
                    assertThat(mockClient.policy().v1().podDisruptionBudget().inNamespace(NAMESPACE).withName(KafkaMirrorMaker2Resources.deploymentName(CLUSTER_NAME)).get(), is(notNullValue()));
                } else {
                    assertThat(mockClient.apps().deployments().inNamespace(NAMESPACE).withName(KafkaMirrorMaker2Resources.deploymentName(CLUSTER_NAME)).get(), is(nullValue()));
                    verify(mockClient, never()).resources(KafkaMirrorMaker2.class);
                }

                created.complete();
            })));
        return created.future();
    }

    @Test
    public void testReconcileUpdate(VertxTestContext context) {
        setMirrorMaker2Resource(new KafkaMirrorMaker2Builder()
                .withMetadata(new ObjectMetaBuilder()
                        .withName(CLUSTER_NAME)
                        .withNamespace(NAMESPACE)
                        .withLabels(TestUtils.map("foo", "bar"))
                        .build())
                .withNewSpec()
                .withReplicas(replicas)
                .endSpec()
            .build());
        KafkaConnectApi mock = mock(KafkaConnectApi.class);
        when(mock.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));
        when(mock.updateConnectLoggers(any(), anyString(), anyInt(), anyString(), any(OrderedProperties.class))).thenReturn(Future.succeededFuture());

        Checkpoint async = context.checkpoint();
        createMirrorMaker2Cluster(context, mock, false)
            .onComplete(context.succeeding(v -> {
                LOGGER.info("Reconciling again -> update");
                kco.reconcile(new Reconciliation("test-trigger", KafkaMirrorMaker2.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME));
            }))
            .onComplete(context.succeeding(v -> async.flag()));
    }

    @Test
    public void testPauseReconcile(VertxTestContext context) {
        setMirrorMaker2Resource(new KafkaMirrorMaker2Builder()
                .withMetadata(new ObjectMetaBuilder()
                        .withName(CLUSTER_NAME)
                        .withNamespace(NAMESPACE)
                        .withLabels(TestUtils.map("foo", "bar"))
                        .withAnnotations(singletonMap("strimzi.io/pause-reconciliation", "true"))
                        .build())
                .withNewSpec()
                .withReplicas(replicas)
                .endSpec()
                .build());
        KafkaConnectApi mock = mock(KafkaConnectApi.class);
        when(mock.list(anyString(), anyInt())).thenReturn(Future.succeededFuture(emptyList()));
        when(mock.updateConnectLoggers(any(), anyString(), anyInt(), anyString(), any(OrderedProperties.class))).thenReturn(Future.succeededFuture());

        Checkpoint async = context.checkpoint();
        createMirrorMaker2Cluster(context, mock, true)
                .onComplete(context.succeeding(v -> {
                    LOGGER.info("Reconciling again -> update");
                    kco.reconcile(new Reconciliation("test-trigger", KafkaMirrorMaker2.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME));
                }))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    Resource<KafkaMirrorMaker2> resource = Crds.kafkaMirrorMaker2Operation(mockClient).inNamespace(NAMESPACE).withName(CLUSTER_NAME);

                    if (resource.get().getStatus() == null) {
                        fail();
                    }
                    List<Condition> conditions = resource.get().getStatus().getConditions();
                    boolean conditionFound = false;
                    if (conditions != null && !conditions.isEmpty()) {
                        for (Condition condition: conditions) {
                            if ("ReconciliationPaused".equals(condition.getType())) {
                                conditionFound = true;
                                break;
                            }
                        }
                    }
                    assertTrue(conditionFound);
                    async.flag();
                })));
    }

}
