/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaMirrorMaker2List;
import io.strimzi.api.kafka.model.DoneableKafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Builder;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Resources;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.resource.DefaultZookeeperScalerProvider;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.ZookeeperLeaderFinder;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.DefaultAdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.test.TestUtils;
import io.strimzi.test.mockkube.MockKube;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Collections.emptyList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
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
    private MockKube mockKube;

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
        mockKube = new MockKube();
        mockClient = mockKube
                .withCustomResourceDefinition(Crds.kafkaMirrorMaker2(), KafkaMirrorMaker2.class, KafkaMirrorMaker2List.class, DoneableKafkaMirrorMaker2.class, KafkaMirrorMaker2::getStatus, KafkaMirrorMaker2::setStatus)
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


    private KafkaMirrorMaker2AssemblyOperator createMirrorMaker2Cluster(VertxTestContext context, KafkaConnectApi kafkaConnectApi)  throws InterruptedException, ExecutionException, TimeoutException {
        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, KubernetesVersion.V1_9);
        ResourceOperatorSupplier supplier = new ResourceOperatorSupplier(vertx, this.mockClient,
                new ZookeeperLeaderFinder(vertx, new SecretOperator(vertx, this.mockClient),
                    // Retry up to 3 times (4 attempts), with overall max delay of 35000ms
                    () -> new BackOff(5_000, 2, 4)),
                new DefaultAdminClientProvider(),
                new DefaultZookeeperScalerProvider(),
                ResourceUtils.metricsProvider(),
                pfa, 60_000L);

        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS);
        KafkaMirrorMaker2AssemblyOperator kco = new KafkaMirrorMaker2AssemblyOperator(vertx, pfa,
            supplier,
            config,
            foo -> {
                return kafkaConnectApi;
            });

        LOGGER.info("Reconciling initially -> create");
        CountDownLatch createAsync = new CountDownLatch(1);
        kco.reconcile(new Reconciliation("test-trigger", KafkaMirrorMaker2.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME))
            .setHandler(context.succeeding(ar -> {
                context.verify(() -> {
                    assertThat(mockClient.apps().deployments().inNamespace(NAMESPACE).withName(KafkaMirrorMaker2Resources.deploymentName(CLUSTER_NAME)).get(), is(notNullValue()));
                    assertThat(mockClient.configMaps().inNamespace(NAMESPACE).withName(KafkaMirrorMaker2Resources.metricsAndLogConfigMapName(CLUSTER_NAME)).get(), is(notNullValue()));
                    assertThat(mockClient.services().inNamespace(NAMESPACE).withName(KafkaMirrorMaker2Resources.serviceName(CLUSTER_NAME)).get(), is(notNullValue()));
                    assertThat(mockClient.policy().podDisruptionBudget().inNamespace(NAMESPACE).withName(KafkaMirrorMaker2Resources.deploymentName(CLUSTER_NAME)).get(), is(notNullValue()));
                });

                createAsync.countDown();
            }));
        if (!createAsync.await(60, TimeUnit.SECONDS)) {
            context.failNow(new Throwable("Test timeout"));
        }
        return kco;
    }

    @Test
    public void testCreateUpdate(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
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
        KafkaMirrorMaker2AssemblyOperator kco = createMirrorMaker2Cluster(context,
                mock);
        LOGGER.info("Reconciling again -> update");
        CountDownLatch updateAsync = new CountDownLatch(1);
        kco.reconcile(new Reconciliation("test-trigger", KafkaMirrorMaker2.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME)).setHandler(ar -> {
            if (ar.failed()) ar.cause().printStackTrace();
            context.verify(() -> assertTrue(ar.succeeded()));
            updateAsync.countDown();
        });
        updateAsync.await(30, TimeUnit.SECONDS);
        context.completeNow();
    }

}
