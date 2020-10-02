/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinitionList;
import io.fabric8.kubernetes.api.model.apiextensions.DoneableCustomResourceDefinition;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.FilterWatchListMultiDeletable;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import okhttp3.OkHttpClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class ClusterOperatorTest {
    private static Vertx vertx;
    private static final Logger log = LogManager.getLogger(ClusterOperatorTest.class);

    private static Map<String, String> buildEnv(String namespaces) {
        Map<String, String> env = new HashMap<>();
        env.put(ClusterOperatorConfig.STRIMZI_NAMESPACE, namespaces);
        env.put(ClusterOperatorConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL_MS, "120000");
        env.put(ClusterOperatorConfig.STRIMZI_KAFKA_IMAGES, KafkaVersionTestUtils.getKafkaImagesEnvVarString());
        env.put(ClusterOperatorConfig.STRIMZI_KAFKA_CONNECT_IMAGES, KafkaVersionTestUtils.getKafkaConnectImagesEnvVarString());
        env.put(ClusterOperatorConfig.STRIMZI_KAFKA_CONNECT_S2I_IMAGES, KafkaVersionTestUtils.getKafkaConnectS2iImagesEnvVarString());
        env.put(ClusterOperatorConfig.STRIMZI_KAFKA_MIRROR_MAKER_IMAGES, KafkaVersionTestUtils.getKafkaMirrorMakerImagesEnvVarString());
        env.put(ClusterOperatorConfig.STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES, KafkaVersionTestUtils.getKafkaMirrorMaker2ImagesEnvVarString());
        return env;
    }

    @BeforeAll
    public static void before() {
        VertxOptions options = new VertxOptions().setMetricsOptions(
                new MicrometerMetricsOptions()
                        .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true))
                        .setEnabled(true));
        vertx = Vertx.vertx(options);
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    @Test
    public void testStartStopSingleNamespaceOnOpenShift(VertxTestContext context) throws InterruptedException {
        startStop(context, "namespace", true);
    }

    @Test
    public void testStartStopMultiNamespaceOnOpenShift(VertxTestContext context) throws InterruptedException {
        startStop(context, "namespace1,namespace2", true);
    }

    @Test
    public void testStartStopSingleNamespaceOnK8s(VertxTestContext context) throws InterruptedException {
        startStop(context, "namespace", false);
    }

    @Test
    public void testStartStopMultiNamespaceOnK8s(VertxTestContext context) throws InterruptedException {
        startStop(context, "namespace1,namespace2", false);
    }

    @Test
    public void testStartStopAllNamespacesOnOpenShift(VertxTestContext context) throws InterruptedException {
        startStopAllNamespaces(context, "*", true);
    }

    @Test
    public void testStartStopAllNamespacesOnK8s(VertxTestContext context) throws InterruptedException {
        startStopAllNamespaces(context, "*", false);
    }

    /**
     * Asserts that Cluster Operator starts and then stops a verticle in each namespace
     * @param context test context passed in for assertions
     * @param namespaces namespaces the operator should be watching and operating on
     */
    private void startStop(VertxTestContext context, String namespaces, boolean openShift) throws InterruptedException {
        AtomicInteger numWatchers = new AtomicInteger(0);

        KubernetesClient client;
        if (openShift) {
            client = mock(OpenShiftClient.class);
            when(client.isAdaptable(eq(OpenShiftClient.class))).thenReturn(true);
            when(client.adapt(eq(OpenShiftClient.class))).thenReturn((OpenShiftClient) client);
        } else {
            client = mock(KubernetesClient.class);
            when(client.isAdaptable(eq(OpenShiftClient.class))).thenReturn(false);
        }
        when(client.isAdaptable(eq(OkHttpClient.class))).thenReturn(true);

        try {
            when(client.getMasterUrl()).thenReturn(new URL("http://localhost"));
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
        MixedOperation mockCms = mock(MixedOperation.class);
        NonNamespaceOperation<CustomResourceDefinition, CustomResourceDefinitionList, DoneableCustomResourceDefinition, Resource<CustomResourceDefinition, DoneableCustomResourceDefinition>> mockCrds = mock(NonNamespaceOperation.class);
        Resource<CustomResourceDefinition, DoneableCustomResourceDefinition> mockResource = mock(Resource.class);
        if (openShift) {
            when(mockResource.get()).thenReturn(Crds.kafkaConnectS2I());
        } else {
            when(mockResource.get()).thenReturn(null);
        }
        when(mockCrds.withName(KafkaConnectS2I.CRD_NAME)).thenReturn(mockResource);
        when(client.customResourceDefinitions()).thenReturn(mockCrds);
        when(client.customResources(any(CustomResourceDefinitionContext.class), any(), any(), any())).thenReturn(mockCms);

        List<String> namespaceList = asList(namespaces.split(" *,+ *"));
        for (String namespace: namespaceList) {

            MixedOperation mockNamespacedCms = mock(MixedOperation.class);
            when(mockNamespacedCms.watch(any())).thenAnswer(invo -> {
                numWatchers.incrementAndGet();
                Watch mockWatch = mock(Watch.class);
                doAnswer(invo2 -> {
                    ((Watcher) invo.getArgument(0)).onClose(null);
                    return null;
                }).when(mockWatch).close();
                return mockWatch;
            });

            when(mockNamespacedCms.withLabels(any())).thenReturn(mockNamespacedCms);
            when(mockCms.inNamespace(namespace)).thenReturn(mockNamespacedCms);
        }

        Map<String, String> env = buildEnv(namespaces);

        CountDownLatch latch = new CountDownLatch(namespaceList.size() + 1);

        Main.run(vertx, client, new PlatformFeaturesAvailability(openShift, KubernetesVersion.V1_9),
                    ClusterOperatorConfig.fromMap(env, KafkaVersionTestUtils.getKafkaVersionLookup()))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertThat("A verticle per namespace", vertx.deploymentIDs(), hasSize(namespaceList.size()));
                for (String deploymentId: vertx.deploymentIDs()) {
                    vertx.undeploy(deploymentId, asyncResult -> {
                        if (asyncResult.failed()) {
                            log.error("Failed to undeploy {}", deploymentId);
                            context.failNow(asyncResult.cause());
                        }
                        latch.countDown();
                    });
                }

                int maximumExpectedNumberOfWatchers = (openShift ? 9 : 7) * namespaceList.size(); // we do not have connectS2I on k8s
                assertThat("Looks like there were more watchers than namespaces",
                        numWatchers.get(), lessThanOrEqualTo(maximumExpectedNumberOfWatchers));
                latch.countDown();
            })));
        latch.await(10, TimeUnit.SECONDS);
        context.completeNow();
    }

    /**
     * Asserts that Cluster Operator starts and then stops a verticle in every namespace using the namespace wildcard (*)
     * @param context test context passed in for assertions
     * @param namespaces namespaces the operator should be watching and operating on
     */
    private void startStopAllNamespaces(VertxTestContext context, String namespaces, boolean openShift) throws InterruptedException {
        AtomicInteger numWatchers = new AtomicInteger(0);
        KubernetesClient client;
        if (openShift) {
            client = mock(OpenShiftClient.class);
            when(client.isAdaptable(eq(OpenShiftClient.class))).thenReturn(true);
            when(client.adapt(eq(OpenShiftClient.class))).thenReturn((OpenShiftClient) client);
        } else {
            client = mock(KubernetesClient.class);
            when(client.isAdaptable(eq(OpenShiftClient.class))).thenReturn(false);
        }
        when(client.isAdaptable(eq(OkHttpClient.class))).thenReturn(true);

        try {
            when(client.getMasterUrl()).thenReturn(new URL("http://localhost"));
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }

        MixedOperation mockCms = mock(MixedOperation.class);
        NonNamespaceOperation<CustomResourceDefinition, CustomResourceDefinitionList, DoneableCustomResourceDefinition,
                Resource<CustomResourceDefinition, DoneableCustomResourceDefinition>> mockCrds = mock(NonNamespaceOperation.class);
        Resource<CustomResourceDefinition, DoneableCustomResourceDefinition> mockResource = mock(Resource.class);
        if (openShift) {
            when(mockResource.get()).thenReturn(Crds.kafkaConnectS2I());
        } else {
            when(mockResource.get()).thenReturn(null);
        }
        when(mockCrds.withName(KafkaConnectS2I.CRD_NAME)).thenReturn(mockResource);
        when(client.customResourceDefinitions()).thenReturn(mockCrds);
        when(client.customResources(any(CustomResourceDefinitionContext.class), any(), any(), any())).thenReturn(mockCms);

        FilterWatchListMultiDeletable mockFilteredCms = mock(FilterWatchListMultiDeletable.class);
        when(mockFilteredCms.withLabels(any())).thenReturn(mockFilteredCms);
        when(mockFilteredCms.watch(any())).thenAnswer(invo -> {
            numWatchers.incrementAndGet();
            Watch mockWatch = mock(Watch.class);
            doAnswer(invo2 -> {
                ((Watcher) invo.getArgument(0)).onClose(null);
                return null;
            }).when(mockWatch).close();
            return mockWatch;
        });
        when(mockCms.inAnyNamespace()).thenReturn(mockFilteredCms);

        Map<String, String> env = buildEnv(namespaces);

        CountDownLatch latch = new CountDownLatch(2);
        Main.run(vertx, client, new PlatformFeaturesAvailability(openShift, KubernetesVersion.V1_9),
                ClusterOperatorConfig.fromMap(env, KafkaVersionTestUtils.getKafkaVersionLookup()))
            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertThat("A verticle per namespace", vertx.deploymentIDs(), hasSize(1));
                for (String deploymentId: vertx.deploymentIDs()) {
                    vertx.undeploy(deploymentId, asyncResult -> {
                        if (asyncResult.failed()) {
                            log.error("Failed to undeploy {}", deploymentId);
                            context.failNow(asyncResult.cause());
                        }
                        latch.countDown();
                    });
                }

                int maximumExpectedNumberOfWatchers = openShift ? 9 : 7; // we do not have connectS2I on k8s
                assertThat("Looks like there were more watchers than namespaces", numWatchers.get(), lessThanOrEqualTo(maximumExpectedNumberOfWatchers));
                latch.countDown();
            })));
        latch.await(10, TimeUnit.SECONDS);
        context.completeNow();
    }
}
