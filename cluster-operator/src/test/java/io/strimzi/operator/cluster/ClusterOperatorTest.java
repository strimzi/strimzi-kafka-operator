/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.AnyNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.cache.Indexer;
import io.strimzi.operator.cluster.model.securityprofiles.PodSecurityProviderFactory;
import io.strimzi.platform.KubernetesVersion;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class ClusterOperatorTest {
    private static final Logger LOGGER = LogManager.getLogger(ClusterOperatorTest.class);
    private static final Vertx VERTX = Vertx.vertx(
        new VertxOptions().setMetricsOptions(
            new MicrometerMetricsOptions()
                .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true))
                .setEnabled(true)));

    private static Map<String, String> buildEnv(String namespaces, boolean podSetsOnly) {
        Map<String, String> env = new HashMap<>();
        env.put(ClusterOperatorConfig.NAMESPACE.key(), namespaces);
        env.put(ClusterOperatorConfig.FULL_RECONCILIATION_INTERVAL_MS.key(), "120000");
        env.put(ClusterOperatorConfig.STRIMZI_KAFKA_IMAGES, KafkaVersionTestUtils.getKafkaImagesEnvVarString());
        env.put(ClusterOperatorConfig.STRIMZI_KAFKA_CONNECT_IMAGES, KafkaVersionTestUtils.getKafkaConnectImagesEnvVarString());
        env.put(ClusterOperatorConfig.STRIMZI_KAFKA_MIRROR_MAKER_IMAGES, KafkaVersionTestUtils.getKafkaMirrorMakerImagesEnvVarString());
        env.put(ClusterOperatorConfig.STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES, KafkaVersionTestUtils.getKafkaMirrorMaker2ImagesEnvVarString());
        env.put(ClusterOperatorConfig.FEATURE_GATES.key(), "-UseKRaft");

        if (podSetsOnly) {
            env.put(ClusterOperatorConfig.POD_SET_RECONCILIATION_ONLY.key(), "true");
        }

        return env;
    }

    @AfterAll
    public static void afterAll()   {
        PodSecurityProviderFactory.initialize();
    }

    @Test
    public void testStartStopSingleNamespace(VertxTestContext context) throws InterruptedException {
        startStop(context, "namespace", false);
    }

    @Test
    public void testStartStopMultiNamespace(VertxTestContext context) throws InterruptedException {
        startStop(context, "namespace1,namespace2", false);
    }

    @Test
    public void testStartStopMultiNamespaceWithPodSetsOnly(VertxTestContext context) throws InterruptedException {
        startStop(context, "namespace1,namespace2", true);
    }

    @Test
    public void testStartStopAllNamespaces(VertxTestContext context) throws InterruptedException {
        startStopAllNamespaces(context, "*", false);
    }

    @Test
    public void testStartStopAllNamespacesWithPodSetsOnly(VertxTestContext context) throws InterruptedException {
        startStopAllNamespaces(context, "*", true);
    }

    /**
     * Asserts that Cluster Operator starts and then stops a verticle in each namespace
     *
     * @param context     test context passed in for assertions
     * @param namespaces  namespaces the operator should be watching and operating on
     * @param podSetsOnly Only PodSets should be refactored
     */
    private void startStop(VertxTestContext context, String namespaces, boolean podSetsOnly) throws InterruptedException {
        AtomicInteger numWatchers = new AtomicInteger(0);
        AtomicInteger numInformers = new AtomicInteger(0);

        KubernetesClient client = mock(KubernetesClient.class);

        try {
            when(client.getMasterUrl()).thenReturn(new URL("http://localhost"));
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }

        MixedOperation mockCms = mock(MixedOperation.class);
        when(client.resources(any(), any())).thenReturn(mockCms);

        MixedOperation mockPods = mock(MixedOperation.class);
        when(client.pods()).thenReturn(mockPods);

        List<String> namespaceList = asList(namespaces.split(" *,+ *"));
        for (String namespace: namespaceList) {
            // Mock CRs
            Indexer mockCmIndexer = mock(Indexer.class);
            SharedIndexInformer mockCmInformer = mock(SharedIndexInformer.class);
            when(mockCmInformer.getIndexer()).thenReturn(mockCmIndexer);
            when(mockCmInformer.stopped()).thenReturn(CompletableFuture.completedFuture(null));

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
            when(mockNamespacedCms.runnableInformer(anyLong())).thenAnswer(i -> {
                numInformers.getAndIncrement();
                return mockCmInformer;
            });

            when(mockNamespacedCms.withLabels(any())).thenReturn(mockNamespacedCms);
            when(mockCms.inNamespace(namespace)).thenReturn(mockNamespacedCms);

            // Mock Pods
            Indexer mockPodIndexer = mock(Indexer.class);
            SharedIndexInformer mockPodInformer = mock(SharedIndexInformer.class);
            MixedOperation mockNamespacedPods = mock(MixedOperation.class);
            when(mockPodInformer.getIndexer()).thenReturn(mockPodIndexer);
            when(mockPodInformer.stopped()).thenReturn(CompletableFuture.completedFuture(null));
            when(mockNamespacedPods.runnableInformer(anyLong())).thenAnswer(i -> {
                numInformers.getAndIncrement();
                return mockPodInformer;
            });
            when(mockNamespacedPods.withLabels(any())).thenReturn(mockNamespacedPods);
            when(mockNamespacedPods.withLabelSelector(any(LabelSelector.class))).thenReturn(mockNamespacedPods);
            when(mockPods.inNamespace(namespace)).thenReturn(mockNamespacedPods);
        }

        Map<String, String> env = buildEnv(namespaces, podSetsOnly);

        CountDownLatch latch = new CountDownLatch(namespaceList.size() + 1);

        Main.deployClusterOperatorVerticles(VERTX, client, ResourceUtils.metricsProvider(), new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                    ClusterOperatorConfig.buildFromMap(env, KafkaVersionTestUtils.getKafkaVersionLookup()), new ShutdownHook())

            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertThat("A verticle per namespace", VERTX.deploymentIDs(), hasSize(namespaceList.size()));
                for (String deploymentId: VERTX.deploymentIDs()) {
                    VERTX.undeploy(deploymentId, asyncResult -> {
                        if (asyncResult.failed()) {
                            LOGGER.error("Failed to undeploy {}", deploymentId);
                            context.failNow(asyncResult.cause());
                        }
                        latch.countDown();
                    });
                }

                int maximumExpectedNumberOfWatchers = podSetsOnly ? 0 : 8 * namespaceList.size();
                assertThat("Looks like there were more watchers than namespaces",
                        numWatchers.get(), lessThanOrEqualTo(maximumExpectedNumberOfWatchers));

                int expectedNumberOfInformers = 5 * namespaceList.size();
                assertThat("Looks like there were more informers than namespaces",
                        numInformers.get(), is(expectedNumberOfInformers));

                latch.countDown();
            })));
        latch.await(10, TimeUnit.SECONDS);
        context.completeNow();
    }

    /**
     * Asserts that Cluster Operator starts and then stops a verticle in every namespace using the namespace wildcard (*)
     *
     * @param context     test context passed in for assertions
     * @param namespaces  namespaces the operator should be watching and operating on
     * @param podSetsOnly Only PodSets should be refactored
     */
    private void startStopAllNamespaces(VertxTestContext context, String namespaces, boolean podSetsOnly) throws InterruptedException {
        AtomicInteger numWatchers = new AtomicInteger(0);
        AtomicInteger numInformers = new AtomicInteger(0);

        KubernetesClient client = mock(KubernetesClient.class);

        try {
            when(client.getMasterUrl()).thenReturn(new URL("http://localhost"));
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }

        // Mock CRs
        MixedOperation mockCms = mock(MixedOperation.class);
        when(client.resources(any(), any())).thenReturn(mockCms);

        Indexer mockCmIndexer = mock(Indexer.class);
        SharedIndexInformer mockCmInformer = mock(SharedIndexInformer.class);
        when(mockCmInformer.getIndexer()).thenReturn(mockCmIndexer);
        when(mockCmInformer.stopped()).thenReturn(CompletableFuture.completedFuture(null));

        AnyNamespaceOperation mockFilteredCms = mock(AnyNamespaceOperation.class);
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
        when(mockFilteredCms.runnableInformer(anyLong())).thenAnswer(i -> {
            numInformers.getAndIncrement();
            return mockCmInformer;
        });
        when(mockCms.inAnyNamespace()).thenReturn(mockFilteredCms);

        // Mock Pods
        MixedOperation mockPods = mock(MixedOperation.class);
        AnyNamespaceOperation mockFilteredPods = mock(AnyNamespaceOperation.class);
        Indexer mockPodIndexer = mock(Indexer.class);
        SharedIndexInformer mockPodInformer = mock(SharedIndexInformer.class);
        when(client.pods()).thenReturn(mockPods);
        when(mockFilteredPods.withLabels(any())).thenReturn(mockFilteredPods);
        when(mockFilteredPods.withLabelSelector(any(LabelSelector.class))).thenReturn(mockFilteredPods);
        when(mockPods.inAnyNamespace()).thenReturn(mockFilteredPods);
        when(mockPodInformer.getIndexer()).thenReturn(mockPodIndexer);
        when(mockPodInformer.stopped()).thenReturn(CompletableFuture.completedFuture(null));
        when(mockFilteredPods.runnableInformer(anyLong())).thenAnswer(i -> {
            numInformers.getAndIncrement();
            return mockPodInformer;
        });

        // Run the operator
        Map<String, String> env = buildEnv(namespaces, podSetsOnly);

        CountDownLatch latch = new CountDownLatch(2);

        Main.deployClusterOperatorVerticles(VERTX, client, ResourceUtils.metricsProvider(), new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                ClusterOperatorConfig.buildFromMap(env, KafkaVersionTestUtils.getKafkaVersionLookup()), new ShutdownHook())
            .onComplete(context.succeeding(v -> context.verify(() -> {
                assertThat("A verticle per namespace", VERTX.deploymentIDs(), hasSize(1));
                for (String deploymentId: VERTX.deploymentIDs()) {
                    VERTX.undeploy(deploymentId, asyncResult -> {
                        if (asyncResult.failed()) {
                            LOGGER.error("Failed to undeploy {}", deploymentId);
                            context.failNow(asyncResult.cause());
                        }
                        latch.countDown();
                    });
                }

                int maximumExpectedNumberOfWatchers = podSetsOnly ? 0 : 8;
                assertThat("Looks like there were more watchers than custom resources", numWatchers.get(), lessThanOrEqualTo(maximumExpectedNumberOfWatchers));

                int numberOfInformers = 5;
                assertThat("Looks like there were more informers than we should", numInformers.get(), is(numberOfInformers));

                latch.countDown();
            })));
        latch.await(10, TimeUnit.SECONDS);
        context.completeNow();
    }
}
