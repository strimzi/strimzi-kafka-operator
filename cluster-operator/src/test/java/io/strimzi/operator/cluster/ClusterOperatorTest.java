/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.AnyNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.cache.Indexer;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.operator.PlatformFeaturesAvailability;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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

    private static Map<String, String> buildEnv(String namespaces, boolean strimziPodSets, boolean podSetsOnly) {
        Map<String, String> env = new HashMap<>();
        env.put(ClusterOperatorConfig.STRIMZI_NAMESPACE, namespaces);
        env.put(ClusterOperatorConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL_MS, "120000");
        env.put(ClusterOperatorConfig.STRIMZI_KAFKA_IMAGES, KafkaVersionTestUtils.getKafkaImagesEnvVarString());
        env.put(ClusterOperatorConfig.STRIMZI_KAFKA_CONNECT_IMAGES, KafkaVersionTestUtils.getKafkaConnectImagesEnvVarString());
        env.put(ClusterOperatorConfig.STRIMZI_KAFKA_MIRROR_MAKER_IMAGES, KafkaVersionTestUtils.getKafkaMirrorMakerImagesEnvVarString());
        env.put(ClusterOperatorConfig.STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES, KafkaVersionTestUtils.getKafkaMirrorMaker2ImagesEnvVarString());

        if (!strimziPodSets) {
            env.put(ClusterOperatorConfig.STRIMZI_FEATURE_GATES, "-UseStrimziPodSets");
        }

        if (podSetsOnly) {
            env.put(ClusterOperatorConfig.STRIMZI_POD_SET_RECONCILIATION_ONLY, "true");
        }

        return env;
    }

    @AfterAll
    public static void afterAll()   {
        PodSecurityProviderFactory.initialize();
    }

    @Test
    public void testStartStopSingleNamespaceOnOpenShift(VertxTestContext context) throws InterruptedException {
        startStop(context, "namespace", true, false, false);
    }

    @Test
    public void testStartStopMultiNamespaceOnOpenShift(VertxTestContext context) throws InterruptedException {
        startStop(context, "namespace1,namespace2", true, false, false);
    }

    @Test
    public void testStartStopSingleNamespaceOnK8s(VertxTestContext context) throws InterruptedException {
        startStop(context, "namespace", false, false, false);
    }

    @Test
    public void testStartStopMultiNamespaceOnK8s(VertxTestContext context) throws InterruptedException {
        startStop(context, "namespace1,namespace2", false, false, false);
    }

    @Test
    public void testStartStopSingleNamespaceWithPodSets(VertxTestContext context) throws InterruptedException {
        startStop(context, "namespace", false, true, false);
    }

    @Test
    public void testStartStopMultiNamespaceWithPodSets(VertxTestContext context) throws InterruptedException {
        startStop(context, "namespace1,namespace2", false, true, false);
    }

    @Test
    public void testStartStopMultiNamespaceWithPodSetsOnly(VertxTestContext context) throws InterruptedException {
        startStop(context, "namespace1,namespace2", false, true, true);
    }

    @Test
    public void testStartStopAllNamespacesOnOpenShift(VertxTestContext context) throws InterruptedException {
        startStopAllNamespaces(context, "*", true, false, false);
    }

    @Test
    public void testStartStopAllNamespacesOnK8s(VertxTestContext context) throws InterruptedException {
        startStopAllNamespaces(context, "*", false, false, false);
    }

    @Test
    public void testStartStopAllNamespacesWithPodSets(VertxTestContext context) throws InterruptedException {
        startStopAllNamespaces(context, "*", false, true, false);
    }

    @Test
    public void testStartStopAllNamespacesWithPodSetsOnly(VertxTestContext context) throws InterruptedException {
        startStopAllNamespaces(context, "*", false, true, true);
    }

    /**
     * Asserts that Cluster Operator starts and then stops a verticle in each namespace
     *
     * @param context       test context passed in for assertions
     * @param namespaces    namespaces the operator should be watching and operating on
     * @param podSetsOnly   Only PodSets should be refactored
     */
    private void startStop(VertxTestContext context, String namespaces, boolean openShift, boolean strimziPodSets, boolean podSetsOnly) throws InterruptedException {
        AtomicInteger numWatchers = new AtomicInteger(0);
        AtomicInteger numInformers = new AtomicInteger(0);

        KubernetesClient client;
        if (openShift) {
            client = mock(OpenShiftClient.class);
            when(client.isAdaptable(eq(OpenShiftClient.class))).thenReturn(true);
            when(client.adapt(eq(OpenShiftClient.class))).thenReturn((OpenShiftClient) client);
        } else {
            client = mock(KubernetesClient.class);
            when(client.isAdaptable(eq(OpenShiftClient.class))).thenReturn(false);
        }

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
            when(mockNamespacedCms.inform()).thenAnswer(i -> {
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
            when(mockNamespacedPods.inform()).thenAnswer(i -> {
                numInformers.getAndIncrement();
                return mockPodInformer;
            });
            when(mockNamespacedPods.withLabels(any())).thenReturn(mockNamespacedPods);
            when(mockPods.inNamespace(namespace)).thenReturn(mockNamespacedPods);
        }

        Map<String, String> env = buildEnv(namespaces, strimziPodSets, podSetsOnly);

        CountDownLatch latch = new CountDownLatch(namespaceList.size() + 1);

        Main.deployClusterOperatorVerticles(VERTX, client, ResourceUtils.metricsProvider(), new PlatformFeaturesAvailability(openShift, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                    ClusterOperatorConfig.fromMap(env, KafkaVersionTestUtils.getKafkaVersionLookup()))
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

                int maximumExpectedNumberOfWatchers = podSetsOnly ? 0 : 7 * namespaceList.size();
                assertThat("Looks like there were more watchers than namespaces",
                        numWatchers.get(), lessThanOrEqualTo(maximumExpectedNumberOfWatchers));

                int expectedNumberOfInformers = strimziPodSets ? 3 * namespaceList.size() : 0;
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
     * @param context       test context passed in for assertions
     * @param namespaces    namespaces the operator should be watching and operating on
     * @param podSetsOnly   Only PodSets should be refactored
     */
    private void startStopAllNamespaces(VertxTestContext context, String namespaces, boolean openShift, boolean strimziPodSets, boolean podSetsOnly) throws InterruptedException {
        AtomicInteger numWatchers = new AtomicInteger(0);
        AtomicInteger numInformers = new AtomicInteger(0);

        KubernetesClient client;
        if (openShift) {
            client = mock(OpenShiftClient.class);
            when(client.isAdaptable(eq(OpenShiftClient.class))).thenReturn(true);
            when(client.adapt(eq(OpenShiftClient.class))).thenReturn((OpenShiftClient) client);
        } else {
            client = mock(KubernetesClient.class);
            when(client.isAdaptable(eq(OpenShiftClient.class))).thenReturn(false);
        }

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
        when(mockFilteredCms.inform()).thenAnswer(i -> {
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
        when(mockPods.inAnyNamespace()).thenReturn(mockFilteredPods);
        when(mockPodInformer.getIndexer()).thenReturn(mockPodIndexer);
        when(mockFilteredPods.inform()).thenAnswer(i -> {
            numInformers.getAndIncrement();
            return mockPodInformer;
        });

        // Run the operator
        Map<String, String> env = buildEnv(namespaces, strimziPodSets, podSetsOnly);

        CountDownLatch latch = new CountDownLatch(2);
        Main.deployClusterOperatorVerticles(VERTX, client, ResourceUtils.metricsProvider(), new PlatformFeaturesAvailability(openShift, KubernetesVersion.MINIMAL_SUPPORTED_VERSION),
                ClusterOperatorConfig.fromMap(env, KafkaVersionTestUtils.getKafkaVersionLookup()))
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

                int maximumExpectedNumberOfWatchers = podSetsOnly ? 0 : 7;
                assertThat("Looks like there were more watchers than custom resources", numWatchers.get(), lessThanOrEqualTo(maximumExpectedNumberOfWatchers));

                int numberOfInformers = strimziPodSets ? 3 : 0;
                assertThat("Looks like there were more informers than we should", numInformers.get(), is(numberOfInformers));

                latch.countDown();
            })));
        latch.await(10, TimeUnit.SECONDS);
        context.completeNow();
    }
}
