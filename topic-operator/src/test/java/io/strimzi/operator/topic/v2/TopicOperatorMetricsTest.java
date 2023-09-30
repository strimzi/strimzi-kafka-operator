/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.v2;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.informers.cache.ItemStore;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.search.RequiredSearch;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaTopicBuilder;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.MicrometerMetricsProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.metrics.MetricsHolder;
import io.strimzi.operator.common.metrics.OperatorMetricsHolder;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static io.strimzi.api.kafka.model.KafkaTopic.RESOURCE_KIND;
import static io.strimzi.operator.topic.v2.BatchingTopicController.topicName;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Mockito.mock;

@ExtendWith(KafkaClusterExtension.class)
public class TopicOperatorMetricsTest {
    private static final String NAMESPACE = "ns";
    private static final int KT_RESOURCES = 100;
    private static final int MAX_QUEUE_SIZE = 200;
    private static final int MAX_BATCH_SIZE = 10;
    private static final int MAX_THREADS = 2;
    private static final long MAX_BATCH_LINGER_MS = 10_000;

    private static KubernetesClient client;
    private static MetricsHolder metrics;

    @BeforeAll
    public static void beforeAll() {
        TopicOperatorTestUtil.setupKubeCluster(NAMESPACE);
        client = new KubernetesClientBuilder().build();

        MetricsProvider metricsProvider = new MicrometerMetricsProvider(new SimpleMeterRegistry());
        metrics = new OperatorMetricsHolder(RESOURCE_KIND, null, metricsProvider);
    }

    @AfterAll
    public static void afterAll(TestInfo testInfo) {
        TopicOperatorTestUtil.cleanupNamespace(client, testInfo, NAMESPACE);
        TopicOperatorTestUtil.teardownKubeCluster2(NAMESPACE);
        client.close();
    }

    @Test
    public void shouldHaveMetricsAfterSomeEvents() {
        BatchingLoop mockQueue = mock(BatchingLoop.class);
        TopicOperatorEventHandler eventHandler = new TopicOperatorEventHandler(mockQueue, true, metrics, NAMESPACE);
        for (int i = 0; i < KT_RESOURCES; i++) {
            KafkaTopic kt = new KafkaTopic();
            kt.getMetadata().setNamespace(NAMESPACE);
            kt.getMetadata().setName("t" + i);
            kt.getMetadata().setResourceVersion("100100");
            eventHandler.onAdd(kt);
        }
        String[] tags = new String[]{"kind", RESOURCE_KIND, "namespace", NAMESPACE};
        assertGaugeMatches("strimzi.resources", is(Double.valueOf(KT_RESOURCES)), tags);

        for (int i = 0; i < KT_RESOURCES; i++) {
            KafkaTopic kt = new KafkaTopic();
            kt.getMetadata().setNamespace(NAMESPACE);
            kt.getMetadata().setName("t" + i);
            kt.getMetadata().setResourceVersion("1");
            eventHandler.onDelete(kt, false);
        }
        assertGaugeMatches("strimzi.resources", is(0.0), tags);
    }

    @Test
    public void shouldHaveMetricsAfterSomeUpserts() throws InterruptedException {
        BatchingLoop batchingLoop = createAndStartBatchingLoop();
        for (int i = 0; i < KT_RESOURCES; i++) {
            if (i < KT_RESOURCES / 2) {
                batchingLoop.offer(new TopicUpsert(0, NAMESPACE, "t0", "10010" + i));
            } else {
                batchingLoop.offer(new TopicUpsert(0, NAMESPACE, "t" + i, "100100"));
            }
        }

        String[] tags = new String[]{"kind", RESOURCE_KIND, "namespace", NAMESPACE};
        assertGaugeMatches("strimzi.reconciliations.max.queue.size", greaterThan(0.0), tags);
        assertGaugeMatches("strimzi.reconciliations.max.queue.size", lessThanOrEqualTo(Double.valueOf(MAX_QUEUE_SIZE)), tags);
        assertGaugeMatches("strimzi.reconciliations.max.batch.size", is(Double.valueOf(MAX_BATCH_SIZE)), tags);
        assertCounterMatches("strimzi.reconciliations.locked", greaterThan(0.0), tags);
        batchingLoop.stop();
    }
    
    private static BatchingLoop createAndStartBatchingLoop() throws InterruptedException {
        BatchingTopicController controller = mock(BatchingTopicController.class);
        ItemStore<KafkaTopic> itemStore = mock(ItemStore.class);
        Runnable stop = mock(Runnable.class);
        BatchingLoop batchingLoop = new BatchingLoop(
            MAX_QUEUE_SIZE,
            controller,
            MAX_THREADS,
            MAX_BATCH_SIZE,
            MAX_BATCH_LINGER_MS,
            itemStore,
            stop,
            metrics,
            NAMESPACE);
        batchingLoop.start();
        while (!batchingLoop.isReady()) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
        return batchingLoop;
    }

    @Test
    public void shouldHaveMetricsAfterSomeReconciliations(KafkaCluster cluster) throws ExecutionException, InterruptedException {
        Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        BatchingTopicController controller = new BatchingTopicController(Map.of("key", "VALUE"), admin, client, true, metrics, NAMESPACE);

        KafkaTopic t1 = createResource(client, "t1", "t1");
        KafkaTopic t2 = createResource(client, "t2", "t1");
        List<ReconcilableTopic> updateBatch = List.of(
            new ReconcilableTopic(new Reconciliation("test", RESOURCE_KIND, NAMESPACE, topicName(t1)), t1, topicName(t1)),
            new ReconcilableTopic(new Reconciliation("test", RESOURCE_KIND, NAMESPACE, topicName(t2)), t2, topicName(t2))
        );
        controller.onUpdate(updateBatch);
        List<ReconcilableTopic> deleteBatch = List.of(
            new ReconcilableTopic(new Reconciliation("test", RESOURCE_KIND, NAMESPACE, topicName(t1)), t1, topicName(t1))
        );
        controller.onDelete(deleteBatch);

        String[] tags = new String[]{"kind", RESOURCE_KIND, "namespace", NAMESPACE};
        assertCounterMatches("strimzi.reconciliations", is(3.0), tags);
        assertCounterMatches("strimzi.reconciliations.successful", is(2.0), tags);
        assertCounterMatches("strimzi.reconciliations.failed", is(1.0), tags);
        assertTimerMatches("strimzi.reconciliations.duration", is(3.0), greaterThan(0.0), tags);
    }

    private KafkaTopic createResource(KubernetesClient client, String resourceName, String topicName) {
        var kt = Crds.topicOperation(client).
            resource(new KafkaTopicBuilder()
                .withNewMetadata()
                    .withName(resourceName)
                    .withNamespace(NAMESPACE)
                    .addToLabels("key", "VALUE")
                .endMetadata()
                .withNewSpec()
                    .withTopicName(topicName)
                    .withPartitions(2)
                    .withReplicas(1)
                .endSpec().build()).create();
        return kt;
    }

    private static void assertCounterMatches(String counterName, Matcher<Double> matcher, String... tags) {
        MeterRegistry registry = metrics.metricsProvider().meterRegistry();
        RequiredSearch requiredSearch = registry.get(counterName).tags(tags);
        assertThat(requiredSearch.counter().count(), matcher);
    }

    private static void assertGaugeMatches(String counterName, Matcher<Double> matcher, String... tags) {
        MeterRegistry registry = metrics.metricsProvider().meterRegistry();
        RequiredSearch requiredSearch = registry.get(counterName).tags(tags);
        assertThat(requiredSearch.gauge().value(), matcher);
    }

    private static void assertTimerMatches(String timerName, Matcher<Double> count, Matcher<Double> greaterThan, String... tags) {
        MeterRegistry registry = metrics.metricsProvider().meterRegistry();
        RequiredSearch requiredSearch = registry.get(timerName).tags(tags);
        assertThat(Double.valueOf(requiredSearch.timer().count()), count);
        assertThat(requiredSearch.timer().totalTime(TimeUnit.MILLISECONDS), greaterThan);
    }
}
