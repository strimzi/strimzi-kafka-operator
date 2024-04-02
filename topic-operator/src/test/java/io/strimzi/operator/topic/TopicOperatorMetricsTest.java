/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.informers.cache.ItemStore;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import io.micrometer.core.instrument.search.RequiredSearch;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicBuilder;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.metrics.MetricsHolder;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsHolder;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsProvider;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.strimzi.api.ResourceAnnotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION;
import static io.strimzi.api.kafka.model.topic.KafkaTopic.RESOURCE_KIND;
import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Mockito.mock;

@ExtendWith(KafkaClusterExtension.class)
public class TopicOperatorMetricsTest {
    private static final String NAMESPACE = "topic-operator-test";
    private static final int MAX_QUEUE_SIZE = 200;
    private static final int MAX_BATCH_SIZE = 10;
    private static final int MAX_THREADS = 2;
    private static final long MAX_BATCH_LINGER_MS = 10_000;

    private static KubernetesClient client;
    private static TopicOperatorMetricsHolder metrics;

    @BeforeAll
    public static void beforeAll(TestInfo testInfo) {
        TopicOperatorTestUtil.setupKubeCluster(testInfo, NAMESPACE);
        client = new KubernetesClientBuilder().build();
        TopicOperatorMetricsProvider metricsProvider = new TopicOperatorMetricsProvider(new SimpleMeterRegistry());
        metrics = new TopicOperatorMetricsHolder(RESOURCE_KIND, null, metricsProvider);
    }

    @AfterAll
    public static void afterAll(TestInfo testInfo) {
        TopicOperatorTestUtil.cleanupNamespace(client, testInfo, NAMESPACE);
        TopicOperatorTestUtil.teardownKubeCluster(NAMESPACE);
        client.close();
    }

    @Test
    public void shouldHaveMetricsAfterSomeEvents() throws InterruptedException {
        var config = TopicOperatorConfig.buildFromMap(Map.of(
            TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:9092",
            TopicOperatorConfig.NAMESPACE.key(), NAMESPACE));
        BatchingLoop mockQueue = mock(BatchingLoop.class);
        TopicOperatorEventHandler eventHandler = new TopicOperatorEventHandler(config, mockQueue, metrics);
        int numOfTestResources = 100;
        for (int i = 0; i < numOfTestResources; i++) {
            KafkaTopic kt = createKafkaTopic("t" + i, "100100");
            eventHandler.onAdd(kt);
        }
        String[] tags = new String[]{"kind", RESOURCE_KIND, "namespace", NAMESPACE};
        assertMetricMatches(MetricsHolder.METRICS_RESOURCES, tags, "gauge", is(Double.valueOf(numOfTestResources)));

        for (int i = 0; i < numOfTestResources; i++) {
            KafkaTopic kt = createKafkaTopic("t" + i, "100100");
            eventHandler.onDelete(kt, false);
        }
        assertMetricMatches(MetricsHolder.METRICS_RESOURCES, tags, "gauge", is(0.0));

        KafkaTopic foo1 = createKafkaTopic("my-topic", "100100");
        eventHandler.onAdd(foo1);
        assertMetricMatches(MetricsHolder.METRICS_RESOURCES_PAUSED, tags, "gauge", is(0.0));

        KafkaTopic foo2 = createKafkaTopic("my-topic", "100100");
        foo2.getMetadata().setAnnotations(Map.of(ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true"));
        eventHandler.onUpdate(foo1, foo2);
        assertMetricMatches(MetricsHolder.METRICS_RESOURCES_PAUSED, tags, "gauge", is(1.0));

        eventHandler.onUpdate(foo1, foo1);
        assertMetricMatches(MetricsHolder.METRICS_RESOURCES_PAUSED, tags, "gauge", is(1.0));

        KafkaTopic foo3 = createKafkaTopic("my-topic", "100100");
        foo3.getMetadata().setAnnotations(Map.of(ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "false"));
        eventHandler.onUpdate(foo2, foo3);
        assertMetricMatches(MetricsHolder.METRICS_RESOURCES_PAUSED, tags, "gauge", is(0.0));
    }

    private static KafkaTopic createKafkaTopic(String name, String version) {
        KafkaTopic kt = new KafkaTopic();
        kt.getMetadata().setNamespace(NAMESPACE);
        kt.getMetadata().setName(name);
        kt.getMetadata().setResourceVersion(version);
        return kt;
    }

    @Test
    public void shouldHaveMetricsAfterSomeUpserts() throws InterruptedException {
        BatchingLoop batchingLoop = createAndStartBatchingLoop();
        int numOfTestResources = 100;
        for (int i = 0; i < numOfTestResources; i++) {
            if (i < numOfTestResources / 2) {
                batchingLoop.offer(new TopicUpsert(0, NAMESPACE, "t0", "10010" + i));
            } else {
                batchingLoop.offer(new TopicUpsert(0, NAMESPACE, "t" + i, "100100"));
            }
        }

        String[] tags = new String[]{"kind", RESOURCE_KIND, "namespace", NAMESPACE};
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_MAX_QUEUE_SIZE, tags, "gauge", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_MAX_QUEUE_SIZE, tags, "gauge", lessThanOrEqualTo(Double.valueOf(MAX_QUEUE_SIZE)));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_MAX_BATCH_SIZE, tags, "gauge", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_MAX_BATCH_SIZE, tags, "gauge", lessThanOrEqualTo(Double.valueOf(MAX_BATCH_SIZE)));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_LOCKED, tags, "counter", greaterThan(0.0));
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
        return batchingLoop;
    }

    @Test
    public void shouldHaveMetricsAfterSomeReconciliations(KafkaCluster cluster) throws InterruptedException {
        Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        var config = Mockito.mock(TopicOperatorConfig.class);
        Mockito.doReturn(NAMESPACE).when(config).namespace();
        Mockito.doReturn(true).when(config).useFinalizer();
        Mockito.doReturn(false).when(config).enableAdditionalMetrics();
        var replicasChangeClient = Mockito.mock(ReplicasChangeHandler.class);
        BatchingTopicController controller = new BatchingTopicController(config, Map.of("key", "VALUE"), admin, client, metrics, replicasChangeClient);

        KafkaTopic t1 = createResource(client, "t1", "t1");
        KafkaTopic t2 = createResource(client, "t2", "t1");
        List<ReconcilableTopic> updateBatch = List.of(
            new ReconcilableTopic(new Reconciliation("test", RESOURCE_KIND, NAMESPACE, TopicOperatorUtil.topicName(t1)), t1, TopicOperatorUtil.topicName(t1)),
            new ReconcilableTopic(new Reconciliation("test", RESOURCE_KIND, NAMESPACE, TopicOperatorUtil.topicName(t2)), t2, TopicOperatorUtil.topicName(t2))
        );
        controller.onUpdate(updateBatch);
        List<ReconcilableTopic> deleteBatch = List.of(
            new ReconcilableTopic(new Reconciliation("test", RESOURCE_KIND, NAMESPACE, TopicOperatorUtil.topicName(t1)), t1, TopicOperatorUtil.topicName(t1))
        );
        controller.onDelete(deleteBatch);

        String[] tags = new String[]{"kind", RESOURCE_KIND, "namespace", NAMESPACE};
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS, tags, "counter", is(2.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL, tags, "counter", is(2.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_FAILED, tags, "counter", is(1.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_DURATION, tags, "timer", greaterThan(0.0));

        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_ADD_FINALIZER_DURATION, tags, "timer", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_REMOVE_FINALIZER_DURATION, tags, "timer", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_CREATE_TOPICS_DURATION, tags, "timer", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_UPDATE_TOPICS_DURATION, tags, "timer", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_LIST_REASSIGNMENTS_DURATION, tags, "timer", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_ALTER_CONFIGS_DURATION, tags, "timer", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_CREATE_PARTITIONS_DURATION, tags, "timer", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_DESCRIBE_TOPICS_DURATION, tags, "timer", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_DESCRIBE_CONFIGS_DURATION, tags, "timer", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_DELETE_TOPICS_DURATION, tags, "timer", greaterThan(0.0));
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

    private static void assertMetricMatches(String name, String[] tags, String type, Matcher<Double> matcher) throws InterruptedException {
        // wait some time because events are queued, and processing may be delayed
        int timeoutSec = 120;
        RequiredSearch requiredSearch = null;
        while (requiredSearch == null && timeoutSec-- > 0) {
            try {
                requiredSearch = metrics.metricsProvider().meterRegistry().get(name).tags(tags);
                switch (type) {
                    case "counter":
                        assertThat(requiredSearch.counter().count(), matcher);
                        break;
                    case "gauge":
                        assertThat(requiredSearch.gauge().value(), matcher);
                        break;
                    case "timer":
                        assertThat(requiredSearch.timer().totalTime(TimeUnit.MILLISECONDS), matcher);
                        break;
                    default:
                        throw new RuntimeException(format("Unknown metric type %s", type));
                }
            } catch (MeterNotFoundException mnfe) {
                TimeUnit.SECONDS.sleep(1);
            }
        }
        if (requiredSearch == null) {
            throw new RuntimeException(format("Unable to find metric %s with tags %s", name, Arrays.toString(tags)));
        }
    }
}
