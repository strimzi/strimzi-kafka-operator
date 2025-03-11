/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.cache.ItemStore;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.strimzi.api.ResourceAnnotations;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicBuilder;
import io.strimzi.operator.common.metrics.MetricsHolder;
import io.strimzi.operator.topic.cruisecontrol.CruiseControlClient;
import io.strimzi.operator.topic.cruisecontrol.CruiseControlHandler;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsHolder;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsProvider;
import io.strimzi.operator.topic.model.TopicEvent.TopicUpsert;
import io.strimzi.test.container.StrimziKafkaCluster;
import io.strimzi.test.interfaces.TestSeparator;
import io.strimzi.test.mockkube3.MockKube3;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;

public class MetricsIT implements TestSeparator {
    private static final Logger LOGGER = LogManager.getLogger(MetricsIT.class);

    private static final String NAMESPACE = TestUtil.namespaceName(MetricsIT.class);
    private static final int MAX_QUEUE_SIZE = 200;
    private static final int MAX_BATCH_SIZE = 10;
    private static final long MAX_BATCH_LINGER_MS = 10_000;

    private static MockKube3 mockKube;
    private static KubernetesClient kubernetesClient;
    private static StrimziKafkaCluster kafkaCluster;

    @BeforeAll
    public static void beforeAll() {
        mockKube = new MockKube3.MockKube3Builder()
            .withKafkaTopicCrd()
            .withDeletionController()
            .withNamespaces(NAMESPACE)
            .build();
        mockKube.start();
        kubernetesClient = mockKube.client();

        kafkaCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withKraft()
                .withNumberOfBrokers(1)
                .withInternalTopicReplicationFactor(1)
                .withSharedNetwork()
                .build();
        kafkaCluster.start();
    }

    @AfterAll
    public static void afterAll() {
        kafkaCluster.stop();
        mockKube.stop();
    }

    @AfterEach
    public void afterEach() {
        TestUtil.cleanupNamespace(kubernetesClient, NAMESPACE);
    }

    @Test
    public void eventHandlerMetrics() throws InterruptedException {
        var config = TopicOperatorConfig.buildFromMap(Map.of(
            TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:9092",
            TopicOperatorConfig.NAMESPACE.key(), NAMESPACE)
        );
        var metricsHolder = new TopicOperatorMetricsHolder(KafkaTopic.RESOURCE_KIND, null, new TopicOperatorMetricsProvider(new SimpleMeterRegistry()));
        var eventHandler = new TopicEventHandler(config, mock(BatchingLoop.class), metricsHolder);

        var numOfTestResources = 100;
        for (int i = 0; i < numOfTestResources; i++) {
            KafkaTopic kafkaTopic = buildTopicWithVersion("my-topic" + i);
            eventHandler.onAdd(kafkaTopic);
        }
        assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RESOURCES, "gauge", is((double) numOfTestResources));

        for (int i = 0; i < numOfTestResources; i++) {
            KafkaTopic kafkaTopic = buildTopicWithVersion("my-topic" + i);
            eventHandler.onDelete(kafkaTopic, false);
        }
        assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RESOURCES, "gauge", is(0.0));

        var t1 = buildTopicWithVersion("my-topic-1");
        var t2 = buildTopicWithVersion("my-topic-2");
        t2.getMetadata().setAnnotations(Map.of(ResourceAnnotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true"));
        eventHandler.onUpdate(t1, t2);
        assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RESOURCES_PAUSED, "gauge", is(1.0));

        var t3 = buildTopicWithVersion("t3");
        t3.getMetadata().setAnnotations(Map.of(ResourceAnnotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "false"));
        eventHandler.onUpdate(t2, t3);
        assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RESOURCES_PAUSED, "gauge", is(0.0));
    }

    @Test
    public void batchingLoopMetrics() throws InterruptedException {
        var config = TopicOperatorConfig.buildFromMap(Map.of(
            TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:9092",
            TopicOperatorConfig.NAMESPACE.key(), NAMESPACE,
            TopicOperatorConfig.MAX_QUEUE_SIZE.key(), String.valueOf(MAX_QUEUE_SIZE),
            TopicOperatorConfig.MAX_BATCH_SIZE.key(), String.valueOf(MAX_BATCH_SIZE),
            TopicOperatorConfig.MAX_BATCH_LINGER_MS.key(), String.valueOf(MAX_BATCH_LINGER_MS)
        ));
        var metricsHolder = new TopicOperatorMetricsHolder(KafkaTopic.RESOURCE_KIND, null, new TopicOperatorMetricsProvider(new SimpleMeterRegistry()));
        var batchingLoop = new BatchingLoop(config, mock(BatchingTopicController.class), 1, mock(ItemStore.class), mock(Runnable.class), metricsHolder);
        batchingLoop.start();
        
        int numOfTestResources = 100;
        for (int i = 0; i < numOfTestResources; i++) {
            if (i < numOfTestResources / 2) {
                batchingLoop.offer(new TopicUpsert(0, NAMESPACE, "t0", "10010" + i));
            } else {
                batchingLoop.offer(new TopicUpsert(0, NAMESPACE, "t" + i, "100100"));
            }
        }

        assertMetricMatches(metricsHolder, TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_MAX_QUEUE_SIZE, "gauge", greaterThan(0.0));
        assertMetricMatches(metricsHolder, TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_MAX_QUEUE_SIZE, "gauge", lessThanOrEqualTo((double) MAX_QUEUE_SIZE));
        assertMetricMatches(metricsHolder, TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_MAX_BATCH_SIZE, "gauge", greaterThan(0.0));
        assertMetricMatches(metricsHolder, TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_MAX_BATCH_SIZE, "gauge", lessThanOrEqualTo((double) MAX_BATCH_SIZE));
        assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS_LOCKED, "counter", greaterThan(0.0));
        batchingLoop.stop();
    }

    @Test
    @SuppressWarnings({"checkstyle:JavaNCSS", "checkstyle:MethodLength"})
    public void batchingTopicControllerMetrics() throws InterruptedException {
        try (var kafkaAdminClient = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers()))) {
            var config = TopicOperatorConfig.buildFromMap(Map.of(
                    TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:9092",
                    TopicOperatorConfig.NAMESPACE.key(), NAMESPACE,
                    TopicOperatorConfig.USE_FINALIZERS.key(), "true",
                    TopicOperatorConfig.ENABLE_ADDITIONAL_METRICS.key(), "true",
                    TopicOperatorConfig.CRUISE_CONTROL_ENABLED.key(), "true"
            ));

            var cruiseControlClient = Mockito.mock(CruiseControlClient.class);
            var userTaskId = "8911ca89-351f-888-8d0f-9aade00e098h";
            Mockito.doReturn(userTaskId).when(cruiseControlClient).topicConfiguration(anyList());
            var userTaskResponse = new CruiseControlClient.UserTasksResponse(List.of(
                    new CruiseControlClient.UserTask("Active", null, null, userTaskId, System.currentTimeMillis())), 1);
            Mockito.doReturn(userTaskResponse).when(cruiseControlClient).userTasks(Set.of(userTaskId));

            var metricsHolder = new TopicOperatorMetricsHolder(KafkaTopic.RESOURCE_KIND, null,
                    new TopicOperatorMetricsProvider(new SimpleMeterRegistry()));
            var controller = new BatchingTopicController(config, Map.of("key", "VALUE"),
                    new KubernetesHandler(config, metricsHolder, kubernetesClient),
                    new KafkaHandler(config, metricsHolder, kafkaAdminClient),
                    metricsHolder,
                    new CruiseControlHandler(config, metricsHolder, cruiseControlClient));

            // create topics
            var t1 = createTopic("t1");
            var t2 = createTopic("t2");
            var t3 = createTopic("t3");
            controller.onUpdate(List.of(
                    TestUtil.reconcilableTopic(t1, NAMESPACE),
                    TestUtil.reconcilableTopic(t2, NAMESPACE),
                    TestUtil.reconcilableTopic(t3, NAMESPACE)
            ));

            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS, "counter", is(3.0));
            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL, "counter", is(3.0));
            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS_DURATION, "timer", greaterThan(0.0));
            assertMetricMatches(metricsHolder, TopicOperatorMetricsHolder.METRICS_DESCRIBE_TOPICS_DURATION, "timer", greaterThan(0.0));
            assertMetricMatches(metricsHolder, TopicOperatorMetricsHolder.METRICS_DESCRIBE_CONFIGS_DURATION, "timer", greaterThan(0.0));
            assertMetricMatches(metricsHolder, TopicOperatorMetricsHolder.METRICS_CREATE_TOPICS_DURATION, "timer", greaterThan(0.0));
            assertMetricMatches(metricsHolder, TopicOperatorMetricsHolder.METRICS_ADD_FINALIZER_DURATION, "timer", greaterThan(0.0));

            // config change
            var t1ConfigChanged = updateTopic(TopicOperatorUtil.topicName(t1), kt -> {
                kt.getSpec().setConfig(Map.of(TopicConfig.RETENTION_MS_CONFIG, "86400000"));
                return kt;
            });
            controller.onUpdate(List.of(TestUtil.reconcilableTopic(t1ConfigChanged, NAMESPACE)));

            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS, "counter", is(4.0));
            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL, "counter", is(4.0));
            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS_DURATION, "timer", greaterThan(0.0));
            assertMetricMatches(metricsHolder, TopicOperatorMetricsHolder.METRICS_DESCRIBE_TOPICS_DURATION, "timer", greaterThan(0.0));
            assertMetricMatches(metricsHolder, TopicOperatorMetricsHolder.METRICS_DESCRIBE_CONFIGS_DURATION, "timer", greaterThan(0.0));
            assertMetricMatches(metricsHolder, TopicOperatorMetricsHolder.METRICS_ALTER_CONFIGS_DURATION, "timer", greaterThan(0.0));
            assertMetricMatches(metricsHolder, TopicOperatorMetricsHolder.METRICS_UPDATE_TOPICS_DURATION, "timer", greaterThan(0.0));

            // increase partitions
            var t2PartIncreased = updateTopic(TopicOperatorUtil.topicName(t2), kt -> {
                kt.getSpec().setPartitions(5);
                return kt;
            });
            controller.onUpdate(List.of(TestUtil.reconcilableTopic(t2PartIncreased, NAMESPACE)));

            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS, "counter", is(5.0));
            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL, "counter", is(5.0));
            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS_DURATION, "timer", greaterThan(0.0));
            assertMetricMatches(metricsHolder, TopicOperatorMetricsHolder.METRICS_DESCRIBE_TOPICS_DURATION, "timer", greaterThan(0.0));
            assertMetricMatches(metricsHolder, TopicOperatorMetricsHolder.METRICS_DESCRIBE_CONFIGS_DURATION, "timer", greaterThan(0.0));
            assertMetricMatches(metricsHolder, TopicOperatorMetricsHolder.METRICS_CREATE_PARTITIONS_DURATION, "timer", greaterThan(0.0));

            // decrease partitions (failure)
            var t2PartDecreased = updateTopic(TopicOperatorUtil.topicName(t2), kt -> {
                kt.getSpec().setPartitions(4);
                return kt;
            });
            controller.onUpdate(List.of(TestUtil.reconcilableTopic(t2PartDecreased, NAMESPACE)));

            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS, "counter", is(6.0));
            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL, "counter", is(5.0));
            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS_FAILED, "counter", is(1.0));
            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS_DURATION, "timer", greaterThan(0.0));
            assertMetricMatches(metricsHolder, TopicOperatorMetricsHolder.METRICS_DESCRIBE_TOPICS_DURATION, "timer", greaterThan(0.0));
            assertMetricMatches(metricsHolder, TopicOperatorMetricsHolder.METRICS_DESCRIBE_CONFIGS_DURATION, "timer", greaterThan(0.0));

            // increase replicas
            // we reconcile two times to trigger requestOngoingChanges operation
            var t3ReplIncreased = updateTopic(TopicOperatorUtil.topicName(t3), kt -> {
                kt.getSpec().setReplicas(2);
                return kt;
            });
            controller.onUpdate(List.of(TestUtil.reconcilableTopic(t3ReplIncreased, NAMESPACE)));
            controller.onUpdate(List.of(TestUtil.reconcilableTopic(t3ReplIncreased, NAMESPACE)));

            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS, "counter", is(8.0));
            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL, "counter", is(7.0));
            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS_FAILED, "counter", is(1.0));
            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS_DURATION, "timer", greaterThan(0.0));
            assertMetricMatches(metricsHolder, TopicOperatorMetricsHolder.METRICS_DESCRIBE_TOPICS_DURATION, "timer", greaterThan(0.0));
            assertMetricMatches(metricsHolder, TopicOperatorMetricsHolder.METRICS_DESCRIBE_CONFIGS_DURATION, "timer", greaterThan(0.0));
            assertMetricMatches(metricsHolder, TopicOperatorMetricsHolder.METRICS_LIST_REASSIGNMENTS_DURATION, "timer", greaterThan(0.0));
            assertMetricMatches(metricsHolder, TopicOperatorMetricsHolder.METRICS_CC_TOPIC_CONFIG_DURATION, "timer", greaterThan(0.0));
            assertMetricMatches(metricsHolder, TopicOperatorMetricsHolder.METRICS_CC_USER_TASKS_DURATION, "timer", greaterThan(0.0));

            // unmanage topic
            var t1Unmanaged = updateTopic(TopicOperatorUtil.topicName(t1), kt -> {
                kt.getMetadata().setAnnotations(Map.of(TopicOperatorUtil.MANAGED, "false"));
                return kt;
            });
            controller.onUpdate(List.of(TestUtil.reconcilableTopic(t1Unmanaged, NAMESPACE)));

            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS, "counter", is(9.0));
            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL, "counter", is(8.0));
            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS_FAILED, "counter", is(1.0));
            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS_DURATION, "timer", greaterThan(0.0));
            assertMetricMatches(metricsHolder, TopicOperatorMetricsHolder.METRICS_DESCRIBE_TOPICS_DURATION, "timer", greaterThan(0.0));
            assertMetricMatches(metricsHolder, TopicOperatorMetricsHolder.METRICS_DESCRIBE_CONFIGS_DURATION, "timer", greaterThan(0.0));

            // delete managed topics
            controller.onDelete(List.of(TestUtil.reconcilableTopic(
                    Crds.topicOperation(kubernetesClient).resource(t2).get(), NAMESPACE)));

            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS, "counter", is(10.0));
            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL, "counter", is(9.0));
            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS_FAILED, "counter", is(1.0));
            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS_DURATION, "timer", greaterThan(0.0));
            assertMetricMatches(metricsHolder, TopicOperatorMetricsHolder.METRICS_DESCRIBE_TOPICS_DURATION, "timer", greaterThan(0.0));
            assertMetricMatches(metricsHolder, TopicOperatorMetricsHolder.METRICS_DESCRIBE_CONFIGS_DURATION, "timer", greaterThan(0.0));
            assertMetricMatches(metricsHolder, TopicOperatorMetricsHolder.METRICS_DELETE_TOPICS_DURATION, "timer", greaterThan(0.0));

            // delete unmanaged topic
            controller.onDelete(List.of(TestUtil.reconcilableTopic(
                    Crds.topicOperation(kubernetesClient).resource(t1).get(), NAMESPACE)));

            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS, "counter", is(11.0));
            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL, "counter", is(10.0));
            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS_FAILED, "counter", is(1.0));
            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS_DURATION, "timer", greaterThan(0.0));
            assertMetricMatches(metricsHolder, TopicOperatorMetricsHolder.METRICS_DESCRIBE_TOPICS_DURATION, "timer", greaterThan(0.0));
            assertMetricMatches(metricsHolder, TopicOperatorMetricsHolder.METRICS_DESCRIBE_CONFIGS_DURATION, "timer", greaterThan(0.0));
            assertMetricMatches(metricsHolder, TopicOperatorMetricsHolder.METRICS_REMOVE_FINALIZER_DURATION, "timer", greaterThan(0.0));

            // pause topic
            var t3Paused = updateTopic(TopicOperatorUtil.topicName(t3), kt -> {
                kt.getMetadata().setAnnotations(Map.of(ResourceAnnotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true"));
                return kt;
            });
            controller.onUpdate(List.of(TestUtil.reconcilableTopic(t3Paused, NAMESPACE)));

            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS, "counter", is(12.0));
            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL, "counter", is(11.0));
            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS_FAILED, "counter", is(1.0));
            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS_DURATION, "timer", greaterThan(0.0));
            assertMetricMatches(metricsHolder, TopicOperatorMetricsHolder.METRICS_DESCRIBE_TOPICS_DURATION, "timer", greaterThan(0.0));
            assertMetricMatches(metricsHolder, TopicOperatorMetricsHolder.METRICS_DESCRIBE_CONFIGS_DURATION, "timer", greaterThan(0.0));

            // delete paused topic
            controller.onDelete(List.of(TestUtil.reconcilableTopic(
                    Crds.topicOperation(kubernetesClient).resource(t3).get(), NAMESPACE)));

            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS, "counter", is(13.0));
            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL, "counter", is(12.0));
            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS_FAILED, "counter", is(1.0));
            assertMetricMatches(metricsHolder, MetricsHolder.METRICS_RECONCILIATIONS_DURATION, "timer", greaterThan(0.0));
            assertMetricMatches(metricsHolder, TopicOperatorMetricsHolder.METRICS_DESCRIBE_TOPICS_DURATION, "timer", greaterThan(0.0));
            assertMetricMatches(metricsHolder, TopicOperatorMetricsHolder.METRICS_DESCRIBE_CONFIGS_DURATION, "timer", greaterThan(0.0));
            assertMetricMatches(metricsHolder, TopicOperatorMetricsHolder.METRICS_REMOVE_FINALIZER_DURATION, "timer", greaterThan(0.0));
        }
    }

    private KafkaTopic buildTopicWithVersion(String name) {
        return new KafkaTopicBuilder()
            .editOrNewMetadata()
                .withNamespace(NAMESPACE)
                .withName(name)
                .withResourceVersion("100100")
            .endMetadata()
            .build();
    }

    private KafkaTopic createTopic(String name) {
        return Crds.topicOperation(kubernetesClient).inNamespace(NAMESPACE).
            resource(new KafkaTopicBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(NAMESPACE)
                    .addToLabels("key", "VALUE")
                .endMetadata()
                .withNewSpec()
                    .withPartitions(2)
                    .withReplicas(1)
                .endSpec()
                .build()).create();
    }

    private KafkaTopic updateTopic(String name, UnaryOperator<KafkaTopic> changer) {
        var kafkaTopic = Crds.topicOperation(kubernetesClient).inNamespace(NAMESPACE).withName(name).get();
        return TestUtil.changeTopic(kubernetesClient, kafkaTopic, changer);
    }

    private void assertMetricMatches(MetricsHolder metricsHolder, String name, String type, Matcher<Double> matcher) throws InterruptedException {
        var found = false;
        var timeoutSec = 30;
        while (!found && --timeoutSec > 0) {
            try {
                LOGGER.info("Searching for metric {}", name);
                var requiredSearch = metricsHolder.metricsProvider().meterRegistry().get(name)
                    .tags("kind", KafkaTopic.RESOURCE_KIND, "namespace", NAMESPACE);
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
                        throw new RuntimeException(String.format("Unknown metric type %s", type));
                }
                found = true;
            } catch (MeterNotFoundException mnfe) {
                LOGGER.info("Metric {} not found", name);
                TimeUnit.SECONDS.sleep(1);
            }
        }
        if (!found) {
            throw new RuntimeException(String.format("Unable to find metric %s", name));
        }
    }
}
