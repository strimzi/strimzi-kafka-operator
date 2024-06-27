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
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicBuilder;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.metrics.MetricsHolder;
import io.strimzi.operator.topic.cruisecontrol.CruiseControlClient;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsHolder;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsProvider;
import io.strimzi.operator.topic.model.ReconcilableTopic;
import io.strimzi.operator.topic.model.TopicEvent.TopicUpsert;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

import static io.strimzi.api.ResourceAnnotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION;
import static io.strimzi.api.kafka.model.topic.KafkaTopic.RESOURCE_KIND;
import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;

@ExtendWith(KafkaClusterExtension.class)
public class TopicOperatorMetricsTest {
    private static final Logger LOGGER = LogManager.getLogger(TopicOperatorMetricsTest.class);
    private static final String NAMESPACE = "topic-operator-test";
    private static final int MAX_QUEUE_SIZE = 200;
    private static final int MAX_BATCH_SIZE = 10;
    private static final int MAX_THREADS = 2;
    private static final long MAX_BATCH_LINGER_MS = 10_000;

    private static KubernetesClient kubeClient;
    private static TopicOperatorMetricsHolder metricsHolder;

    @BeforeAll
    public static void beforeAll(TestInfo testInfo) {
        TopicOperatorTestUtil.setupKubeCluster(testInfo, NAMESPACE);
        kubeClient = new KubernetesClientBuilder().build();
        metricsHolder = new TopicOperatorMetricsHolder(RESOURCE_KIND, null, 
            new TopicOperatorMetricsProvider(new SimpleMeterRegistry()));
    }

    @AfterAll
    public static void afterAll(TestInfo testInfo) {
        TopicOperatorTestUtil.cleanupNamespace(kubeClient, testInfo, NAMESPACE);
        TopicOperatorTestUtil.teardownKubeCluster(NAMESPACE);
        kubeClient.close();
    }

    @Test
    public void eventHandlerMetrics() throws InterruptedException {
        var config = TopicOperatorConfig.buildFromMap(Map.of(
            TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:9092",
            TopicOperatorConfig.NAMESPACE.key(), NAMESPACE));
        BatchingLoop mockQueue = mock(BatchingLoop.class);
        TopicOperatorEventHandler eventHandler = new TopicOperatorEventHandler(config, mockQueue, metricsHolder);
        
        int numOfTestResources = 100;
        for (int i = 0; i < numOfTestResources; i++) {
            KafkaTopic kt = buildTopicWithVersion("t" + i);
            eventHandler.onAdd(kt);
        }
        assertMetricMatches(MetricsHolder.METRICS_RESOURCES, "gauge", is(Double.valueOf(numOfTestResources)));

        for (int i = 0; i < numOfTestResources; i++) {
            KafkaTopic kt = buildTopicWithVersion("t" + i);
            eventHandler.onDelete(kt, false);
        }
        assertMetricMatches(MetricsHolder.METRICS_RESOURCES, "gauge", is(0.0));

        KafkaTopic t1 = buildTopicWithVersion("t1");
        KafkaTopic t2 = buildTopicWithVersion("t2");
        t2.getMetadata().setAnnotations(Map.of(ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true"));
        eventHandler.onUpdate(t1, t2);
        assertMetricMatches(MetricsHolder.METRICS_RESOURCES_PAUSED, "gauge", is(1.0));

        KafkaTopic t3 = buildTopicWithVersion("t3");
        t3.getMetadata().setAnnotations(Map.of(ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "false"));
        eventHandler.onUpdate(t2, t3);
        assertMetricMatches(MetricsHolder.METRICS_RESOURCES_PAUSED, "gauge", is(0.0));
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

    @Test
    public void batchingLoopMetrics() throws InterruptedException {
        BatchingLoop batchingLoop = createAndStartBatchingLoop();
        int numOfTestResources = 100;
        for (int i = 0; i < numOfTestResources; i++) {
            if (i < numOfTestResources / 2) {
                batchingLoop.offer(new TopicUpsert(0, NAMESPACE, "t0", "10010" + i));
            } else {
                batchingLoop.offer(new TopicUpsert(0, NAMESPACE, "t" + i, "100100"));
            }
        }
        
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_MAX_QUEUE_SIZE, "gauge", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_MAX_QUEUE_SIZE, "gauge", lessThanOrEqualTo(Double.valueOf(MAX_QUEUE_SIZE)));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_MAX_BATCH_SIZE,  "gauge", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_MAX_BATCH_SIZE, "gauge", lessThanOrEqualTo(Double.valueOf(MAX_BATCH_SIZE)));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_LOCKED, "counter", greaterThan(0.0));
        batchingLoop.stop();
    }
    
    private BatchingLoop createAndStartBatchingLoop() {
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
            metricsHolder,
            NAMESPACE);
        batchingLoop.start();
        return batchingLoop;
    }
    
    @Test
    public void batchingTopicControllerMetrics(KafkaCluster cluster) throws InterruptedException {
        var admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        var config = Mockito.mock(TopicOperatorConfig.class);
        Mockito.doReturn(NAMESPACE).when(config).namespace();
        Mockito.doReturn(true).when(config).useFinalizer();
        Mockito.doReturn(true).when(config).enableAdditionalMetrics();
        Mockito.doReturn(true).when(config).cruiseControlEnabled();
        
        var cruiseControlClient = Mockito.mock(CruiseControlClient.class);
        var userTaskId = "8911ca89-351f-888-8d0f-9aade00e098h";
        Mockito.doReturn(userTaskId).when(cruiseControlClient).topicConfiguration(anyList());
        var userTaskResponse = new CruiseControlClient.UserTasksResponse(List.of(new CruiseControlClient.UserTask("Active", null, null, userTaskId, System.currentTimeMillis())), 1);
        Mockito.doReturn(userTaskResponse).when(cruiseControlClient).userTasks(Set.of(userTaskId));
        
        var replicasChangeHandler = new ReplicasChangeHandler(config, metricsHolder, cruiseControlClient);
        var controller = new BatchingTopicController(config, Map.of("key", "VALUE"), admin, kubeClient, metricsHolder, replicasChangeHandler);

        // create topics, 3 reconciliations, success
        var t1 = createTopic("t1", 2, 1);
        var t2 = createTopic("t2", 2, 1);
        var t3 = createTopic("t3", 2, 1);
        controller.onUpdate(List.of(reconcilableTopic(t1), reconcilableTopic(t2), reconcilableTopic(t3)));
        
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS, "counter", is(3.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL, "counter", is(3.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_DURATION, "timer", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_DESCRIBE_TOPICS_DURATION, "timer", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_DESCRIBE_CONFIGS_DURATION, "timer", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_CREATE_TOPICS_DURATION, "timer", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_ADD_FINALIZER_DURATION, "timer", greaterThan(0.0));
        
        // config change, 1 reconciliation, success
        var t1ConfigChanged = updateTopic("t1", kt -> {
            kt.getSpec().setConfig(Map.of("retention.ms", "86400000"));
            return kt;
        });
        controller.onUpdate(List.of(reconcilableTopic(t1ConfigChanged)));
        
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS, "counter", is(4.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL, "counter", is(4.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_DURATION, "timer", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_DESCRIBE_TOPICS_DURATION, "timer", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_DESCRIBE_CONFIGS_DURATION, "timer", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_ALTER_CONFIGS_DURATION, "timer", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_UPDATE_TOPICS_DURATION, "timer", greaterThan(0.0));
        
        // increase partitions, 1 reconciliation, success
        var t2PartIncreased = updateTopic("t2", kt -> {
            kt.getSpec().setPartitions(5);
            return kt;
        });
        controller.onUpdate(List.of(reconcilableTopic(t2PartIncreased)));

        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS, "counter", is(5.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL, "counter", is(5.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_DURATION, "timer", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_DESCRIBE_TOPICS_DURATION, "timer", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_DESCRIBE_CONFIGS_DURATION, "timer", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_CREATE_PARTITIONS_DURATION, "timer", greaterThan(0.0));

        // decrease partitions, 1 reconciliation, fail
        var t2PartDecreased = updateTopic("t2", kt -> {
            kt.getSpec().setPartitions(4);
            return kt;
        });
        controller.onUpdate(List.of(reconcilableTopic(t2PartDecreased)));

        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS, "counter", is(6.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL, "counter", is(5.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_FAILED, "counter", is(1.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_DURATION, "timer", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_DESCRIBE_TOPICS_DURATION, "timer", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_DESCRIBE_CONFIGS_DURATION, "timer", greaterThan(0.0));

        // increase replicas, 1 reconciliation, success
        var t3ReplIncreased = updateTopic("t3", kt -> {
            kt.getSpec().setReplicas(2);
            return kt;
        });
        controller.onUpdate(List.of(reconcilableTopic(t3ReplIncreased)));

        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS, "counter", is(7.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL, "counter", is(6.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_FAILED, "counter", is(1.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_DURATION, "timer", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_DESCRIBE_TOPICS_DURATION, "timer", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_DESCRIBE_CONFIGS_DURATION, "timer", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_LIST_REASSIGNMENTS_DURATION, "timer", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_CC_TOPIC_CONFIG_DURATION, "timer", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_CC_USER_TASKS_DURATION, "timer", greaterThan(0.0));

        // unmanage topic, 1 reconciliation, success
        var t1Unmanaged = updateTopic("t1", kt -> {
            kt.getMetadata().setAnnotations(Map.of(TopicOperatorUtil.MANAGED, "false"));
            return kt;
        });
        controller.onUpdate(List.of(reconcilableTopic(t1Unmanaged)));

        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS, "counter", is(8.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL, "counter", is(7.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_FAILED, "counter", is(1.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_DURATION, "timer", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_DESCRIBE_TOPICS_DURATION, "timer", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_DESCRIBE_CONFIGS_DURATION, "timer", greaterThan(0.0));
        
        // delete managed topics, 1 reconciliation, success
        controller.onDelete(List.of(reconcilableTopic(t2)));

        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS, "counter", is(9.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL, "counter", is(8.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_FAILED, "counter", is(1.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_DURATION, "timer", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_DESCRIBE_TOPICS_DURATION, "timer", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_DESCRIBE_CONFIGS_DURATION, "timer", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_DELETE_TOPICS_DURATION, "timer", greaterThan(0.0));

        // delete unmanaged topic, 1 reconciliation, success
        var t3Unmanaged = updateTopic("t3", kt -> {
            kt.getMetadata().setAnnotations(Map.of(TopicOperatorUtil.MANAGED, "false"));
            return kt;
        });
        controller.onDelete(List.of(reconcilableTopic(t3Unmanaged)));

        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS, "counter", is(10.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_SUCCESSFUL, "counter", is(9.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_FAILED, "counter", is(1.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_RECONCILIATIONS_DURATION, "timer", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_DESCRIBE_TOPICS_DURATION, "timer", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_DESCRIBE_CONFIGS_DURATION, "timer", greaterThan(0.0));
        assertMetricMatches(TopicOperatorMetricsHolder.METRICS_REMOVE_FINALIZER_DURATION, "timer", greaterThan(0.0));
    }
    
    private KafkaTopic createTopic(String name, int partitions, int replicas) {
        return Crds.topicOperation(kubeClient).inNamespace(NAMESPACE).
            resource(new KafkaTopicBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(NAMESPACE)
                    .addToLabels("key", "VALUE")
                .endMetadata()
                .withNewSpec()
                    .withPartitions(partitions)
                    .withReplicas(replicas)
                .endSpec()
                .build()).create();
    }
    
    private KafkaTopic updateTopic(String name, UnaryOperator<KafkaTopic> changer) {
        var kt = Crds.topicOperation(kubeClient).inNamespace(NAMESPACE).withName(name).get();
        return TopicOperatorTestUtil.modifyTopic(kubeClient, kt, changer);
    }
    
    private ReconcilableTopic reconcilableTopic(KafkaTopic kafkaTopic) {
        return new ReconcilableTopic(new Reconciliation("test", RESOURCE_KIND, NAMESPACE,
            TopicOperatorUtil.topicName(kafkaTopic)), kafkaTopic, TopicOperatorUtil.topicName(kafkaTopic));
    }
    
    private void assertMetricMatches(String name, String type, Matcher<Double> matcher) throws InterruptedException {
        var found = false;
        var timeoutSec = 30;
        while (!found && --timeoutSec > 0) {
            try {
                LOGGER.info("Searching for metric {}", name);
                var requiredSearch = metricsHolder.metricsProvider().meterRegistry().get(name)
                    .tags("kind", RESOURCE_KIND, "namespace", NAMESPACE);
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
                found = true;
            } catch (MeterNotFoundException mnfe) {
                LOGGER.info("Metric {} not found", name);
                TimeUnit.SECONDS.sleep(1);
            }
        }
        if (!found) {
            throw new RuntimeException(format("Unable to find metric %s", name));
        }
    }
}
