/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.common.ConditionBuilder;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicBuilder;
import io.strimzi.api.kafka.model.topic.KafkaTopicStatusBuilder;
import io.strimzi.operator.common.model.StatusUtils;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsHolder;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsProvider;
import io.strimzi.test.mockkube3.MockKube3;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.strimzi.api.kafka.model.topic.KafkaTopic.RESOURCE_KIND;
import static io.strimzi.operator.topic.KubernetesHandler.FINALIZER_STRIMZI_IO_TO;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KubernetesHandlerTest {
    private static final String NAMESPACE = TopicOperatorTestUtil.namespaceName(KubernetesHandlerTest.class);

    private static MockKube3 mockKube;
    private static KubernetesClient kubernetesClient;
    private KubernetesHandler kubernetesHandler;

    @BeforeAll
    public static void beforeAll() {
        mockKube = new MockKube3.MockKube3Builder()
            .withKafkaTopicCrd()
            .withDeletionController()
            .withNamespaces(NAMESPACE)
            .build();
        mockKube.start();
        kubernetesClient = mockKube.client();
    }

    @AfterAll
    public static void afterAll() {
        mockKube.stop();
    }

    @BeforeEach
    public void beforeEach() {
        var config = TopicOperatorConfig.buildFromMap(Map.of(
            TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:9092",
            TopicOperatorConfig.NAMESPACE.key(), NAMESPACE
        ));
        var metricsHolder = new TopicOperatorMetricsHolder(RESOURCE_KIND, null, 
            new TopicOperatorMetricsProvider(new SimpleMeterRegistry()));
        kubernetesHandler = new KubernetesHandler(config, metricsHolder, kubernetesClient);
    }

    @AfterEach
    public void afterEach() {
        TopicOperatorTestUtil.cleanupNamespace(kubernetesClient, NAMESPACE);
    }

    @Test
    public void addFinalizerShouldWork() {
        var kafkaTopic = createTopic("my-topic", false);
        assertTrue(kafkaTopic.getMetadata().getFinalizers().isEmpty());

        var update = kubernetesHandler.addFinalizer(TopicOperatorTestUtil.reconcilableTopic(kafkaTopic, NAMESPACE));
        assertThat(update.getMetadata().getFinalizers().get(0), is(FINALIZER_STRIMZI_IO_TO));
    }

    @Test
    public void addFinalizerShouldBeIdempotent() {
        var kafkaTopic = createTopic("my-topic", false);
        assertTrue(kafkaTopic.getMetadata().getFinalizers().isEmpty());

        var update1 = kubernetesHandler.addFinalizer(TopicOperatorTestUtil.reconcilableTopic(kafkaTopic, NAMESPACE));
        var update2 = kubernetesHandler.addFinalizer(TopicOperatorTestUtil.reconcilableTopic(update1, NAMESPACE));
        assertThat(update2.getMetadata().getFinalizers().size(), is(1));
    }

    @Test
    public void removeFinalizerShouldWork() {
        var kafkaTopic = createTopic("my-topic", true);
        assertThat(kafkaTopic.getMetadata().getFinalizers().get(0), is(FINALIZER_STRIMZI_IO_TO));

        var update = kubernetesHandler.removeFinalizer(TopicOperatorTestUtil.reconcilableTopic(kafkaTopic, NAMESPACE));
        assertTrue(update.getMetadata().getFinalizers().isEmpty());
    }

    @Test
    public void shouldNotUpdateStatusWithNoChanges() {
        var kafkaTopic = createTopicWithReadyState("my-topic", Map.of(TopicConfig.RETENTION_MS_CONFIG, "604800000"));

        var update1 = kubernetesHandler.updateStatus(TopicOperatorTestUtil.reconcilableTopic(kafkaTopic, NAMESPACE));
        assertThat(update1.getStatus().getObservedGeneration(), is(1L));

        var change = TopicOperatorTestUtil.changeTopic(kubernetesClient, update1, kt -> kt);

        var update = kubernetesHandler.updateStatus(TopicOperatorTestUtil.reconcilableTopic(change, NAMESPACE));
        assertThat(update.getStatus().getObservedGeneration(), is(1L));
    }

    @Test
    public void shouldUpdateStatusWithConfigChange() {
        var kafkaTopic = createTopicWithReadyState("my-topic", Map.of(TopicConfig.RETENTION_MS_CONFIG, "604800000"));

        var update1 = kubernetesHandler.updateStatus(TopicOperatorTestUtil.reconcilableTopic(kafkaTopic, NAMESPACE));
        assertThat(update1.getStatus().getObservedGeneration(), is(1L));

        var change = TopicOperatorTestUtil.changeTopic(kubernetesClient, update1, kt -> {
            kt.getSpec().setConfig(Map.of(TopicConfig.RETENTION_MS_CONFIG, "86400000"));
            return kt;
        });

        var update2 = kubernetesHandler.updateStatus(TopicOperatorTestUtil.reconcilableTopic(change, NAMESPACE));
        assertThat(update2.getStatus().getObservedGeneration(), is(2L));
    }

    @Test
    public void shouldUpdateStatusWithPartitionChange() {
        var kafkaTopic = createTopicWithReadyState("my-topic", Map.of(TopicConfig.RETENTION_MS_CONFIG, "604800000"));

        var update1 = kubernetesHandler.updateStatus(TopicOperatorTestUtil.reconcilableTopic(kafkaTopic, NAMESPACE));
        assertThat(update1.getStatus().getObservedGeneration(), is(1L));

        var change = TopicOperatorTestUtil.changeTopic(kubernetesClient, update1, kt -> {
            kt.getSpec().setPartitions(3);
            return kt;
        });

        var update2 = kubernetesHandler.updateStatus(TopicOperatorTestUtil.reconcilableTopic(change, NAMESPACE));
        assertThat(update2.getStatus().getObservedGeneration(), is(2L));
    }

    @Test
    public void shouldUpdateStatusWithReplicasChange() {
        var kafkaTopic = createTopicWithReadyState("my-topic", Map.of(TopicConfig.RETENTION_MS_CONFIG, "604800000"));

        var update1 = kubernetesHandler.updateStatus(TopicOperatorTestUtil.reconcilableTopic(kafkaTopic, NAMESPACE));
        assertThat(update1.getStatus().getObservedGeneration(), is(1L));

        var change = TopicOperatorTestUtil.changeTopic(kubernetesClient, update1, kt -> {
            kt.getSpec().setReplicas(2);
            return kt;
        });

        var update2 = kubernetesHandler.updateStatus(TopicOperatorTestUtil.reconcilableTopic(change, NAMESPACE));
        assertThat(update2.getStatus().getObservedGeneration(), is(2L));
    }

    private KafkaTopic createTopic(String name, boolean withFinalizer) {
        var kafkaTopic = new KafkaTopicBuilder()
            .withNewMetadata()
                .withName(name)
                .withNamespace(NAMESPACE)
                .addToLabels("key", "VALUE")
            .endMetadata()
            .withNewSpec()
                .withConfig(Map.of(
                    TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy",
                    TopicConfig.CLEANUP_POLICY_CONFIG, "compact"))
                .withPartitions(2)
                .withReplicas(1)
            .endSpec()
            .build();
        if (withFinalizer) {
            kafkaTopic.getMetadata().getFinalizers().add(FINALIZER_STRIMZI_IO_TO);
        }
        return Crds.topicOperation(kubernetesClient).resource(kafkaTopic).create();
    }

    private static KafkaTopic createTopicWithReadyState(String name, Map<String, Object> config) {
        var kafkaTopic = new KafkaTopicBuilder()
            .withNewMetadata()
                .withGeneration(1L)
                .withName(name)
                .withNamespace(NAMESPACE)
                .addToLabels("key", "VALUE")
            .endMetadata()
            .withNewSpec()
                .withConfig(config)
                .withPartitions(2)
                .withReplicas(1)
            .endSpec()
            .withStatus(new KafkaTopicStatusBuilder()
                .withObservedGeneration(1L)
                .withTopicName(name)
                .withTopicId("WyhsoDQRSXqa8k2myxQrrA")
                .withConditions(List.of(new ConditionBuilder()
                    .withType("Ready")
                    .withStatus("True")
                    .withLastTransitionTime(StatusUtils.iso8601Now())
                    .build()))
                .build())
            .build();
        Crds.topicOperation(kubernetesClient).resource(kafkaTopic).create();
        return Crds.topicOperation(kubernetesClient).resource(kafkaTopic).updateStatus();
    }
}
