/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicBuilder;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsHolder;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsProvider;
import io.strimzi.operator.topic.model.Pair;
import io.strimzi.operator.topic.model.ReconcilableTopic;
import io.strimzi.operator.topic.model.TopicState;
import io.strimzi.test.TestUtils;
import io.strimzi.test.container.StrimziKafkaCluster;
import io.strimzi.test.interfaces.TestSeparator;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class KafkaHandlerIT implements TestSeparator {
    private static final String NAMESPACE = TopicOperatorTestUtil.namespaceName(KafkaHandlerIT.class);

    private StrimziKafkaCluster kafkaCluster;

    @BeforeEach
    public void beforeEach() {
        kafkaCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withKraft()
                .withNumberOfBrokers(1)
                .withInternalTopicReplicationFactor(1)
                .withAdditionalKafkaConfiguration(Map.of("auto.create.topics.enable", "false"))
                .withSharedNetwork()
                .build();
        kafkaCluster.start();
    }

    @AfterEach
    public void afterEach() {
        kafkaCluster.stop();
    }

    @Test
    public void shouldGetClusterConfig() {
        try (var kafkaAdminClientSpy = spy(Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers())))) {
            var config = TopicOperatorConfig.buildFromMap(Map.of(
                    TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), kafkaCluster.getBootstrapServers(),
                    TopicOperatorConfig.NAMESPACE.key(), NAMESPACE)
            );

            var kafkaHandler = new KafkaHandler(config,
                    new TopicOperatorMetricsHolder(KafkaTopic.RESOURCE_KIND, null, new TopicOperatorMetricsProvider(new SimpleMeterRegistry())),
                    kafkaAdminClientSpy);
            var autoCreateValue = kafkaHandler.clusterConfig(KafkaHandler.AUTO_CREATE_TOPICS_ENABLE);

            verify(kafkaAdminClientSpy, times(1)).describeCluster(any());
            verify(kafkaAdminClientSpy, times(1)).describeConfigs(any());

            assertThat(autoCreateValue.isPresent(), is(true));
            assertThat(autoCreateValue.get(), is("false"));
        }
    }

    @Test
    public void shouldCreateTopics() {
        try (var kafkaAdminClientSpy = spy(Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers())))) {
            var config = TopicOperatorConfig.buildFromMap(Map.of(
                    TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), kafkaCluster.getBootstrapServers(),
                    TopicOperatorConfig.NAMESPACE.key(), NAMESPACE)
            );

            var kafkaHandler = new KafkaHandler(config,
                    new TopicOperatorMetricsHolder(KafkaTopic.RESOURCE_KIND, null, new TopicOperatorMetricsProvider(new SimpleMeterRegistry())),
                    kafkaAdminClientSpy);
            var reconcilableTopics = List.of(
                    TopicOperatorTestUtil.reconcilableTopic(buildTopic("t1", 1, 1), NAMESPACE),
                    TopicOperatorTestUtil.reconcilableTopic(buildTopic("t2", 1, 1), NAMESPACE)
            );
            var result = kafkaHandler.createTopics(reconcilableTopics);

            verify(kafkaAdminClientSpy, times(1)).createTopics(any());

            var resultTopicNames = result.ok()
                    .map(pair -> pair.getKey().kt().getMetadata().getName())
                    .collect(Collectors.toSet());
            assertThat(resultTopicNames, is(Set.of("t1", "t2")));
        }
    }

    @Test
    public void shouldFilterByReassignmentTargetReplicas() {
        try (var kafkaAdminClientSpy = spy(Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers())))) {
            var config = TopicOperatorConfig.buildFromMap(Map.of(
                    TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), kafkaCluster.getBootstrapServers(),
                    TopicOperatorConfig.NAMESPACE.key(), NAMESPACE)
            );

            var kafkaHandler = new KafkaHandler(config,
                    new TopicOperatorMetricsHolder(KafkaTopic.RESOURCE_KIND, null, new TopicOperatorMetricsProvider(new SimpleMeterRegistry())),
                    kafkaAdminClientSpy);

            // current RF = 2
            var topicName = "my-topic";
            var topicPartition0 = mock(TopicPartitionInfo.class);
            doReturn(0).when(topicPartition0).partition();
            doReturn(List.of(new Node(0, null, 0), new Node(1, null, 0))).when(topicPartition0).replicas();

            // desired RF = 1
            List<Pair<ReconcilableTopic, TopicState>> pairs = List.of(
                    new Pair(TopicOperatorTestUtil.reconcilableTopic(buildTopic(topicName, 1, 1), NAMESPACE),
                            new TopicState(new TopicDescription(topicName, false, List.of(topicPartition0)), null))
            );
            kafkaHandler.filterByReassignmentTargetReplicas(pairs);

            verify(kafkaAdminClientSpy, times(1)).listPartitionReassignments(anySet());
        }
    }

    @Test
    public void shouldAlterConfigs() throws ExecutionException, InterruptedException {
        String t1Name = "should-alter-configs-1";
        createKafkaTopic(t1Name, Map.of(TopicConfig.RETENTION_MS_CONFIG, "604800000"));
        String t2Name = "should-alter-configs-2";
        createKafkaTopic(t2Name, Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, "delete"));

        try (var kafkaAdminClientSpy = spy(Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers())))) {
            var config = TopicOperatorConfig.buildFromMap(Map.of(
                    TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), kafkaCluster.getBootstrapServers(),
                    TopicOperatorConfig.NAMESPACE.key(), NAMESPACE)
            );

            var kafkaHandler = new KafkaHandler(config,
                    new TopicOperatorMetricsHolder(KafkaTopic.RESOURCE_KIND, null, new TopicOperatorMetricsProvider(new SimpleMeterRegistry())),
                    kafkaAdminClientSpy);

            List<Pair<ReconcilableTopic, Collection<AlterConfigOp>>> pairs = List.of(
                    new Pair(TopicOperatorTestUtil.reconcilableTopic(buildTopic(t1Name, 1, 1), NAMESPACE),
                            List.of(new AlterConfigOp(new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "86400000"), AlterConfigOp.OpType.SET))),
                    new Pair(TopicOperatorTestUtil.reconcilableTopic(buildTopic(t2Name, 1, 1), NAMESPACE),
                            List.of(new AlterConfigOp(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, "compact"), AlterConfigOp.OpType.SET)))
            );
            var result = kafkaHandler.alterConfigs(pairs);

            verify(kafkaAdminClientSpy, times(1)).incrementalAlterConfigs(any());

            var resultTopicNames = result.ok()
                    .map(pair -> pair.getKey().kt().getSpec().getTopicName())
                    .collect(Collectors.toSet());
            assertThat(resultTopicNames, is(Set.of(t1Name, t2Name)));
        }
    }

    @Test
    public void shouldCreatePartitions() throws ExecutionException, InterruptedException {
        String t1Name = "should-create-partitions-1";
        createKafkaTopic(t1Name, Map.of());
        String t2Name = "should-create-partitions-2";
        createKafkaTopic(t2Name, Map.of());

        try (var kafkaAdminClientSpy = spy(Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers())))) {
            var config = TopicOperatorConfig.buildFromMap(Map.of(
                    TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), kafkaCluster.getBootstrapServers(),
                    TopicOperatorConfig.NAMESPACE.key(), NAMESPACE)
            );

            var kafkaHandler = new KafkaHandler(config,
                    new TopicOperatorMetricsHolder(KafkaTopic.RESOURCE_KIND, null, new TopicOperatorMetricsProvider(new SimpleMeterRegistry())),
                    kafkaAdminClientSpy);
            List<Pair<ReconcilableTopic, NewPartitions>> pairs = List.of(
                    new Pair(TopicOperatorTestUtil.reconcilableTopic(buildTopic(t1Name, 1, 1), NAMESPACE), NewPartitions.increaseTo(2)),
                    new Pair(TopicOperatorTestUtil.reconcilableTopic(buildTopic(t2Name, 1, 1), NAMESPACE), NewPartitions.increaseTo(2))
            );
            var result = kafkaHandler.createPartitions(pairs);

            verify(kafkaAdminClientSpy, times(1)).createPartitions(any());

            var resultTopicNames = result.ok()
                    .map(pair -> pair.getKey().kt().getSpec().getTopicName())
                    .collect(Collectors.toSet());
            assertThat(resultTopicNames, is(Set.of(t1Name, t2Name)));
        }
    }

    @Test
    public void shouldDescribeTopics() throws ExecutionException, InterruptedException {
        String t1Name = "should-describe-topics-1";
        createKafkaTopic(t1Name, Map.of());
        String t2Name = "should-describe-topics-2";
        createKafkaTopic(t2Name, Map.of());

        // Wait until both topics actually exist
        try (var admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers()))) {
            TestUtils.waitFor("Wait for topic creation", Duration.ofMillis(500).toMillis(), Duration.ofSeconds(30).toMillis(), () -> {
                try {
                    var existingTopics = admin.listTopics().names().get();
                    return existingTopics.containsAll(List.of(t1Name, t2Name));
                } catch (Exception e) {
                    return false;
                }
            });
        }

        try (var kafkaAdminClientSpy = spy(Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers())))) {
            var config = TopicOperatorConfig.buildFromMap(Map.of(
                    TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), kafkaCluster.getBootstrapServers(),
                    TopicOperatorConfig.NAMESPACE.key(), NAMESPACE)
            );

            var kafkaHandler = new KafkaHandler(config,
                    new TopicOperatorMetricsHolder(KafkaTopic.RESOURCE_KIND, null, new TopicOperatorMetricsProvider(new SimpleMeterRegistry())),
                    kafkaAdminClientSpy);
            var reconcilableTopics = List.of(
                    TopicOperatorTestUtil.reconcilableTopic(buildTopic(t1Name, 1, 1), NAMESPACE),
                    TopicOperatorTestUtil.reconcilableTopic(buildTopic(t2Name, 1, 1), NAMESPACE)
            );
            var result = kafkaHandler.describeTopics(reconcilableTopics);

            verify(kafkaAdminClientSpy, times(1)).describeTopics(anyCollection());
            verify(kafkaAdminClientSpy, times(1)).describeConfigs(any());

            var t1State = result.ok()
                    .filter(pair -> Objects.equals(pair.getKey().kt().getSpec().getTopicName(), t1Name))
                    .map(pair -> pair.getValue()).findFirst();
            assertThat(t1State.get().description().name(), is(t1Name));
            var t2State = result.ok()
                    .filter(pair -> Objects.equals(pair.getKey().kt().getSpec().getTopicName(), t2Name))
                    .map(pair -> pair.getValue()).findFirst();
            assertThat(t2State.get().description().name(), is(t2Name));
        }
    }

    @Test
    public void shouldDeleteTopics() throws ExecutionException, InterruptedException {
        String t1Name = "should-delete-topics-1";
        createKafkaTopic(t1Name, Map.of());
        String t2Name = "should-delete-topics-2";
        createKafkaTopic(t2Name, Map.of());
        String t3Name = "should-delete-topics-3";
        createKafkaTopic(t3Name, Map.of());

        try (var kafkaAdminClientSpy = spy(Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers())))) {
            var config = TopicOperatorConfig.buildFromMap(Map.of(
                    TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), kafkaCluster.getBootstrapServers(),
                    TopicOperatorConfig.NAMESPACE.key(), NAMESPACE)
            );

            var kafkaHandler = new KafkaHandler(config,
                    new TopicOperatorMetricsHolder(KafkaTopic.RESOURCE_KIND, null, new TopicOperatorMetricsProvider(new SimpleMeterRegistry())),
                    kafkaAdminClientSpy);
            var reconcilableTopics = List.of(
                    TopicOperatorTestUtil.reconcilableTopic(buildTopic(t1Name, 1, 1), NAMESPACE),
                    TopicOperatorTestUtil.reconcilableTopic(buildTopic(t2Name, 1, 1), NAMESPACE),
                    TopicOperatorTestUtil.reconcilableTopic(buildTopic(t3Name, 1, 1), NAMESPACE)
            );
            var topicNamesToDelete = reconcilableTopics.stream().map(ReconcilableTopic::topicName).collect(Collectors.toSet());
            topicNamesToDelete.removeIf(name -> Objects.equals(name, t3Name));
            var result = kafkaHandler.deleteTopics(reconcilableTopics, topicNamesToDelete);

            verify(kafkaAdminClientSpy, times(1)).deleteTopics(any(TopicCollection.TopicNameCollection.class));

            var resultTopicNames = result.ok()
                    .map(pair -> pair.getKey().kt().getSpec().getTopicName())
                    .collect(Collectors.toSet());
            assertThat(resultTopicNames, is(topicNamesToDelete));
        }
    }

    private void createKafkaTopic(String name, Map<String, String> config) throws ExecutionException, InterruptedException {
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers()))) {

            NewTopic topic = new NewTopic(name, 1, (short) 1);
            topic.configs(config);

            admin.createTopics(List.of(topic)).all().get();
        }
    }

    private KafkaTopic buildTopic(String name, int partitions, int replicas) {
        return new KafkaTopicBuilder()
            .withNewMetadata()
                .withName(name.replaceAll("_", "-"))
                .withNamespace(NAMESPACE)
                .addToLabels("key", "VALUE")
            .endMetadata()
            .withNewSpec()
                .withTopicName(name)
                .withPartitions(partitions)
                .withReplicas(replicas)
            .endSpec()
            .build();
    }
}
