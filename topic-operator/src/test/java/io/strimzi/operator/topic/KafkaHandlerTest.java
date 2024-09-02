/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.common.BrokerConfig;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;
import io.kroxylicious.testing.kafka.junit5ext.TopicPartitions;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicBuilder;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsHolder;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsProvider;
import io.strimzi.operator.topic.model.Pair;
import io.strimzi.operator.topic.model.ReconcilableTopic;
import io.strimzi.operator.topic.model.TopicState;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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

@ExtendWith(KafkaClusterExtension.class)
public class KafkaHandlerTest {
    private static final String NAMESPACE = TopicOperatorTestUtil.namespaceName(KafkaHandlerTest.class);

    @Test
    public void shouldGetClusterConfig(
            @BrokerConfig(name = "auto.create.topics.enable", value = "false")
            KafkaCluster cluster) {
        var config = TopicOperatorConfig.buildFromMap(Map.of(
            TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), cluster.getBootstrapServers(),
            TopicOperatorConfig.NAMESPACE.key(), NAMESPACE)
        );
        var kafkaAdminClientSpy = spy(Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers())));

        var kafkaHandler = new KafkaHandler(config, 
            new TopicOperatorMetricsHolder(KafkaTopic.RESOURCE_KIND, null, new TopicOperatorMetricsProvider(new SimpleMeterRegistry())), 
            kafkaAdminClientSpy);
        var autoCreateValue = kafkaHandler.clusterConfig(KafkaHandler.AUTO_CREATE_TOPICS_ENABLE);

        verify(kafkaAdminClientSpy, times(1)).describeCluster(any());
        verify(kafkaAdminClientSpy, times(1)).describeConfigs(any());
        
        assertThat(autoCreateValue.isPresent(), is(true));
        assertThat(autoCreateValue.get(), is("false"));
    }

    @Test
    public void shouldCreateTopics(KafkaCluster cluster) {
        var config = TopicOperatorConfig.buildFromMap(Map.of(
            TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), cluster.getBootstrapServers(),
            TopicOperatorConfig.NAMESPACE.key(), NAMESPACE)
        );
        var kafkaAdminClientSpy = spy(Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers())));

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

    @Test
    public void shouldFilterByReassignmentTargetReplicas(@BrokerCluster(numBrokers = 2) KafkaCluster cluster) {
        var config = TopicOperatorConfig.buildFromMap(Map.of(
            TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), cluster.getBootstrapServers(),
            TopicOperatorConfig.NAMESPACE.key(), NAMESPACE)
        );
        var kafkaAdminClientSpy = spy(Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers())));

        var kafkaHandler = new KafkaHandler(config,
            new TopicOperatorMetricsHolder(KafkaTopic.RESOURCE_KIND, null, new TopicOperatorMetricsProvider(new SimpleMeterRegistry())),
            kafkaAdminClientSpy);

        // current RF = 2
        var topicName = "my-topic";
        var topicPartition0 = mock(TopicPartitionInfo.class);
        doReturn(0).when(topicPartition0).partition();
        doReturn(parseNodes(cluster.getBootstrapServers())).when(topicPartition0).replicas();

        // desired RF = 1
        List<Pair<ReconcilableTopic, TopicState>> pairs = List.of(
            new Pair(TopicOperatorTestUtil.reconcilableTopic(buildTopic(topicName, 1, 1), NAMESPACE),
                new TopicState(new TopicDescription(topicName, false, List.of(topicPartition0)), null))
        );
        kafkaHandler.filterByReassignmentTargetReplicas(pairs);
            
        verify(kafkaAdminClientSpy, times(1)).listPartitionReassignments(anySet());
    }

    @Test
    public void shouldAlterConfigs(KafkaCluster cluster,
                                   @io.kroxylicious.testing.kafka.junit5ext.TopicConfig(name = TopicConfig.RETENTION_MS_CONFIG, value = "604800000") Topic t1,
                                   @io.kroxylicious.testing.kafka.junit5ext.TopicConfig(name = TopicConfig.CLEANUP_POLICY_CONFIG, value = "delete") Topic t2) {
        var config = TopicOperatorConfig.buildFromMap(Map.of(
            TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), cluster.getBootstrapServers(),
            TopicOperatorConfig.NAMESPACE.key(), NAMESPACE)
        );
        var kafkaAdminClientSpy = spy(Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers())));

        var kafkaHandler = new KafkaHandler(config,
            new TopicOperatorMetricsHolder(KafkaTopic.RESOURCE_KIND, null, new TopicOperatorMetricsProvider(new SimpleMeterRegistry())),
            kafkaAdminClientSpy);
        
        List<Pair<ReconcilableTopic, Collection<AlterConfigOp>>> pairs = List.of(
            new Pair(TopicOperatorTestUtil.reconcilableTopic(buildTopic(t1.name(), 1, 1), NAMESPACE), 
                List.of(new AlterConfigOp(new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "86400000"), AlterConfigOp.OpType.SET))),
            new Pair(TopicOperatorTestUtil.reconcilableTopic(buildTopic(t2.name(), 1, 1), NAMESPACE), 
                List.of(new AlterConfigOp(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, "compact"), AlterConfigOp.OpType.SET)))
        );
        var result = kafkaHandler.alterConfigs(pairs);

        verify(kafkaAdminClientSpy, times(1)).incrementalAlterConfigs(any());

        var resultTopicNames = result.ok()
            .map(pair -> pair.getKey().kt().getSpec().getTopicName())
            .collect(Collectors.toSet());
        assertThat(resultTopicNames, is(Set.of(t1.name(), t2.name())));
    }

    @Test
    public void shouldCreatePartitions(KafkaCluster cluster, @TopicPartitions(1) Topic t1, @TopicPartitions(1) Topic t2) {
        var config = TopicOperatorConfig.buildFromMap(Map.of(
            TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), cluster.getBootstrapServers(),
            TopicOperatorConfig.NAMESPACE.key(), NAMESPACE)
        );
        var kafkaAdminClientSpy = spy(Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers())));

        var kafkaHandler = new KafkaHandler(config,
            new TopicOperatorMetricsHolder(KafkaTopic.RESOURCE_KIND, null, new TopicOperatorMetricsProvider(new SimpleMeterRegistry())),
            kafkaAdminClientSpy);
        List<Pair<ReconcilableTopic, NewPartitions>> pairs = List.of(
            new Pair(TopicOperatorTestUtil.reconcilableTopic(buildTopic(t1.name(), 1, 1), NAMESPACE), NewPartitions.increaseTo(2)),
            new Pair(TopicOperatorTestUtil.reconcilableTopic(buildTopic(t2.name(), 1, 1), NAMESPACE), NewPartitions.increaseTo(2))
        );
        var result = kafkaHandler.createPartitions(pairs);

        verify(kafkaAdminClientSpy, times(1)).createPartitions(any());

        var resultTopicNames = result.ok()
            .map(pair -> pair.getKey().kt().getSpec().getTopicName())
            .collect(Collectors.toSet());
        assertThat(resultTopicNames, is(Set.of(t1.name(), t2.name())));
    }

    @Test
    public void shouldDescribeTopics(KafkaCluster cluster, Topic t1, Topic t2) {
        var config = TopicOperatorConfig.buildFromMap(Map.of(
            TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), cluster.getBootstrapServers(),
            TopicOperatorConfig.NAMESPACE.key(), NAMESPACE)
        );
        var kafkaAdminClientSpy = spy(Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers())));

        var kafkaHandler = new KafkaHandler(config,
            new TopicOperatorMetricsHolder(KafkaTopic.RESOURCE_KIND, null, new TopicOperatorMetricsProvider(new SimpleMeterRegistry())),
            kafkaAdminClientSpy);
        var reconcilableTopics = List.of(
            TopicOperatorTestUtil.reconcilableTopic(buildTopic(t1.name(), 1, 1), NAMESPACE),
            TopicOperatorTestUtil.reconcilableTopic(buildTopic(t2.name(), 1, 1), NAMESPACE)
        );
        var result = kafkaHandler.describeTopics(reconcilableTopics);

        verify(kafkaAdminClientSpy, times(1)).describeTopics(anyCollection());
        verify(kafkaAdminClientSpy, times(1)).describeConfigs(any());

        var t1State = result.ok()
            .filter(pair -> Objects.equals(pair.getKey().kt().getSpec().getTopicName(), t1.name()))
            .map(pair -> pair.getValue()).findFirst();
        assertThat(t1State.get().description().name(), is(t1.name()));
        var t2State = result.ok()
            .filter(pair -> Objects.equals(pair.getKey().kt().getSpec().getTopicName(), t2.name()))
            .map(pair -> pair.getValue()).findFirst();
        assertThat(t2State.get().description().name(), is(t2.name()));
    }

    @Test
    public void shouldDeleteTopics(KafkaCluster cluster, Topic t1, Topic t2, Topic t3) {
        var config = TopicOperatorConfig.buildFromMap(Map.of(
            TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), cluster.getBootstrapServers(),
            TopicOperatorConfig.NAMESPACE.key(), NAMESPACE)
        );
        var kafkaAdminClientSpy = spy(Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers())));

        var kafkaHandler = new KafkaHandler(config,
            new TopicOperatorMetricsHolder(KafkaTopic.RESOURCE_KIND, null, new TopicOperatorMetricsProvider(new SimpleMeterRegistry())),
            kafkaAdminClientSpy);
        var reconcilableTopics = List.of(
            TopicOperatorTestUtil.reconcilableTopic(buildTopic(t1.name(), 1, 1), NAMESPACE),
            TopicOperatorTestUtil.reconcilableTopic(buildTopic(t2.name(), 1, 1), NAMESPACE),
            TopicOperatorTestUtil.reconcilableTopic(buildTopic(t3.name(), 1, 1), NAMESPACE)
        );
        var topicNamesToDelete = reconcilableTopics.stream().map(ReconcilableTopic::topicName).collect(Collectors.toSet());
        topicNamesToDelete.removeIf(name -> Objects.equals(name, t3.name()));
        var result = kafkaHandler.deleteTopics(reconcilableTopics, topicNamesToDelete);

        verify(kafkaAdminClientSpy, times(1)).deleteTopics(any(TopicCollection.TopicNameCollection.class));

        var resultTopicNames = result.ok()
            .map(pair -> pair.getKey().kt().getSpec().getTopicName())
            .collect(Collectors.toSet());
        assertThat(resultTopicNames, is(topicNamesToDelete));
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

    public static List<Node> parseNodes(String input) {
        List<Node> nodes = new ArrayList<>();
        var nodeStrings = input.split(",");
        var id = 0;
        for (String nodeString : nodeStrings) {
            String[] parts = nodeString.split(":");
            if (parts.length == 2) {
                nodes.add(new Node(id++, parts[0], Integer.parseInt(parts[1])));
            } else {
                throw new IllegalArgumentException("Invalid node string: " + nodeString);
            }
        }
        return nodes;
    }
}
