/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.mockito.stubbing.OngoingStubbing;

import static com.google.common.primitives.Ints.asList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A logical model of a Kafka cluster, from which a (partial) mock Admin client can be created.
 * The state of the cluster (topics, partitions, ISR, leaders, controller, etc.)
 * can be easily manipulated and those changes a reflected in the view of the cluster observed via the Admin client.
 */
class ClusterModel {
    private Throwable listTopicsResult;
    private final Map<String, Throwable> describeTopicsResult = new HashMap<>(1);
    private final Map<ConfigResource, Throwable> describeConfigsResult = new HashMap<>(1);

    private int controller = -1;
    private final Map<String, TopicState> topics = new HashMap<>();
    private final Map<Integer, BrokerState> brokers = new HashMap<>();
    //private final Map<Integer, Node> nodes = new HashMap<>();
    private final Admin ac = mock(AdminClient.class);

    class TopicState {
        class PartitionState {
            private final int id;
            private final Set<Integer> isr = new HashSet<>(3);
            private int leader = -1;
            private final List<Integer> replicas = new ArrayList<>(3);

            public PartitionState(int partitionId) {
                if (partitionId < 0) {
                    throw new IllegalArgumentException();
                }
                this.id = partitionId;
            }

            PartitionState replicaOn(int... brokerIds) {
                this.replicas.addAll(asList(brokerIds));
                return this;
            }

            PartitionState leader(int brokerId) {
                if (brokerId < -1) {
                    throw new IllegalArgumentException();
                }
                this.leader = brokerId;
                return this;
            }

            PartitionState isr(int... brokerIds) {
                this.isr.addAll(asList(brokerIds));
                return this;
            }

            TopicState endPartition() {
                if (this.leader != -1) {
                    if (this.replicas.stream().noneMatch(x -> x == this.leader)) {
                        throw new RuntimeException("Leader must be one of the replicas");
                    }
                    if (!this.isr.isEmpty() && this.isr.stream().noneMatch(x -> x == this.leader)) {
                        throw new RuntimeException("ISR must include the leader");
                    }
                }
                if (!this.isr.stream().allMatch(x -> this.replicas.stream().anyMatch(y -> x == y))) {
                    throw new RuntimeException("ISR must be a subset of the replicas");
                }
                return TopicState.this;
            }
        }

        private final String name;
        private final boolean internal;
        private final Map<String, String> configs = new HashMap<>();
        private final Map<Integer, PartitionState> partitions = new HashMap<>();

        public TopicState(String name) {
            if (Objects.requireNonNull(name).isEmpty()) {
                throw new IllegalArgumentException();
            }
            this.name = name;
            this.internal = name.equals("__transaction_state") || name.equals("__consumer_offsets");
        }

        TopicState addToConfig(String config, String value) {
            configs.put(config, value);
            return this;
        }

        PartitionState addNewPartition(int partition) {
            return partitions.computeIfAbsent(partition, PartitionState::new);
        }


        ClusterModel endTopic() {
            return ClusterModel.this;
        }
    }

    class BrokerState {

        private final int id;
        private volatile boolean isUp = false;

        public BrokerState(int id) {
            this.id = id;
        }

        ClusterModel endBroker() {
            return ClusterModel.this;
        }
    }

    TopicState addNewTopic(String name) {
        return topics.computeIfAbsent(name, n -> new TopicState(n));
    }

    ClusterModel addNBrokers(int n) {
        if (n <= 0)
            throw new IllegalArgumentException();
        var remaining = n;
        var brokerId = 0;
        while (remaining > 0) {
            if (!brokers.containsKey(brokerId)) {
                if (brokers.putIfAbsent(brokerId, new BrokerState(brokerId)) != null) {
                    throw new IllegalStateException("Already have broker " + brokerId);
                }
                remaining--;
            }
            brokerId++;
        }
        return this;
    }

    ClusterModel addBrokersWithIds(int... ids) {
        for (int id : ids) {
            if (id < 0)
                throw new IllegalArgumentException();
            brokers.computeIfAbsent(id, BrokerState::new);
        }
        return this;
    }

    /**
     * @return The ids of all the brokers in the cluster (including brokers which are "down").
     */
    public Set<Integer> brokerIds() {
        return Set.copyOf(brokers.keySet());
    }

    /**
     * @return The ids of all the live (up) brokers in the cluster.
     */
    public Set<Integer> liveBrokerIds() {
        return Set.copyOf(brokers.keySet());
    }

    private static <T> KafkaFuture<T> failedFuture(Throwable t) {
        KafkaFutureImpl<T> kafkaFuture = new KafkaFutureImpl<>();
        kafkaFuture.completeExceptionally(t);
        return kafkaFuture;
    }

    private ListTopicsResult mockListTopics(boolean shouldListInternal) {
        ListTopicsResult ltr = mock(ListTopicsResult.class);
        when(ltr.names()).thenAnswer(invocation ->
                listTopicsResult != null ? failedFuture(listTopicsResult) : KafkaFuture.completedFuture(
                        topics.keySet().stream()
                                .filter(name ->
                                        shouldListInternal || !(name.equals("__consumer_offsets") || name.equals("__transaction_state")))
                                .collect(Collectors.toSet()))
        );
        when(ltr.listings()).thenThrow(notImplemented());
        when(ltr.namesToListings()).thenThrow(notImplemented());
        return ltr;
    }

    ClusterModel listTopicsResult(Throwable t) {
        listTopicsResult = t;
        return this;
    }

    ClusterModel describeTopicsResult(String topic, Throwable t) {
        describeTopicsResult.put(topic, t);
        return this;
    }

    ClusterModel describeConfigsResult(ConfigResource config, Throwable t) {
        describeConfigsResult.put(config, t);
        return this;
    }

    private static Throwable notImplemented() {
        return new UnsupportedOperationException("Not implemented by " + ClusterModel.class.getName());
    }

    private static <T> KafkaFuture<T> notImplementedFuture() {
        KafkaFutureImpl<T> future = new KafkaFutureImpl<>();
        future.completeExceptionally(notImplemented());
        return future;
    }

    void mockDescribeTopics() {
        when(ac.describeTopics(any())).thenAnswer(invocation -> {
            DescribeTopicsResult dtr = mock(DescribeTopicsResult.class);
            Collection<String> topicNames = invocation.getArgument(0);
            Throwable throwable = null;
            for (String topicName : topicNames) {
                throwable = describeTopicsResult.get(topicName);
                if (throwable != null) {
                    break;
                }
            }
            if (throwable != null) {
                when(dtr.all()).thenReturn(failedFuture(throwable));
            } else {
                Map<String, TopicDescription> tds = topics.entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> {
                        TopicState topicState = entry.getValue();
                        return new TopicDescription(topicState.name, topicState.internal,
                                topicState.partitions.values().stream().map(partitionState -> new TopicPartitionInfo(partitionState.id,
                                        asNode(partitionState.leader),
                                        partitionState.replicas.stream().map(ClusterModel::asNode).collect(Collectors.toList()),
                                        partitionState.isr.stream().map(ClusterModel::asNode).collect(Collectors.toList()))).collect(Collectors.toList()));
                    }
                ));
                when(dtr.all()).thenReturn(KafkaFuture.completedFuture(tds));
                when(dtr.values()).thenThrow(notImplemented());
            }
            return dtr;
        });
    }

    void mockDescribeConfigs() {
        when(ac.describeConfigs(any())).thenAnswer(invocation -> {
            Collection<ConfigResource> argument = invocation.getArgument(0);
            DescribeConfigsResult dcr = mock(DescribeConfigsResult.class);
            Throwable throwable = null;
            for (ConfigResource configResource : argument) {
                throwable = describeConfigsResult.get(configResource);
                if (throwable != null) {
                    break;
                }
            }
            when(dcr.values()).thenThrow(notImplemented());
            if (throwable != null) {
                when(dcr.all()).thenReturn(failedFuture(throwable));
            } else {
                Map<ConfigResource, Config> result = new HashMap<>();
                for (ConfigResource cr : argument) {
                    List<ConfigEntry> entries = new ArrayList<>();
                    for (Map.Entry<String, String> e : topics.get(cr.name()).configs.entrySet()) {
                        ConfigEntry ce = new ConfigEntry(e.getKey(), e.getValue());
                        entries.add(ce);
                    }
                    result.put(cr, new Config(entries));
                }
                when(dcr.all()).thenReturn(KafkaFuture.completedFuture(result));
            }
            return dcr;
        });
    }

    void mockDescribeCluster() {
        when(ac.describeCluster()).thenCallRealMethod();
        when(ac.describeCluster(any())).thenAnswer(describeInvocation -> {
            DescribeClusterResult result = mock(DescribeClusterResult.class);
            when(result.clusterId()).thenReturn(notImplementedFuture());
            when(result.authorizedOperations()).thenReturn(notImplementedFuture());
            when(result.controller()).thenAnswer(controllerInvocation -> KafkaFuture.completedFuture(asNode(controller)));
            when(result.nodes()).thenAnswer(nodesInvocation -> KafkaFuture.completedFuture(brokers.keySet().stream()
                    .map(ClusterModel::asNode)
                    .collect(Collectors.toList())));
            return result;
        });

    }

    static Node asNode(int brokerId) {
        if (brokerId == -1) {
            return Node.noNode();
        }
        return new Node(brokerId, "kafka-" + brokerId + ".example.org", 9092);
    }

    Admin buildAdminClient() {

        mockDescribeCluster();

        mockListTopics();

        mockDescribeTopics();

        mockDescribeConfigs();

        return ac;
    }

    private OngoingStubbing<ListTopicsResult> mockListTopics() {
        return when(ac.listTopics(any())).thenAnswer(invocation -> mockListTopics(((ListTopicsOptions) invocation.getArgument(0)).shouldListInternal()));
    }

    static final class Foo {
        private Foo() {}
    }

    /**
     * Bring the given broker up. It will not resume leadership of any partitions which it is replicating.
     * @param brokerId The broker id
     * @throws IllegalStateException If the broker does not exist.
     * @return This.
     */
    public ClusterModel brokerUp(int brokerId) {
        BrokerState brokerState = checkBroker(brokerId);
        brokerState.isUp = true;
        return this;
    }

    private BrokerState checkBroker(int brokerId) {
        BrokerState brokerState = brokers.get(brokerId);
        if (brokerState == null) {
            throw new IllegalStateException("Unknown broker " + brokerId);
        }
        return brokerState;
    }

    /**
     * Bring the given broker down. It will be removed from the ISR for all partitions it is replicating,
     * and wil be removed as leader (replaced with another broker that's in the ISR, if there is one).
     * @param brokerId
     * @throws IllegalStateException If the broker does not exist.
     * @return This.
     */
    public ClusterModel brokerDown(int brokerId) {
        // elect new leaders from the ISR
        var broker = checkBroker(brokerId);
        topics.values().forEach(topic -> topic.partitions.values().forEach(partition -> {
            maybeElectAnotherLeader(topic.name, partition.id, brokerId);
            partition.isr.remove(brokerId);
        }));

        // Update the brokers
        broker.isUp = false;
        return this;
    }

    /**
     * Removes the current leader. Does not change the ISR.
     * @param topicName The topic name.
     * @param partitionId The partition id.
     * @throws IllegalStateException If the topic or partition do not exist
     * @return This.
     */
    public ClusterModel unelectLeader(String topicName, int partitionId) {
        partition(topicName, partitionId).leader(-1);
        return this;
    }

    private TopicState.PartitionState partition(String topicName, int partitionId) {
        var topic = topics.get(topicName);
        if (topic == null) {
            throw new IllegalStateException("Unknown topic " + topicName);
        }
        var partition = topic.partitions.get(partitionId);
        if (partition == null) {
            throw new IllegalStateException("Unknown partition " + partitionId + " of topic " + topicName);
        }
        return partition;
    }

    /**
     * Make the given brokerId the current leader, adding it to the ISR if necessary
     * @param topicName The topic name
     * @param partitionId The partition
     * @param brokerId The broker to make leader
     * @throws IllegalStateException If the topic or partition do not exist, or the given brokerId is not a replica.
     * @return This.
     */
    public ClusterModel electLeader(String topicName, int partitionId, int brokerId) {
        var partition = partition(topicName, partitionId);

        checkReplica(topicName, partition, brokerId);
        partition.isr.add(brokerId);
        partition.leader(brokerId);
        return this;
    }

    /**
     * IF the given brokerId is the current leader, elects another member of the ISR as leader, or elects no leader.
     * @param topicName The topic name
     * @param partitionId The partition id
     * @param brokerId The broker to de-elect.
     * @return This.
     */
    public ClusterModel maybeElectAnotherLeader(String topicName, int partitionId, int brokerId) {
        var partition = partition(topicName, partitionId);
        if (partition.leader == brokerId) {
            var newLeader = partition.replicas.stream().filter(partition.isr::contains).filter(replica -> replica != brokerId).findFirst().orElse(-1);
            if (newLeader >= 0) {
                electLeader(topicName, partition.id, newLeader);
            } else {
                unelectLeader(topicName, partition.id);
            }
        }
        return this;
    }

    /**
     * Make the preferred leader the current leader, adding it to the ISR if needed.
     * @param topicName The topic name
     * @param partitionId The partition id
     * @throws IllegalStateException If the topic or partition do not exist, or the partition has not replicas.
     * @return This.
     */
    public ClusterModel electPreferredLeader(String topicName, int partitionId) {
        var partition = partition(topicName, partitionId);
        if (partition.replicas.isEmpty()) {
            throw new IllegalStateException("Topic " + topicName + " partition " + partitionId + " has empty replicas");
        }
        Integer preferredLeader = partition.replicas.get(0);
        electLeader(topicName, partitionId, preferredLeader);
        return this;
    }

    private void checkReplica(String topicName, TopicState.PartitionState partition, Integer preferredLeader) {
        if (!partition.replicas.contains(preferredLeader)) {
            throw new IllegalStateException("Topic " + topicName + " partition " + partition.id + " is not replicated on " + preferredLeader + ", so it cannot be made leader");
        }
    }

    /**
     * Remove the given broker from the ISR, electing another member if the ISR as leader if there is one.
     * @param topicName The topic name
     * @param partitionId The partition id
     * @param removedBroker The broker to remove
     * @throws IllegalStateException If the topic or partition do not exist, or the given brokerId is not a replica.
     * @return This.
     */
    public ClusterModel shrinkIsr(String topicName, int partitionId, int removedBroker) {
        var partition = partition(topicName, partitionId);
        checkReplica(topicName, partition, removedBroker);
        partition.isr.remove(removedBroker);
        maybeElectAnotherLeader(topicName, partitionId, removedBroker);
        return this;
    }

    /**
     * Add the given broker to the ISR. Does not change the leader.
     * @param topicName The topic name
     * @param partitionId The partition id
     * @param addedBroker The broker to remove
     * @throws IllegalStateException If the topic or partition do not exist, or the given brokerId is not a replica.
     * @return This.
     */
    public ClusterModel expandIsr(String topicName, int partitionId, int addedBroker) {
        var partition = partition(topicName, partitionId);
        checkReplica(topicName, partition, addedBroker);
        partition.isr.add(addedBroker);
        return this;
    }
}
