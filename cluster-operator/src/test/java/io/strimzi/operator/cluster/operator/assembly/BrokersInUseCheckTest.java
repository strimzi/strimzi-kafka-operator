/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static io.strimzi.operator.common.auth.TlsPemIdentity.DUMMY_IDENTITY;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BrokersInUseCheckTest {
    private static final String NAMESPACE = "my-namespace";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final Reconciliation RECONCILIATION = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME);
    private static final Function<Integer, Node> NODE = id -> new Node(id, Node.noNode().host(), Node.noNode().port());

    @Test
    public void testBrokersInUse() {
        Admin admin = mock(Admin.class);
        AdminClientProvider mock = mock(AdminClientProvider.class);
        when(mock.createAdminClient(anyString(), any(), any())).thenReturn(admin);

        // Mock topic description
        TopicDescription t1 = new TopicDescription("my-topic", false, List.of(new TopicPartitionInfo(0, NODE.apply(0), List.of(NODE.apply(0)), List.of(NODE.apply(0)))));
        TopicDescription t2 = new TopicDescription("my-topic2", false, List.of(new TopicPartitionInfo(0, NODE.apply(1), List.of(NODE.apply(1)), List.of(NODE.apply(1)))));
        TopicDescription t3 = new TopicDescription("my-topic3", false, List.of(new TopicPartitionInfo(0, NODE.apply(2), List.of(NODE.apply(2)), List.of(NODE.apply(2)))));
        DescribeTopicsResult dtr = mock(DescribeTopicsResult.class);
        when(dtr.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of(t1.name(), t1, t2.name(), t2, t3.name(), t3)));

        @SuppressWarnings(value = "unchecked")
        ArgumentCaptor<Collection<String>> topicListCaptor = ArgumentCaptor.forClass(Collection.class);
        when(admin.describeTopics(topicListCaptor.capture())).thenReturn(dtr);

        // Mock list topics
        ListTopicsResult ltr = mock(ListTopicsResult.class);
        when(ltr.names()).thenReturn(KafkaFuture.completedFuture(Set.of("my-topic", "my-topic2", "my-topic3")));
        when(admin.listTopics(any())).thenReturn(ltr);

        // Get brokers in use
        BrokersInUseCheck operations = new BrokersInUseCheck();
        Set<Integer> brokersInUse = operations.brokersInUse(RECONCILIATION, DUMMY_IDENTITY, mock)
                .toCompletableFuture()
                .join();

        Collection<String> topicList = topicListCaptor.getValue();
        assertThat(topicList.size(), is(3));
        assertThat(topicList, hasItems("my-topic", "my-topic2", "my-topic3"));

        assertThat(brokersInUse.size(), is(3));
        assertThat(brokersInUse, is(Set.of(0, 1, 2)));
    }

    @Test
    public void testBrokersInUseWithSingleTopicAndMultiplePartitions() {
        Admin admin = mock(Admin.class);
        AdminClientProvider mock = mock(AdminClientProvider.class);
        when(mock.createAdminClient(anyString(), any(), any())).thenReturn(admin);

        // Mock topic description
        TopicDescription t = new TopicDescription("my-topic", false, List.of(
                new TopicPartitionInfo(0, NODE.apply(2), List.of(NODE.apply(0), NODE.apply(1), NODE.apply(4)), List.of(NODE.apply(0), NODE.apply(1), NODE.apply(4))),
                new TopicPartitionInfo(1, NODE.apply(2), List.of(NODE.apply(1), NODE.apply(1), NODE.apply(4)), List.of(NODE.apply(0), NODE.apply(1), NODE.apply(4))),
                new TopicPartitionInfo(2, NODE.apply(2), List.of(NODE.apply(4), NODE.apply(1), NODE.apply(4)), List.of(NODE.apply(0), NODE.apply(1), NODE.apply(4)))));
        DescribeTopicsResult dtr = mock(DescribeTopicsResult.class);
        when(dtr.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of(t.name(), t)));

        @SuppressWarnings(value = "unchecked")
        ArgumentCaptor<Collection<String>> topicListCaptor = ArgumentCaptor.forClass(Collection.class);
        when(admin.describeTopics(topicListCaptor.capture())).thenReturn(dtr);

        // Mock list topics
        ListTopicsResult ltr = mock(ListTopicsResult.class);
        when(ltr.names()).thenReturn(KafkaFuture.completedFuture(Set.of("my-topic")));
        when(admin.listTopics(any())).thenReturn(ltr);

        // Get brokers in use
        BrokersInUseCheck operations = new BrokersInUseCheck();
        Set<Integer> brokersInUse = operations.brokersInUse(RECONCILIATION, DUMMY_IDENTITY, mock)
                .toCompletableFuture()
                .join();

        Collection<String> topicList = topicListCaptor.getValue();
        assertThat(topicList.size(), is(1));
        assertThat(topicList, hasItems("my-topic"));

        assertThat(brokersInUse.size(), is(3));
        assertThat(brokersInUse, is(Set.of(0, 1, 4)));
    }

    @Test
    public void testTopicDescriptionFailure() {
        Admin admin = mock(Admin.class);
        AdminClientProvider mock = mock(AdminClientProvider.class);
        when(mock.createAdminClient(anyString(), any(), any())).thenReturn(admin);

        // Mock topic description
        @SuppressWarnings(value = "unchecked")
        KafkaFuture<Map<String, TopicDescription>> kf = mock(KafkaFuture.class);
        when(kf.toCompletionStage()).thenReturn(CompletableFuture.failedFuture(new Throwable("Test error ...")));
        DescribeTopicsResult dtr = mock(DescribeTopicsResult.class);
        when(dtr.allTopicNames()).thenReturn(kf);
        when(admin.describeTopics(anyCollection())).thenReturn(dtr);

        // Mock list topics
        ListTopicsResult ltr = mock(ListTopicsResult.class);
        when(ltr.names()).thenReturn(KafkaFuture.completedFuture(Set.of("my-topic")));
        when(admin.listTopics(any())).thenReturn(ltr);

        // Get brokers in use
        BrokersInUseCheck operations = new BrokersInUseCheck();
        Exception e = assertThrows(Exception.class, () ->
                operations.brokersInUse(RECONCILIATION, DUMMY_IDENTITY, mock)
                        .toCompletableFuture()
                        .join());

        assertThat(e.getCause().getMessage(), is("Test error ..."));
    }

    @Test
    public void testListTopicsFailure() {
        Admin admin = mock(Admin.class);
        AdminClientProvider mock = mock(AdminClientProvider.class);
        when(mock.createAdminClient(anyString(), any(), any())).thenReturn(admin);

        // Mock list topics
        @SuppressWarnings(value = "unchecked")
        KafkaFuture<Set<String>> kf = mock(KafkaFuture.class);
        when(kf.toCompletionStage()).thenReturn(CompletableFuture.failedFuture(new Throwable("Test error ...")));
        ListTopicsResult ltr = mock(ListTopicsResult.class);
        when(ltr.names()).thenReturn(kf);
        when(admin.listTopics(any())).thenReturn(ltr);

        // Get brokers in use
        BrokersInUseCheck operations = new BrokersInUseCheck();
        Exception e = assertThrows(Exception.class, () ->
                operations.brokersInUse(RECONCILIATION, DUMMY_IDENTITY, mock)
                        .toCompletableFuture()
                        .join());

        assertThat(e.getCause().getMessage(), is("Test error ..."));
    }

    @Test
    public void testKafkaClientFailure() {
        Admin admin = mock(Admin.class);
        AdminClientProvider mock = mock(AdminClientProvider.class);
        when(mock.createAdminClient(anyString(), any(), any())).thenReturn(admin);

        // Mock list topics
        when(admin.listTopics(any())).thenThrow(new KafkaException("Test error ..."));

        // Get brokers in use
        BrokersInUseCheck operations = new BrokersInUseCheck();
        Exception e = assertThrows(Exception.class, () ->
                operations.brokersInUse(RECONCILIATION, DUMMY_IDENTITY, mock)
                        .toCompletableFuture()
                        .join());

        assertThat(e.getCause().getMessage(), is("Test error ..."));
    }
}