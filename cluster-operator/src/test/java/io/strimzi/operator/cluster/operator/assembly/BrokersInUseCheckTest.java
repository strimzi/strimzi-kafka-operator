/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.strimzi.operator.common.auth.Identity.DUMMY_IDENTITY;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Timeout(value = 30, unit = TimeUnit.SECONDS)
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

    @Test
    public void testVolumesInUseWithEmptyVolumes() {
        Admin admin = mock(Admin.class);
        AdminClientProvider mock = mock(AdminClientProvider.class);
        when(mock.createAdminClient(anyString(), any(), any())).thenReturn(admin);

        // Volume 1 exists on both nodes, but holds no partition replicas
        mockLogDirs(admin, Map.of(
                0, Map.of(logDir(1, 0), new LogDirDescription(null, Map.of())),
                1, Map.of(logDir(1, 1), new LogDirDescription(null, Map.of()))));

        BrokersInUseCheck operations = new BrokersInUseCheck();
        BrokersInUseCheck.VolumesInUse nodesInUse = operations.volumesInUse(RECONCILIATION, DUMMY_IDENTITY, mock, Map.of(0, Set.of(1), 1, Set.of(1)))
                .toCompletableFuture()
                .join();

        assertThat(nodesInUse.nothingBlocked(), is(true));
        verify(admin).close();

        @SuppressWarnings(value = "unchecked")
        ArgumentCaptor<Collection<Integer>> nodeIdCaptor = ArgumentCaptor.forClass(Collection.class);
        verify(admin).describeLogDirs(nodeIdCaptor.capture());
        assertThat(nodeIdCaptor.getValue(), containsInAnyOrder(0, 1));
    }

    @Test
    public void testVolumesInUseWithPartitionReplicas() {
        Admin admin = mock(Admin.class);
        AdminClientProvider mock = mock(AdminClientProvider.class);
        when(mock.createAdminClient(anyString(), any(), any())).thenReturn(admin);

        // Volume 1 holds a partition replica on node 1 only
        mockLogDirs(admin, Map.of(
                0, Map.of(logDir(1, 0), new LogDirDescription(null, Map.of())),
                1, Map.of(logDir(1, 1), new LogDirDescription(null, Map.of(new TopicPartition("my-topic", 0), new ReplicaInfo(1000L, 0L, false))))));

        BrokersInUseCheck operations = new BrokersInUseCheck();
        BrokersInUseCheck.VolumesInUse nodesInUse = operations.volumesInUse(RECONCILIATION, DUMMY_IDENTITY, mock, Map.of(0, Set.of(1), 1, Set.of(1)))
                .toCompletableFuture()
                .join();

        assertThat(nodesInUse.notEmpty(), is(Map.of(1, Set.of(1))));
        assertThat(nodesInUse.notChecked(), is(Map.of()));
    }

    @Test
    public void testVolumesInUseIgnoresVolumesWhichAreNotRemoved() {
        Admin admin = mock(Admin.class);
        AdminClientProvider mock = mock(AdminClientProvider.class);
        when(mock.createAdminClient(anyString(), any(), any())).thenReturn(admin);

        // Volume 0 holds a partition replica, but only volume 1 is being removed
        mockLogDirs(admin, Map.of(
                0, Map.of(logDir(0, 0), new LogDirDescription(null, Map.of(new TopicPartition("my-topic", 0), new ReplicaInfo(1000L, 0L, false))),
                        logDir(1, 0), new LogDirDescription(null, Map.of()))));

        BrokersInUseCheck operations = new BrokersInUseCheck();
        BrokersInUseCheck.VolumesInUse nodesInUse = operations.volumesInUse(RECONCILIATION, DUMMY_IDENTITY, mock, Map.of(0, Set.of(1)))
                .toCompletableFuture()
                .join();

        assertThat(nodesInUse.nothingBlocked(), is(true));
    }

    @Test
    public void testVolumesInUseWithOfflineLogDir() {
        Admin admin = mock(Admin.class);
        AdminClientProvider mock = mock(AdminClientProvider.class);
        when(mock.createAdminClient(anyString(), any(), any())).thenReturn(admin);

        // An offline log directory reports an error and no replicas, so its content is not known
        mockLogDirs(admin, Map.of(
                0, Map.of(logDir(1, 0), new LogDirDescription(new KafkaStorageException("Log dir is offline"), Map.of()))));

        BrokersInUseCheck operations = new BrokersInUseCheck();
        BrokersInUseCheck.VolumesInUse nodesInUse = operations.volumesInUse(RECONCILIATION, DUMMY_IDENTITY, mock, Map.of(0, Set.of(1)))
                .toCompletableFuture()
                .join();

        assertThat(nodesInUse.notEmpty(), is(Map.of()));
        assertThat(nodesInUse.notChecked(), is(Map.of(0, Set.of(1))));
    }

    @Test
    public void testVolumesInUseWhenTheAdminClientCannotBeCreated() {
        AdminClientProvider mock = mock(AdminClientProvider.class);
        when(mock.createAdminClient(anyString(), any(), any())).thenThrow(new KafkaException("Test error ..."));

        BrokersInUseCheck operations = new BrokersInUseCheck();
        Exception e = assertThrows(Exception.class, () ->
                operations.volumesInUse(RECONCILIATION, DUMMY_IDENTITY, mock, Map.of(0, Set.of(1)))
                        .toCompletableFuture()
                        .join());

        // The check fails instead of blocking, because without a client it cannot ask Kafka anything at all
        assertThat(e.getCause().getMessage(), is("Test error ..."));
    }

    @Test
    public void testVolumesInUseKafkaClientFailure() {
        Admin admin = mock(Admin.class);
        AdminClientProvider mock = mock(AdminClientProvider.class);
        when(mock.createAdminClient(anyString(), any(), any())).thenReturn(admin);

        when(admin.describeLogDirs(anyCollection())).thenThrow(new KafkaException("Test error ..."));

        BrokersInUseCheck operations = new BrokersInUseCheck();
        Exception e = assertThrows(Exception.class, () ->
                operations.volumesInUse(RECONCILIATION, DUMMY_IDENTITY, mock, Map.of(0, Set.of(1)))
                        .toCompletableFuture()
                        .join());

        assertThat(e.getCause().getMessage(), is("Test error ..."));
        verify(admin).close();
    }

    @Test
    public void testVolumesInUseWhenNodeDoesNotAnswer() {
        Admin admin = mock(Admin.class);
        AdminClientProvider mock = mock(AdminClientProvider.class);
        when(mock.createAdminClient(anyString(), any(), any())).thenReturn(admin);

        KafkaFutureImpl<Map<String, LogDirDescription>> failed = new KafkaFutureImpl<>();
        failed.completeExceptionally(new TimeoutException("Timed out waiting for a node assignment."));

        // Node 0 answers and its volume is empty. Node 1 does not answer at all.
        mockLogDirFutures(admin, Map.of(
                0, KafkaFuture.completedFuture(Map.of(logDir(1, 0), new LogDirDescription(null, Map.of()))),
                1, failed));

        BrokersInUseCheck operations = new BrokersInUseCheck();
        BrokersInUseCheck.VolumesInUse nodesInUse = operations.volumesInUse(RECONCILIATION, DUMMY_IDENTITY, mock, Map.of(0, Set.of(1), 1, Set.of(1)))
                .toCompletableFuture()
                .join();

        // The node which did not answer blocks the removal, but it is not reported as holding replicas
        assertThat(nodesInUse.notEmpty(), is(Map.of()));
        assertThat(nodesInUse.notChecked(), is(Map.of(1, Set.of(1))));
    }

    @Test
    public void testVolumesInUseWhenNodeIsMissingFromTheResponse() {
        Admin admin = mock(Admin.class);
        AdminClientProvider mock = mock(AdminClientProvider.class);
        when(mock.createAdminClient(anyString(), any(), any())).thenReturn(admin);

        mockLogDirFutures(admin, Map.of());

        BrokersInUseCheck operations = new BrokersInUseCheck();
        BrokersInUseCheck.VolumesInUse nodesInUse = operations.volumesInUse(RECONCILIATION, DUMMY_IDENTITY, mock, Map.of(0, Set.of(1)))
                .toCompletableFuture()
                .join();

        assertThat(nodesInUse.notEmpty(), is(Map.of()));
        assertThat(nodesInUse.notChecked(), is(Map.of(0, Set.of(1))));
    }

    private static String logDir(int volumeId, int nodeId) {
        return "/var/lib/kafka/data-" + volumeId + "/kafka-log" + nodeId;
    }

    private static void mockLogDirs(Admin admin, Map<Integer, Map<String, LogDirDescription>> logDirs) {
        mockLogDirFutures(admin, logDirs.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> KafkaFuture.completedFuture(entry.getValue()))));
    }

    private static void mockLogDirFutures(Admin admin, Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> logDirs) {
        DescribeLogDirsResult dldr = mock(DescribeLogDirsResult.class);
        when(dldr.descriptions()).thenReturn(logDirs);
        when(admin.describeLogDirs(anyCollection())).thenReturn(dldr);
    }
}
