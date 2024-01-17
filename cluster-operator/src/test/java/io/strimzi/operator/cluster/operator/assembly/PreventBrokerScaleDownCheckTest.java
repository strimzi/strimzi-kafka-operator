/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class PreventBrokerScaleDownCheckTest {
    private static final String NAMESPACE = "my-namespace";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final Reconciliation RECONCILIATION = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME);
    private static Vertx vertx;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    private void mockDescribeTopics(Admin mockAc) {
        when(mockAc.describeTopics(anyCollection())).thenAnswer(invocation -> {
            DescribeTopicsResult dtr = mock(DescribeTopicsResult.class);
            Node leader = new Node(3, Node.noNode().host(), Node.noNode().port());

            TopicDescription td = new TopicDescription("my-topic", false,
                    List.of(new TopicPartitionInfo(4, leader, List.of(leader), List.of(leader))));

            Map<String, TopicDescription> tds = Map.of(td.name(), td);

            when(dtr.allTopicNames()).thenReturn(KafkaFuture.completedFuture(tds));

            return dtr;
        });
    }

    private ListTopicsResult mockListTopics() {
        ListTopicsResult ltr = mock(ListTopicsResult.class);
        when(ltr.names()).thenAnswer(invocation -> KafkaFuture.completedFuture(Set.of("my-topic"))
        );
        return ltr;
    }

    @Test
    public void testPartitionReplicasNotPresentOnRemovedBrokers(VertxTestContext context) {
        Admin admin = mock(Admin.class);
        AdminClientProvider mock = mock(AdminClientProvider.class);
        when(mock.createAdminClient(anyString(), any(), any(), anyString())).thenReturn(admin);

        ListTopicsResult ltr = mockListTopics();
        when(admin.listTopics(any())).thenReturn(ltr);

        mockDescribeTopics(admin);

        KafkaCluster cluster = mock(KafkaCluster.class);
        doReturn(Set.of(4, 5)).when(cluster).removedNodes();

        Checkpoint checkpoint = context.checkpoint();
        SecretOperator mockSecretOps = ResourceUtils.supplierWithMocks(false).secretOperations;
        Secret secret = new Secret();
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.secretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));

        PreventBrokerScaleDownCheck operations = new PreventBrokerScaleDownCheck();

        operations.canScaleDownBrokers(RECONCILIATION, vertx, cluster.removedNodes(), mockSecretOps, mock)
                .onComplete(context.succeeding(s -> {
                    assertThat(s.isEmpty(), is(true));
                    assertThat(s.size(), is(0));
                    checkpoint.flag();
                }));
    }

    @Test
    public void testPartitionReplicasPresentOnBrokers(VertxTestContext context) {
        Admin admin = mock(Admin.class);
        AdminClientProvider mock = mock(AdminClientProvider.class);
        when(mock.createAdminClient(anyString(), any(), any(), anyString())).thenReturn(admin);

        ListTopicsResult ltr = mockListTopics();
        when(admin.listTopics(any())).thenReturn(ltr);

        mockDescribeTopics(admin);

        KafkaCluster cluster = mock(KafkaCluster.class);
        doReturn(Set.of(2, 3)).when(cluster).removedNodes();

        Checkpoint checkpoint = context.checkpoint();
        SecretOperator mockSecretOps = ResourceUtils.supplierWithMocks(false).secretOperations;
        Secret secret = new Secret();
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.secretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));

        PreventBrokerScaleDownCheck operations = new PreventBrokerScaleDownCheck();

        operations.canScaleDownBrokers(RECONCILIATION, vertx, cluster.removedNodes(), mockSecretOps, mock)
                .onComplete(context.succeeding(s -> {
                    assertThat(s.isEmpty(), is(false));
                    assertThat(s.size(), is(1));
                    checkpoint.flag();
                }));
    }

    @Test
    public void testTopicNamesBeingRetrievedCorrectly(VertxTestContext context) {
        Admin admin = mock(Admin.class);
        AdminClientProvider mock = mock(AdminClientProvider.class);
        when(mock.createAdminClient(anyString(), any(), any(), anyString())).thenReturn(admin);

        mockDescribeTopics(admin);

        ListTopicsResult ltr = mockListTopics();
        when(admin.listTopics(any())).thenReturn(ltr);

        Checkpoint checkpoint = context.checkpoint();

        PreventBrokerScaleDownCheck operations = new PreventBrokerScaleDownCheck();

        operations.topicNames(RECONCILIATION, vertx, admin)
                .onComplete(context.succeeding(topicNames -> {
                    assertThat(topicNames.size(), is(1));
                    assertEquals(topicNames, Set.of("my-topic"));
                    checkpoint.flag();
                }));
    }

    @Test
    public void testTopicDescriptionBeingRetrievedCorrectly(VertxTestContext context) {
        Admin admin = mock(Admin.class);
        AdminClientProvider mock = mock(AdminClientProvider.class);
        when(mock.createAdminClient(anyString(), any(), any(), anyString())).thenReturn(admin);

        mockDescribeTopics(admin);

        ListTopicsResult ltr = mockListTopics();
        when(admin.listTopics(any())).thenReturn(ltr);

        Checkpoint checkpoint = context.checkpoint();

        PreventBrokerScaleDownCheck operations = new PreventBrokerScaleDownCheck();

        operations.describeTopics(RECONCILIATION, vertx, admin, Set.of("my-topic"))
                .onComplete(context.succeeding(topicDescriptions -> {
                    assertThat(topicDescriptions.size(), is(1));
                    assertEquals(topicDescriptions.values().iterator().next().name(), "my-topic");
                    checkpoint.flag();
                }));
    }
}