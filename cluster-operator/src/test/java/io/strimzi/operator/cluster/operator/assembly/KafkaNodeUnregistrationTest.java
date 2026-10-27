/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.UnregisterBrokerResult;
import org.apache.kafka.clients.admin.UnregisterControllerResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KafkaNodeUnregistrationTest {

    @Test
    void testUnregistration() {
        Admin mockAdmin = ResourceUtils.adminClient();

        // Registered nodes: 1874 and 1919 are no longer in the current cluster and should be unregistered
        List<Node> registeredNodes = List.of(new Node(1874, "host-1874", 9092), new Node(1919, "host-1919", 9092), new Node(2000, "host-2000", 9092));
        when(mockAdmin.describeCluster(any(DescribeClusterOptions.class))).thenAnswer(i -> {
            DescribeClusterResult dcr = mock(DescribeClusterResult.class);
            when(dcr.nodes()).thenReturn(KafkaFuture.completedFuture(registeredNodes));
            return dcr;
        });

        UnregisterBrokerResult ubr = mock(UnregisterBrokerResult.class);
        when(ubr.all()).thenReturn(KafkaFuture.completedFuture(null));
        ArgumentCaptor<Integer> unregisteredNodeIdCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockAdmin.unregisterBroker(unregisteredNodeIdCaptor.capture())).thenReturn(ubr);

        AdminClientProvider mockProvider = ResourceUtils.adminClientProvider(mockAdmin);

        // Only node 2000 is still active, 1874 and 1919 were scaled down
        Set<NodeRef> currentBrokerNodes = Set.of(new NodeRef("host-2000", 2000, "brokers", false, true));

        KafkaNodeUnregistration.unregisterBrokerNodes(Reconciliation.DUMMY_RECONCILIATION, mockProvider, null, null, currentBrokerNodes)
                .whenComplete((v, t) -> {
                    assertThat(t, is(nullValue()));
                    assertThat(unregisteredNodeIdCaptor.getAllValues().size(), is(2));
                    assertThat(unregisteredNodeIdCaptor.getAllValues(), hasItems(1874, 1919));
                }).toCompletableFuture().join();
    }

    @Test
    void testFailingNodeUnregistration() {
        Admin mockAdmin = ResourceUtils.adminClient();

        // Registered nodes: both 1874 and 1919 are no longer in the current cluster and should be unregistered
        List<Node> registeredNodes = List.of(new Node(1874, "host-1874", 9092), new Node(1919, "host-1919", 9092));
        when(mockAdmin.describeCluster(any(DescribeClusterOptions.class))).thenAnswer(i -> {
            DescribeClusterResult dcr = mock(DescribeClusterResult.class);
            when(dcr.nodes()).thenReturn(KafkaFuture.completedFuture(registeredNodes));
            return dcr;
        });

        ArgumentCaptor<Integer> unregisteredNodeIdCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockAdmin.unregisterBroker(unregisteredNodeIdCaptor.capture())).thenAnswer(i -> {
            if (i.getArgument(0, Integer.class) == 1919)   {
                KafkaFutureImpl<Void> unregistrationFuture = new KafkaFutureImpl<>();
                unregistrationFuture.completeExceptionally(new TimeoutException("Fake timeout"));
                UnregisterBrokerResult ubr = mock(UnregisterBrokerResult.class);
                when(ubr.all()).thenReturn(unregistrationFuture);
                return ubr;
            } else {
                UnregisterBrokerResult ubr = mock(UnregisterBrokerResult.class);
                when(ubr.all()).thenReturn(KafkaFuture.completedFuture(null));
                return ubr;
            }
        });

        AdminClientProvider mockProvider = ResourceUtils.adminClientProvider(mockAdmin);

        // No current broker nodes, all have been scaled down
        AtomicBoolean completed = new AtomicBoolean(false);
        KafkaNodeUnregistration.unregisterBrokerNodes(Reconciliation.DUMMY_RECONCILIATION, mockProvider, null, null, Set.of())
                .whenComplete((v, t) -> {
                    // unregistration failed but the future always completes with success, retry on next reconciliation
                    assertThat(t, is(nullValue()));
                    assertThat(unregisteredNodeIdCaptor.getAllValues().size(), is(2));
                    assertThat(unregisteredNodeIdCaptor.getAllValues(), hasItems(1874, 1919));
                    completed.set(true);
                }).toCompletableFuture().join();
        assertThat(completed.get(), is(true));
    }

    @Test
    void testFailingBrokerListing() {
        Admin mockAdmin = ResourceUtils.adminClient();

        // Listing fails with a timeout
        when(mockAdmin.describeCluster(any(DescribeClusterOptions.class))).thenAnswer(i -> {
            KafkaFutureImpl<Collection<Node>> listingFuture = new KafkaFutureImpl<>();
            listingFuture.completeExceptionally(new TimeoutException("Fake timeout"));
            DescribeClusterResult dcr = mock(DescribeClusterResult.class);
            when(dcr.nodes()).thenReturn(listingFuture);
            return dcr;
        });

        AdminClientProvider mockProvider = ResourceUtils.adminClientProvider(mockAdmin);

        AtomicBoolean completed = new AtomicBoolean(false);
        KafkaNodeUnregistration.unregisterBrokerNodes(Reconciliation.DUMMY_RECONCILIATION, mockProvider, null, null, Set.of(new NodeRef("host-2000", 2000, "brokers", false, true)))
                .whenComplete((v, t) -> {
                    // listing failed but the future always completes with success, retry on next reconciliation
                    assertThat(t, is(nullValue()));
                    // unregistration was never attempted since listing failed
                    verify(mockAdmin, never()).unregisterBroker(anyInt());
                    completed.set(true);
                }).toCompletableFuture().join();
        assertThat(completed.get(), is(true));
    }

    @Test
    void testListRegisteredBrokerNodes() {
        Admin mockAdmin = ResourceUtils.adminClient();

        List<Node> cluster = List.of(
                new Node(0, "my-broker-0", 9092, "rack-1", true),
                new Node(1, "my-broker-1", 9092, "rack-1", true),
                new Node(2, "my-broker-2", 9092, "rack-2", false),
                new Node(3, "my-broker-3", 9092, "rack-2", false),
                new Node(4, "my-broker-4", 9092, "rack-2", false)
        );

        ArgumentCaptor<DescribeClusterOptions> dcoCaptor = ArgumentCaptor.forClass(DescribeClusterOptions.class);
        when(mockAdmin.describeCluster(dcoCaptor.capture())).thenAnswer(i -> {
            DescribeClusterResult dcr = mock(DescribeClusterResult.class);
            if (i.getArgument(0, DescribeClusterOptions.class).includeFencedBrokers()) {
                when(dcr.nodes()).thenReturn(KafkaFuture.completedFuture(cluster));
            } else {
                when(dcr.nodes()).thenReturn(KafkaFuture.completedFuture(cluster.stream().filter(node -> !node.isFenced()).toList()));
            }
            return dcr;
        });

        AdminClientProvider mockProvider = ResourceUtils.adminClientProvider(mockAdmin);

        KafkaNodeUnregistration.listRegisteredBrokerNodes(Reconciliation.DUMMY_RECONCILIATION, mockProvider, null, null, true)
                .whenComplete((nodes, t) -> {
                    assertThat(nodes.size(), is(5));
                }).toCompletableFuture().join();

        KafkaNodeUnregistration.listRegisteredBrokerNodes(Reconciliation.DUMMY_RECONCILIATION, mockProvider, null, null, false)
                .whenComplete((nodes, t) -> {
                    assertThat(nodes.size(), is(3));
                    for (Node node : nodes) {
                        assertThat(node.isFenced(), is(false));
                    }
                }).toCompletableFuture().join();
    }

    @Test
    void testListRegisteredControllerNodes() {
        Admin mockAdmin = ResourceUtils.adminClient();

        // describeCluster() via controller endpoint returns all controllers present in the metadata log,
        // including node 7 which was removed from the quorum voters but never explicitly unregistered.
        List<Node> registeredControllers = List.of(
                new Node(3, "my-cluster-controllers-3", 9090),
                new Node(4, "my-cluster-controllers-4", 9090),
                new Node(5, "my-cluster-controllers-5", 9090),
                new Node(6, "my-cluster-controllers-6", 9090),
                new Node(7, "my-cluster-controllers-7", 9090)
        );

        DescribeClusterResult dcr = mock(DescribeClusterResult.class);
        when(dcr.nodes()).thenReturn(KafkaFuture.completedFuture(registeredControllers));
        when(mockAdmin.describeCluster()).thenReturn(dcr);

        AdminClientProvider mockProvider = ResourceUtils.adminClientProvider(mockAdmin);

        // The desired (active) controller set after scale-down, node 7 is missing here.
        Set<NodeRef> controllerNodes = Set.of(
                new NodeRef("my-cluster-controllers-3", 3, "controllers", true, false),
                new NodeRef("my-cluster-controllers-4", 4, "controllers", true, false),
                new NodeRef("my-cluster-controllers-5", 5, "controllers", true, false),
                new NodeRef("my-cluster-controllers-6", 6, "controllers", true, false)
        );

        KafkaNodeUnregistration.listRegisteredControllerNodes(Reconciliation.DUMMY_RECONCILIATION, mockProvider, null, null, controllerNodes)
                .whenComplete((nodes, t) -> {
                    assertThat(nodes.size(), is(5));
                    assertThat(nodes.stream().map(Node::id).toList(), hasItems(3, 4, 5, 6, 7));
                }).toCompletableFuture().join();
    }

    @Test
    void testControllerUnregistration() {
        Admin mockAdmin = ResourceUtils.adminClient();

        // Registered controllers: 1874 and 1919 are no longer in the current cluster and should be unregistered
        List<Node> registeredControllers = List.of(new Node(1874, "host-1874", 9090), new Node(1919, "host-1919", 9090), new Node(2000, "host-2000", 9090));
        DescribeClusterResult dcr = mock(DescribeClusterResult.class);
        when(dcr.nodes()).thenReturn(KafkaFuture.completedFuture(registeredControllers));
        when(mockAdmin.describeCluster()).thenReturn(dcr);

        UnregisterControllerResult ucr = mock(UnregisterControllerResult.class);
        when(ucr.all()).thenReturn(KafkaFuture.completedFuture(null));
        ArgumentCaptor<Integer> unregisteredNodeIdCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockAdmin.unregisterController(unregisteredNodeIdCaptor.capture())).thenReturn(ucr);

        AdminClientProvider mockProvider = ResourceUtils.adminClientProvider(mockAdmin);

        // Only node 2000 is still active, 1874 and 1919 were scaled down
        Set<NodeRef> currentControllerNodes = Set.of(new NodeRef("host-2000", 2000, "controllers", true, false));

        KafkaNodeUnregistration.unregisterControllerNodes(Reconciliation.DUMMY_RECONCILIATION, mockProvider, null, null, currentControllerNodes)
                .whenComplete((v, t) -> {
                    assertThat(t, is(nullValue()));
                    assertThat(unregisteredNodeIdCaptor.getAllValues().size(), is(2));
                    assertThat(unregisteredNodeIdCaptor.getAllValues(), hasItems(1874, 1919));
                }).toCompletableFuture().join();
    }

    @Test
    void testFailingControllerNodeUnregistration() {
        Admin mockAdmin = ResourceUtils.adminClient();

        // Registered controllers: both 1874 and 1919 are no longer in the current cluster and should be unregistered
        List<Node> registeredControllers = List.of(new Node(1874, "host-1874", 9090), new Node(1919, "host-1919", 9090));
        DescribeClusterResult dcr = mock(DescribeClusterResult.class);
        when(dcr.nodes()).thenReturn(KafkaFuture.completedFuture(registeredControllers));
        when(mockAdmin.describeCluster()).thenReturn(dcr);

        ArgumentCaptor<Integer> unregisteredNodeIdCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockAdmin.unregisterController(unregisteredNodeIdCaptor.capture())).thenAnswer(i -> {
            if (i.getArgument(0, Integer.class) == 1919)   {
                KafkaFutureImpl<Void> unregistrationFuture = new KafkaFutureImpl<>();
                unregistrationFuture.completeExceptionally(new TimeoutException("Fake timeout"));
                UnregisterControllerResult ucr = mock(UnregisterControllerResult.class);
                when(ucr.all()).thenReturn(unregistrationFuture);
                return ucr;
            } else {
                UnregisterControllerResult ucr = mock(UnregisterControllerResult.class);
                when(ucr.all()).thenReturn(KafkaFuture.completedFuture(null));
                return ucr;
            }
        });

        AdminClientProvider mockProvider = ResourceUtils.adminClientProvider(mockAdmin);

        // No current controller nodes, all have been scaled down
        AtomicBoolean completed = new AtomicBoolean(false);
        KafkaNodeUnregistration.unregisterControllerNodes(Reconciliation.DUMMY_RECONCILIATION, mockProvider, null, null, Set.of())
                .whenComplete((v, t) -> {
                    // unregistration failed but the future always completes with success, retry on next reconciliation
                    assertThat(t, is(nullValue()));
                    assertThat(unregisteredNodeIdCaptor.getAllValues().size(), is(2));
                    assertThat(unregisteredNodeIdCaptor.getAllValues(), hasItems(1874, 1919));
                    completed.set(true);
                }).toCompletableFuture().join();
        assertThat(completed.get(), is(true));
    }

    @Test
    void testFailingControllerListing() {
        Admin mockAdmin = ResourceUtils.adminClient();

        // Listing fails with a timeout
        KafkaFutureImpl<Collection<Node>> listingFuture = new KafkaFutureImpl<>();
        listingFuture.completeExceptionally(new TimeoutException("Fake timeout"));
        DescribeClusterResult dcr = mock(DescribeClusterResult.class);
        when(dcr.nodes()).thenReturn(listingFuture);
        when(mockAdmin.describeCluster()).thenReturn(dcr);

        AdminClientProvider mockProvider = ResourceUtils.adminClientProvider(mockAdmin);

        AtomicBoolean completed = new AtomicBoolean(false);
        KafkaNodeUnregistration.unregisterControllerNodes(Reconciliation.DUMMY_RECONCILIATION, mockProvider, null, null, Set.of(new NodeRef("host-2000", 2000, "controllers", true, false)))
                .whenComplete((v, t) -> {
                    // listing failed but the future always completes with success, retry on next reconciliation
                    assertThat(t, is(nullValue()));
                    // unregistration was never attempted since listing failed
                    verify(mockAdmin, never()).unregisterController(anyInt());
                    completed.set(true);
                }).toCompletableFuture().join();
        assertThat(completed.get(), is(true));
    }
}
