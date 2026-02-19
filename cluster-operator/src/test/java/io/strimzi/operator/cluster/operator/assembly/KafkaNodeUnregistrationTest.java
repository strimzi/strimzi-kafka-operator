/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.UnregisterBrokerResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Set;

import static io.strimzi.operator.common.Util.unwrap;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaNodeUnregistrationTest {

    @Test
    void testUnregistration() {
        Admin mockAdmin = ResourceUtils.adminClient();

        UnregisterBrokerResult ubr = mock(UnregisterBrokerResult.class);
        when(ubr.all()).thenReturn(KafkaFuture.completedFuture(null));
        ArgumentCaptor<Integer> unregisteredNodeIdCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockAdmin.unregisterBroker(unregisteredNodeIdCaptor.capture())).thenReturn(ubr);

        AdminClientProvider mockProvider = ResourceUtils.adminClientProvider(mockAdmin);

        KafkaNodeUnregistration.unregisterBrokerNodes(Reconciliation.DUMMY_RECONCILIATION, mockProvider, null, null, Set.of(1874, 1919))
                .whenComplete((v, t) -> {
                    assertThat(unregisteredNodeIdCaptor.getAllValues().size(), is(2));
                    assertThat(unregisteredNodeIdCaptor.getAllValues(), hasItems(1874, 1919));
                }).join();
    }

    @Test
    void testFailingNodeUnregistration() {
        Admin mockAdmin = ResourceUtils.adminClient();

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

        try {
            KafkaNodeUnregistration.unregisterBrokerNodes(Reconciliation.DUMMY_RECONCILIATION, mockProvider, null, null, Set.of(1874, 1919))
                    .join();
            throw new AssertionError("Expected TimeoutException but none was thrown");
        } catch (Exception e) {
            Throwable cause = unwrap(e);
            assertThat(cause, instanceOf(TimeoutException.class));
            assertThat(unregisteredNodeIdCaptor.getAllValues().size(), is(2));
            assertThat(unregisteredNodeIdCaptor.getAllValues(), hasItems(1874, 1919));
        }
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
                }).join();

        KafkaNodeUnregistration.listRegisteredBrokerNodes(Reconciliation.DUMMY_RECONCILIATION, mockProvider, null, null, false)
                .whenComplete((nodes, t) -> {
                    assertThat(nodes.size(), is(3));
                    for (Node node : nodes) {
                        assertThat(node.isFenced(), is(false));
                    }
                }).join();
    }
}
