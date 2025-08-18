/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.UnregisterBrokerResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaNodeUnregistrationTest {
    private static Vertx vertx;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    @Test
    void testUnregistration(VertxTestContext context) {
        Admin mockAdmin = ResourceUtils.adminClient();

        UnregisterBrokerResult ubr = mock(UnregisterBrokerResult.class);
        when(ubr.all()).thenReturn(KafkaFuture.completedFuture(null));
        ArgumentCaptor<Integer> unregisteredNodeIdCaptor = ArgumentCaptor.forClass(Integer.class);
        when(mockAdmin.unregisterBroker(unregisteredNodeIdCaptor.capture())).thenReturn(ubr);

        AdminClientProvider mockProvider = ResourceUtils.adminClientProvider(mockAdmin);

        Checkpoint async = context.checkpoint();
        KafkaNodeUnregistration.unregisterBrokerNodes(Reconciliation.DUMMY_RECONCILIATION, vertx, mockProvider, null, null, Set.of(1874, 1919))
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(unregisteredNodeIdCaptor.getAllValues().size(), is(2));
                    assertThat(unregisteredNodeIdCaptor.getAllValues(), hasItems(1874, 1919));

                    async.flag();
                })));
    }

    @Test
    void testFailingNodeUnregistration(VertxTestContext context) {
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

        Checkpoint async = context.checkpoint();
        KafkaNodeUnregistration.unregisterBrokerNodes(Reconciliation.DUMMY_RECONCILIATION, vertx, mockProvider, null, null, Set.of(1874, 1919))
                .onComplete(context.failing(v -> context.verify(() -> {
                    assertThat(unregisteredNodeIdCaptor.getAllValues().size(), is(2));
                    assertThat(unregisteredNodeIdCaptor.getAllValues(), hasItems(1874, 1919));

                    async.flag();
                })));
    }

    @Test
    void testListRegisteredBrokerNodes(VertxTestContext context) throws InterruptedException {
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

        CountDownLatch latch = new CountDownLatch(2);

        KafkaNodeUnregistration.listRegisteredBrokerNodes(Reconciliation.DUMMY_RECONCILIATION, vertx, mockProvider, null, null, true)
                .onComplete(context.succeeding(nodes -> context.verify(() -> {
                    assertThat(nodes.size(), is(5));
                    latch.countDown();
                })));

        KafkaNodeUnregistration.listRegisteredBrokerNodes(Reconciliation.DUMMY_RECONCILIATION, vertx, mockProvider, null, null, false)
                .onComplete(context.succeeding(nodes -> context.verify(() -> {
                    assertThat(nodes.size(), is(3));
                    for (Node node : nodes) {
                        assertThat(node.isFenced(), is(false));
                    }
                    latch.countDown();
                })));

        latch.await(10, TimeUnit.SECONDS);
        context.completeNow();
    }
}
