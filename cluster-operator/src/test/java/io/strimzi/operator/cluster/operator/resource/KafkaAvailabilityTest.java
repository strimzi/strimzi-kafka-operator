/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaAvailabilityTest {

    private static Vertx vertx;

    private KafkaAvailability kafkaAvailability;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    KafkaAvailability ka(Admin admin) {
        return kafkaAvailability = new KafkaAvailability(Reconciliation.DUMMY_RECONCILIATION, vertx, admin);
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    static class UncheckedInterruptedException extends RuntimeException{
        public UncheckedInterruptedException(Throwable cause) {
            super(cause);
        }
    }

    private static <T> T await(Future<T> future) {
        CountDownLatch latch = new CountDownLatch(1);
        future.onComplete(ar -> latch.countDown());
        try {
            if (latch.await(1, TimeUnit.SECONDS)) {
                if (future.failed()) {
                    Assertions.fail(future.cause());
                }
                return future.result();
            } else {
                Assertions.fail("Future wasn't completed within timeout");
                throw new RuntimeException(); // to appease definite return checking
            }
        } catch (InterruptedException e) {
            throw new UncheckedInterruptedException(e);
        }
    }

    private static Throwable awaitThrows(Future<?> future) {
        CountDownLatch latch = new CountDownLatch(1);
        future.onComplete(ar -> latch.countDown());
        try {
            if (latch.await(1, TimeUnit.SECONDS)) {
                if (future.succeeded()) {
                    Assertions.fail("Expected future to have failed");
                }
                return future.cause();
            } else {
                Assertions.fail("Future wasn't completed within timeout");
                throw new RuntimeException(); // to appease definite return checking
            }
        } catch (InterruptedException e) {
            throw new UncheckedInterruptedException(e);
        }
    }

    private ClusterModel clusterWithTwoTopics(int numBrokers, int minIsr, List<Integer> isr) {
        return new ClusterModel()
                .addNBrokers(numBrokers)
                .addNewTopic("A")
                    .addToConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, Integer.toString(minIsr))
                    .addNewPartition(0)
                        .replicaOn(0, 1, 2)
                        .leader(0)
                        .isr(isr.stream().mapToInt(Integer::intValue).toArray())
                    .endPartition()
                .endTopic()
                .addNewTopic("B")
                    .addToConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, Integer.toString(minIsr))
                    .addNewPartition(0)
                        .replicaOn(1, 2, 0)
                        .leader(1)
                        .isr(isr.stream().mapToInt(Integer::intValue).toArray())
                    .endPartition()
                .endTopic();
    }

    @Test
    public void testBelowMinIsr() {
        ClusterModel clusterModel = new ClusterModel()
            .addNewTopic("A")
                .addToConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
                .addNewPartition(0)
                    .replicaOn(0, 1, 3)
                    .leader(0)
                    .isr(0, 1)
                .endPartition()
            .endTopic()
            .addNewTopic("B")
                .addToConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
                .addNewPartition(0)
                    .replicaOn(0, 1, 3)
                    .leader(1)
                    .isr(1)
                .endPartition()
            .endTopic()

            .addNBrokers(4);

        ka(clusterModel.buildAdminClient());
        assertEquals(Map.of(
                0, false,
                1, false,
                2, true,
                3, false),
                canRollBrokers(clusterModel.brokerIds(), Set.of()));
    }

    private Map<Integer, Boolean> canRollBrokers(Set<Integer> brokers,
                                                 Set<Integer> rollingBrokers) {
        return brokers.stream().collect(Collectors.toMap(
                brokerId -> brokerId,
                brokerId -> await(kafkaAvailability.canRoll(brokerId, rollingBrokers))));
    }

    @Test
    public void testAtMinIsr() {
        ClusterModel clusterModel = new ClusterModel()
            .addNewTopic("A")
                .addToConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
                .addNewPartition(0)
                    .replicaOn(0, 1)
                    .leader(0)
                    .isr(0, 1)
                .endPartition()
            .endTopic()
            .addNewTopic("B")
                .addToConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
                .addNewPartition(0)
                    .replicaOn(0, 1)
                    .leader(1)
                    .isr(0, 1)
                .endPartition()
            .endTopic()
            .addNBrokers(2);

        ka(clusterModel.buildAdminClient());
        for (Integer brokerId : clusterModel.brokerIds()) {
            boolean canRoll = await(kafkaAvailability.canRoll(brokerId, Set.of()));
            if (brokerId == 2) {
                assertTrue(canRoll,
                        "broker " + brokerId + " should be rollable, having no partitions");
            } else {
                assertTrue(canRoll,
                        "broker " + brokerId + " should be rollable, because although rolling it will impact availability minisr=|replicas|");
            }
        }
    }


    @Test
    public void testAboveMinIsr() {
        ClusterModel clusterModel = clusterWithTwoTopics(3, 3, List.of(0, 1, 2));
        ka(clusterModel.buildAdminClient());
        for (Integer brokerId : clusterModel.brokerIds()) {
            boolean canRoll = await(kafkaAvailability.canRoll(brokerId, Set.of()));
            Assertions.assertTrue(canRoll, "broker " + brokerId + " should be rollable, since it has no URP");
        }
    }

    @Test
    public void testAboveMinIsrWhileRestartingBrokers() {
        ClusterModel clusterModel = clusterWithTwoTopics(3, 2, List.of(0, 1, 2));
        ka(clusterModel.buildAdminClient());
        // We should be able to roll broker 1 because it's not in the ISR
        assertEquals(Map.of(0, false, 1, true, 2, false),
                canRollBrokers(clusterModel.brokerIds(), Set.of(1)));
    }


    @Test
    public void testMinIsrEqualsReplicas() {
        ClusterModel clusterModel = new ClusterModel()
                .addNewTopic("A")
                    .addToConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "3")
                    .addNewPartition(0)
                        .replicaOn(0, 1, 2)
                        .leader(0)
                        .isr(0, 1, 2)
                    .endPartition()
                .endTopic()
                .addNBrokers(3);

        ka(clusterModel.buildAdminClient());
        for (Integer brokerId : clusterModel.brokerIds()) {

            boolean canRoll = await(kafkaAvailability.canRoll(brokerId, Set.of()));
            assertTrue(canRoll,
                    "broker " + brokerId + " should be rollable, being minisr = 3, but only 3 replicas");
        }
    }

    @Test
    public void testMinIsrEqualsReplicasWithOfflineReplicas() {
        ClusterModel clusterModel = new ClusterModel()
                .addNewTopic("A")
                .addToConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "3")
                .addNewPartition(0)
                .replicaOn(0, 1, 2)
                .leader(0)
                .isr(0, 1)
                .endPartition()
                .endTopic()

                .addNBrokers(3);

        ka(clusterModel.buildAdminClient());
        for (Integer brokerId : clusterModel.brokerIds()) {
            boolean canRoll = await(kafkaAvailability.canRoll(brokerId, Set.of()));
            assertTrue(canRoll,
                    "broker " + brokerId + " should be rollable, being minisr = 3, but only 3 replicas");
        }
    }

    @Test
    public void testMinIsrMoreThanReplicas() {
        ClusterModel clusterModel = new ClusterModel()
                .addNewTopic("A")
                    .addToConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
                    .addNewPartition(0)
                        .replicaOn(0)
                        .leader(0)
                        .isr(0)
                    .endPartition()
                .endTopic()
                .addNBrokers(3);

        ka(clusterModel.buildAdminClient());
        for (Integer brokerId : clusterModel.brokerIds()) {
            boolean canRoll = await(kafkaAvailability.canRoll(brokerId, Set.of()));
            assertTrue(canRoll,
                    "broker " + brokerId + " should be rollable, being minisr = 2, but only 1 replicas");
        }
    }

    @Test
    public void testNoLeader() {
        ClusterModel clusterModel = new ClusterModel()
                .addNewTopic("A")
                    .addToConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                    .addNewPartition(0)
                        .replicaOn(0, 1, 2)
                        .leader(-1)
                        .isr(1, 2)
                    .endPartition()
                .endTopic()
                .addNewTopic("B")
                    .addToConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                    .addNewPartition(0)
                        .replicaOn(0, 1, 2)
                        .leader(-1)
                        .isr(0)
                    .endPartition()
                .endTopic()

                .addNBrokers(3);

        ka(clusterModel.buildAdminClient());
        for (Integer brokerId : clusterModel.brokerIds()) {
            boolean canRoll = await(kafkaAvailability.canRoll(brokerId, Set.of()));
            if (brokerId == 0) {
                assertFalse(canRoll,
                        "broker " + brokerId + " should not be rollable, because B/0 would be below min isr");
            } else {
                assertTrue(canRoll,
                        "broker " + brokerId + " should be rollable, being minisr = 1 and having two brokers in its isr");
            }
        }
    }

    @Test
    public void testNoMinIsr() {
        ClusterModel clusterModel = new ClusterModel()
                .addNewTopic("A")
                    .addNewPartition(0)
                        .replicaOn(0, 1, 2)
                        .leader(0)
                        .isr(0, 1, 2)
                    .endPartition()
                .endTopic()
                .addNewTopic("B")
                    .addNewPartition(0)
                        .replicaOn(0, 1, 2)
                        .leader(1)
                        .isr(1, 0, 2)
                    .endPartition()
                .endTopic()
                .addNBrokers(3);

        ka(clusterModel.buildAdminClient());
        for (Integer brokerId : clusterModel.brokerIds()) {
            boolean canRoll = await(kafkaAvailability.canRoll(brokerId, Set.of()));
            assertTrue(canRoll,
                    "broker " + brokerId + " should be rollable, being minisr = 1 and having two brokers in its isr");
        }
    }

    // TODO when AC throws various exceptions (e.g. UnknownTopicOrPartitionException)
    @Test
    public void testCanRollThrowsTimeoutExceptionWhenTopicsListThrowsException() {
        TimeoutException ex = new TimeoutException();
        ClusterModel clusterModel = new ClusterModel()
                .addNewTopic("A")
                    .addToConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                    .addNewPartition(0)
                        .replicaOn(0, 1, 2)
                        .leader(0)
                        .isr(0, 1, 2)
                    .endPartition()
                .endTopic()
                .addNewTopic("B")
                    .addToConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                    .addNewPartition(0)
                        .replicaOn(0, 1, 2)
                        .leader(1)
                        .isr(0, 1, 2)
                    .endPartition()
                .endTopic()

                .addNBrokers(3)
                .listTopicsResult(ex);

        ka(clusterModel.buildAdminClient());
        for (Integer brokerId : clusterModel.brokerIds()) {
            Throwable cause = awaitThrows(kafkaAvailability.canRoll(brokerId, Set.of()));
            assertTrue(cause instanceof AdminClientException);
            assertEquals("KafkaAvailability call of Admin.listTopics failed", cause.getMessage());
            assertEquals(ex, cause.getCause());
        }
    }

    @Test
    public void testCanRollThrowsExceptionWhenTopicDescribeThrows() {
        UnknownTopicOrPartitionException ex = new UnknownTopicOrPartitionException();
        ClusterModel clusterModel = new ClusterModel()
                .addNewTopic("A")
                    .addToConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                    .addNewPartition(0)
                        .replicaOn(0, 1, 2)
                        .leader(0)
                        .isr(0, 1, 2)
                    .endPartition()
                .endTopic()
                .addNewTopic("B")
                    .addToConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                    .addNewPartition(0)
                        .replicaOn(0, 1, 2)
                        .leader(1)
                        .isr(0, 1, 2)
                    .endPartition()
                .endTopic()

                .addNBrokers(3)
                .describeTopicsResult("A", ex);

        ka(clusterModel.buildAdminClient());
        for (Integer brokerId : clusterModel.brokerIds()) {
            Throwable cause = awaitThrows(kafkaAvailability.canRoll(brokerId, Set.of()));
            assertTrue(cause instanceof AdminClientException);
            assertEquals("KafkaAvailability call of Admin.describeTopics failed", cause.getMessage());
            assertEquals(ex, cause.getCause());
        }
    }

    @Test
    public void testCanRollThrowsExceptionWhenDescribeConfigsThrows() {
        UnknownTopicOrPartitionException ex = new UnknownTopicOrPartitionException();
        ClusterModel clusterModel = new ClusterModel()
                .addNewTopic("A")
                    .addToConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                    .addNewPartition(0)
                        .replicaOn(0, 1, 2)
                        .leader(0)
                        .isr(0, 1, 2)
                    .endPartition()
                .endTopic()
                .addNewTopic("B")
                    .addToConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                    .addNewPartition(0)
                        .replicaOn(0, 1, 2)
                        .leader(1)
                        .isr(0, 1, 2)
                    .endPartition()
                .endTopic()

                .addNBrokers(3)
                .describeConfigsResult(new ConfigResource(ConfigResource.Type.TOPIC, "A"), ex);

        ka(clusterModel.buildAdminClient());
        for (Integer brokerId : clusterModel.brokerIds()) {
            if (brokerId <= 2) {
                Throwable cause = awaitThrows(kafkaAvailability.canRoll(brokerId, Set.of()));
                assertTrue(cause instanceof AdminClientException);
                assertEquals("KafkaAvailability call of Admin.describeConfigs (topics) failed", cause.getMessage());
                assertEquals(ex, cause.getCause());
            } else {
                boolean canRoll = await(kafkaAvailability.canRoll(brokerId, Set.of()));
                // TODO assertion
                assertTrue(canRoll);
            }
        }
    }

    @Test
    public void testPartitionsWithPreferredButNotCurrentLeader() {
        ClusterModel clusterModel = new ClusterModel()
            .addNewTopic("A")
                .addToConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                .addNewPartition(0)
                    .replicaOn(0, 1, 2) // broker 0 preferred...
                    .leader(0) // ...and current
                    .isr(0, 1, 2)
                .endPartition()
            .endTopic()
            .addNewTopic("B")
                .addToConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                .addNewPartition(0)
                    .replicaOn(0, 1, 2) // broker 0 preferred...
                    .leader(1) // ...but not current
                    .isr(0, 1, 2)
                .endPartition()
                .addNewPartition(1)
                    .replicaOn(2, 1, 0) // broker 2 preferred...
                    .leader(1) // ...but not current
                    .isr(0, 1, 2)
                .endPartition()
            .endTopic()
            .addNBrokers(3);

        ka(clusterModel.buildAdminClient());
        var map = clusterModel.brokerIds().stream()
                .collect(Collectors.toMap(
                        brokerId -> brokerId,
                        broker -> await(kafkaAvailability.partitionsWithPreferredButNotCurrentLeader(broker))));
        assertEquals(Map.of(
                0, Set.of(new TopicPartition("B", 0)),
                1, Set.of(),
                2, Set.of(new TopicPartition("B", 1))), map);
    }

    @Test
    public void testPartitionsWithPreferredButNotCurrentLeader_describeError() {
        UnknownTopicOrPartitionException ex = new UnknownTopicOrPartitionException();
        ClusterModel clusterModel = new ClusterModel()
                .addNewTopic("A")
                .addToConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                .addNewPartition(0)
                .replicaOn(0, 1, 2) // broker 0 preferred...
                .leader(0) // ...and current
                .isr(0, 1, 2)
                .endPartition()
                .endTopic()
                .addNewTopic("B")
                .addToConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                .addNewPartition(0)
                .replicaOn(0, 1, 2) // broker 0 preferred...
                .leader(1) // ...but not current
                .isr(0, 1, 2)
                .endPartition()
                .addNewPartition(1)
                .replicaOn(2, 1, 0) // broker 2 preferred...
                .leader(1) // ...but not current
                .isr(0, 1, 2)
                .endPartition()
                .endTopic()

                .addNBrokers(3)
                .describeTopicsResult("B", ex);

        ka(clusterModel.buildAdminClient());
        for (var brokerId : clusterModel.brokerIds()) {
            var cause = awaitThrows(kafkaAvailability.partitionsWithPreferredButNotCurrentLeader(brokerId));
            assertTrue(cause instanceof AdminClientException);
            assertEquals("KafkaAvailability call of Admin.describeTopics failed", cause.getMessage());
            assertEquals(ex, cause.getCause());
        }
    }
}
