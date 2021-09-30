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
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaAvailabilityTest {

    private static Vertx vertx;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
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
        future.onComplete(ar -> {
            latch.countDown();
        });
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
        future.onComplete(ar -> {
            latch.countDown();
        });
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
        ClusterModel clusterModel = new ClusterModel()
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
        return clusterModel;
    }

    @Test
    public void testBelowMinIsr() throws InterruptedException {
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

        KafkaAvailability kafkaAvailability = new KafkaAvailability(Reconciliation.DUMMY_RECONCILIATION, vertx, clusterModel.buildAdminClient());

        assertEquals(Map.of(
                0, false,
                1, false,
                2, true,
                3, false),
                canRollBrokers(kafkaAvailability, clusterModel.brokerIds(), Set.of()));
//        for (Integer brokerId : brokers) {
//            boolean canRoll = await(kafkaAvailability.canRoll(brokerId, Set.of()));
//            if (brokerId == 4) {
//                assertTrue(canRoll,
//                        "broker " + brokerId + " should be rollable, having no partitions");
//            } else {
//                assertFalse(canRoll,
//                        "broker " + brokerId + " should not be rollable, being minisr = 2 and it's only replicated on two brokers");
//            }
//        }
    }

    private Map<Integer, Boolean> canRollBrokers(KafkaAvailability kafkaAvailability,
                                                 Set<Integer> brokers,
                                                 Set<Integer> rollingBrokers) {
        return brokers.stream().collect(Collectors.toMap(brokerId -> brokerId, brokerId -> await(kafkaAvailability.canRoll(brokerId, rollingBrokers))));
    }

    @Test
    public void testAtMinIsr() throws InterruptedException {
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

        KafkaAvailability kafkaAvailability = new KafkaAvailability(Reconciliation.DUMMY_RECONCILIATION, vertx, clusterModel.buildAdminClient());

        Set<Integer> brokers = clusterModel.brokerIds();

        for (Integer brokerId : brokers) {
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
    public void testAboveMinIsr() throws InterruptedException {
        ClusterModel clusterModel = clusterWithTwoTopics(3, 3, List.of(0, 1, 2));

        KafkaAvailability kafkaAvailability = new KafkaAvailability(Reconciliation.DUMMY_RECONCILIATION, vertx, clusterModel.buildAdminClient());

        Set<Integer> brokers = clusterModel.brokerIds();
        for (Integer brokerId1 : brokers) {
            boolean canRoll1 = await(kafkaAvailability.canRoll(brokerId1, Set.of()));
            ((BiConsumer<Integer, Boolean>) (brokerId, canRoll) -> Assertions.assertTrue(canRoll, "broker " + brokerId + " should be rollable, since it has no URP")).accept(brokerId1, canRoll1);
        }
    }

    @Test
    public void testAboveMinIsrWhileRestartingBrokers() throws InterruptedException {
        ClusterModel clusterModel = clusterWithTwoTopics(3, 3, List.of(0, 1, 2));

        KafkaAvailability kafkaAvailability = new KafkaAvailability(Reconciliation.DUMMY_RECONCILIATION, vertx, clusterModel.buildAdminClient());

        assertEquals(Map.of(0, true, 1, false, 2, true),
                canRollBrokers(kafkaAvailability, clusterModel.brokerIds(), Set.of(1)));

//        Set<Integer> brokers = adminMockBuilder.brokers();
//        for (Integer brokerId1 : brokers) {
//            boolean canRoll1 = await(kafkaAvailability.canRoll(brokerId1, Set.of(1)));
//            ((BiConsumer<Integer, Boolean>) (brokerId, canRoll) -> {
//                if (brokerId != 1) {
//                    Assertions.assertTrue(canRoll,
//                            "broker " + brokerId + " should be rollable, since it has no URP");
//                } else {
//                    Assertions.assertFalse(canRoll,
//                            "broker " + brokerId + " should not be rollable, because while Kafka says no URP, we're already rolling it");
//                }
//            }).accept(brokerId1, canRoll1);
//        }
    }


    @Test
    public void testMinIsrEqualsReplicas() throws InterruptedException {
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

        KafkaAvailability kafkaAvailability = new KafkaAvailability(Reconciliation.DUMMY_RECONCILIATION, vertx, clusterModel.buildAdminClient());

        Set<Integer> brokers = clusterModel.brokerIds();

        for (Integer brokerId : brokers) {
            boolean canRoll = await(kafkaAvailability.canRoll(brokerId, Set.of()));
            assertTrue(canRoll,
                    "broker " + brokerId + " should be rollable, being minisr = 3, but only 3 replicas");
        }
    }

    @Test
    public void testMinIsrEqualsReplicasWithOfflineReplicas() throws InterruptedException {
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

        KafkaAvailability kafkaAvailability = new KafkaAvailability(Reconciliation.DUMMY_RECONCILIATION, vertx, clusterModel.buildAdminClient());

        Set<Integer> brokers = clusterModel.brokerIds();
        for (Integer brokerId : brokers) {
            boolean canRoll = await(kafkaAvailability.canRoll(brokerId, Set.of()));
            assertTrue(canRoll,
                    "broker " + brokerId + " should be rollable, being minisr = 3, but only 3 replicas");
        }
    }

    @Test
    public void testMinIsrMoreThanReplicas() throws InterruptedException {
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

        KafkaAvailability kafkaAvailability = new KafkaAvailability(Reconciliation.DUMMY_RECONCILIATION, vertx, clusterModel.buildAdminClient());

        Set<Integer> brokers = clusterModel.brokerIds();

        for (Integer brokerId : brokers) {
            boolean canRoll = await(kafkaAvailability.canRoll(brokerId, Set.of()));
            assertTrue(canRoll,
                    "broker " + brokerId + " should be rollable, being minisr = 2, but only 1 replicas");
        }
    }

    @Test
    public void testNoLeader() throws InterruptedException {
        ClusterModel clusterModel = new ClusterModel()
                .addNewTopic("A")
                    .addToConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                    .addNewPartition(0)
                        .replicaOn(0, 1, 2)
                        //.leader(0)
                        .isr(1, 2)
                    .endPartition()
                .endTopic()
                .addNewTopic("B")
                    .addToConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                    .addNewPartition(0)
                        .replicaOn(0, 1, 2)
                        //.leader(1)
                        .isr(0)
                    .endPartition()
                .endTopic()

                .addNBrokers(3);

        KafkaAvailability kafkaSorted = new KafkaAvailability(Reconciliation.DUMMY_RECONCILIATION, vertx, clusterModel.buildAdminClient());

        Set<Integer> brokers = clusterModel.brokerIds();
        for (Integer brokerId : brokers) {
            boolean canRoll = await(kafkaSorted.canRoll(brokerId, Set.of()));
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
    public void testNoMinIsr() throws InterruptedException {
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

        KafkaAvailability kafkaAvailability = new KafkaAvailability(Reconciliation.DUMMY_RECONCILIATION, vertx, clusterModel.buildAdminClient());

        Set<Integer> brokers = clusterModel.brokerIds();
        for (Integer brokerId : brokers) {
            boolean canRoll = await(kafkaAvailability.canRoll(brokerId, Set.of()));
            assertTrue(canRoll,
                    "broker " + brokerId + " should be rollable, being minisr = 1 and having two brokers in its isr");
        }
    }

    // TODO when AC throws various exceptions (e.g. UnknownTopicOrPartitionException)
    @Test
    public void testCanRollThrowsTimeoutExceptionWhenTopicsListThrowsException() throws InterruptedException {
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
                .listTopicsResult(new TimeoutException());

        KafkaAvailability kafkaAvailability = new KafkaAvailability(Reconciliation.DUMMY_RECONCILIATION, vertx, clusterModel.buildAdminClient());

        Set<Integer> brokers = clusterModel.brokerIds();
        for (Integer brokerId : brokers) {
            Throwable cause = awaitThrows(kafkaAvailability.canRoll(brokerId, Set.of()));
            assertThat(cause, instanceOf(TimeoutException.class));
        }
    }

    @Test
    public void testCanRollThrowsExceptionWhenTopicDescribeThrows() throws InterruptedException {
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
                .describeTopicsResult("A", new UnknownTopicOrPartitionException());

        KafkaAvailability kafkaAvailability = new KafkaAvailability(Reconciliation.DUMMY_RECONCILIATION, vertx, clusterModel.buildAdminClient());

        Set<Integer> brokers = clusterModel.brokerIds();
        for (Integer brokerId : brokers) {
            Throwable cause = awaitThrows(kafkaAvailability.canRoll(brokerId, Set.of()));
            assertThat(cause, instanceOf(UnknownTopicOrPartitionException.class));
        }
    }

    @Test
    public void testCanRollThrowsExceptionWhenDescribeConfigsThrows() throws InterruptedException {
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
                .describeConfigsResult(new ConfigResource(ConfigResource.Type.TOPIC, "A"), new UnknownTopicOrPartitionException());

        KafkaAvailability kafkaAvailability = new KafkaAvailability(Reconciliation.DUMMY_RECONCILIATION, vertx, clusterModel.buildAdminClient());

        Set<Integer> brokers = clusterModel.brokerIds();
        for (Integer brokerId : brokers) {
            if (brokerId <= 2) {
                Throwable cause = awaitThrows(kafkaAvailability.canRoll(brokerId, Set.of()));
                assertThat(cause, instanceOf(UnknownTopicOrPartitionException.class));
            } else {
                boolean canRoll = await(kafkaAvailability.canRoll(brokerId, Set.of()));
            }
        }
    }
}
