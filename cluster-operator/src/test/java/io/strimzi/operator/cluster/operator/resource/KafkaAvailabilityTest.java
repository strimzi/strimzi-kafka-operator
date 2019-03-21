/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class KafkaAvailabilityTest {


    class KSB {
        class TSB {
            class PSB {
                private final Integer id;
                private int[] isr = new int[0];
                private Integer leader;
                private int[] replicaOn = new int[0];

                public PSB(Integer p) {
                    this.id = p;
                }
                PSB replicaOn(int... broker) {
                    addBroker(broker);
                    this.replicaOn = broker;
                    return this;
                }

                PSB leader(int broker) {
                    addBroker(broker);
                    this.leader = broker;
                    return this;
                }

                PSB isr(int... broker) {
                    addBroker(broker);
                    this.isr = broker;
                    return this;
                }
                TSB endPartition() {
                    if (this.leader != null) {
                        if (!IntStream.of(this.replicaOn).anyMatch(x -> x == this.leader)) {
                            throw new RuntimeException("Leader must be one of the replicas");
                        }
                        if (IntStream.of(this.isr).anyMatch(x -> x == this.leader)) {
                            throw new RuntimeException("ISR must not include the leader");
                        }
                    }
                    if (!IntStream.of(this.isr).allMatch(x -> IntStream.of(this.replicaOn).anyMatch(y -> x == y))) {
                        throw new RuntimeException("ISR must be a subset of the replicas");
                    }
                    return TSB.this;
                }
            }
            private final String name;
            private final boolean internal;
            private Map<String, String> configs = new HashMap<>();
            private Map<Integer, PSB> partitions = new HashMap<>();

            public TSB(String name, boolean internal) {
                this.name = name;
                this.internal = internal;
            }

            TSB addToConfig(String config, String value) {
                configs.put(config, value);
                return this;
            }
            PSB partition(int partition) {
                return partitions.computeIfAbsent(partition, p -> new PSB(p));
            }


            KSB endTopic() {
                return KSB.this;
            }
        }

        class BSB {

            private int id;

            public BSB(int id) {
                this.id = id;
                KSB.this.nodes.put(id, new Node(id, "localhost", 1234 + id));
            }

            KSB endBroker() {
                return KSB.this;
            }
        }

        private Map<String, TSB> topics = new HashMap<>();
        private Map<Integer, BSB> brokers = new HashMap<>();
        private Map<Integer, Node> nodes = new HashMap<>();

        TSB topic(String name, boolean internal) {
            return topics.computeIfAbsent(name, n -> new TSB(n, internal));
        }

        KSB addBroker(int... ids) {
            for (int id : ids) {
                brokers.computeIfAbsent(id, i -> new BSB(i));
            }
            return this;
        }

        ListTopicsResult ltr() {
            ListTopicsResult ltr = mock(ListTopicsResult.class);
            when(ltr.names()).thenReturn(KafkaFuture.completedFuture(new HashSet<>(topics.keySet())));
            return ltr;
        }

        DescribeTopicsResult dtr() {
            Map<String, TopicDescription> tds = topics.entrySet().stream().collect(Collectors.toMap(
                e -> e.getKey(),
                e -> {
                    TSB tsb = e.getValue();
                    return new TopicDescription(tsb.name, tsb.internal,
                            tsb.partitions.entrySet().stream().map(e1 -> {
                                TSB.PSB psb = e1.getValue();
                                return new TopicPartitionInfo(psb.id,
                                        psb.leader != null ? node(psb.leader) : Node.noNode(),
                                        Arrays.stream(psb.replicaOn).boxed().map(broker -> node(broker)).collect(Collectors.toList()),
                                        Arrays.stream(psb.isr).boxed().map(broker -> node(broker)).collect(Collectors.toList()));
                            }).collect(Collectors.toList()));
                }
            ));
            DescribeTopicsResult dtr = mock(DescribeTopicsResult.class);
            when(dtr.all()).thenReturn(KafkaFuture.completedFuture(tds));
            return dtr;
        }

        private Node node(int id) {
            return nodes.computeIfAbsent(id, x -> {
                throw new RuntimeException("Unknown node " + id);
            });
        }

        DescribeConfigsResult dcr(Collection<ConfigResource> argument) {
            Map<ConfigResource, Config> result = new HashMap<>();
            for (ConfigResource cr : argument) {
                List<ConfigEntry> entries = new ArrayList<>();
                for (Map.Entry<String, String> e : topics.get(cr.name()).configs.entrySet()) {
                    ConfigEntry ce = new ConfigEntry(e.getKey(), e.getValue());
                    entries.add(ce);
                }
                result.put(cr, new Config(entries));
            }
            DescribeConfigsResult dcr = mock(DescribeConfigsResult.class);
            when(dcr.all()).thenReturn(KafkaFuture.completedFuture(result));
            return dcr;
        }

        AdminClient ac() {
            AdminClient ac = mock(AdminClient.class);

            ListTopicsResult ltr = ltr();
            when(ac.listTopics(any())).thenReturn(ltr);

            DescribeTopicsResult dtr = dtr();
            when(ac.describeTopics(any())).thenReturn(dtr);

            when(ac.describeConfigs(any())).thenAnswer(invocation -> dcr(invocation.getArgument(0)));
            return ac;
        }
    }

    @Test
    public void belowMinIsr(TestContext context) {
        KSB ksb = new KSB().topic("A", false)
                .addToConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
                .partition(0)
                    .replicaOn(0, 1)
                    .leader(0)
                    .isr(1)
                .endPartition()

                .endTopic()
                .topic("B", false)
                .addToConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
                .partition(0)
                    .replicaOn(0, 1)
                    .leader(1)
                    .isr()
                .endPartition()
                .endTopic()

                .addBroker(3);

        KafkaAvailability kafkaAvailability = new KafkaAvailability(ksb.ac());

        for (Integer brokerId : ksb.brokers.keySet()) {
            Async async = context.async();

            kafkaAvailability.canRoll(brokerId).setHandler(ar -> {
                if (ar.failed()) {
                    context.fail(ar.cause());
                } else {
                    if (brokerId == 3) {
                        context.assertTrue(ar.result(),
                                "broker " + brokerId + " should be rollable, having no partitions");
                    } else {
                        context.assertFalse(ar.result(),
                                "broker " + brokerId + " should not be rollable, being minisr = 2 and it's only replicated on two brokers");
                    }
                }
                async.complete();
            });
        }
    }

    @Test
    public void atMinIsr(TestContext context) {
        KSB ksb = new KSB().topic("A", false)
                .addToConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                .partition(0)
                .replicaOn(0, 1)
                .leader(0)
                .isr(1)
                .endPartition()

                .endTopic()
                .topic("B", false)
                .addToConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                .partition(0)
                .replicaOn(0, 1)
                .leader(1)
                .isr(0)
                .endPartition()

                .endTopic()

                .addBroker(2);

        KafkaAvailability kafkaAvailability = new KafkaAvailability(ksb.ac());

        for (Integer brokerId : ksb.brokers.keySet()) {
            Async async = context.async();

            kafkaAvailability.canRoll(brokerId).setHandler(ar -> {
                if (ar.failed()) {
                    context.fail(ar.cause());
                } else {
                    if (brokerId == 2) {
                        context.assertTrue(ar.result(),
                                "broker " + brokerId + " should be rollable, having no partitions");
                    } else {
                        context.assertFalse(ar.result(),
                                "broker " + brokerId + " should not be rollable, being minisr = 2 and it's only replicated on two brokers");
                    }
                }
                async.complete();
            });
        }
    }

    @Test
    public void aboveMinIsr(TestContext context) {
        KSB ksb = new KSB().topic("A", false)
                .addToConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                .partition(0)
                    .replicaOn(0, 1, 2)
                    .leader(0)
                    .isr(1, 2)
                .endPartition()

                .endTopic()
                .topic("B", false)
                .addToConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                .partition(0)
                    .replicaOn(0, 1, 2)
                    .leader(1)
                    .isr(0, 2)
                .endPartition()
                .endTopic()

                .addBroker(3);

        KafkaAvailability kafkaAvailability = new KafkaAvailability(ksb.ac());

        for (Integer brokerId : ksb.brokers.keySet()) {
            Async async = context.async();

            kafkaAvailability.canRoll(brokerId).setHandler(ar -> {
                if (ar.failed()) {
                    context.fail(ar.cause());
                } else {
                    context.assertTrue(ar.result(),
                            "broker " + brokerId + " should be rollable, being minisr = 1 and having two brokers in its isr");
                }
                async.complete();
            });
        }
    }

//    @Test
//    public void noLeader(TestContext context) {
//        KSB ksb = new KSB().topic("A", false)
//                .addToConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
//                .partition(0)
//                .replicaOn(0, 1, 2)
//                //.leader(0)
//                .isr(1, 2)
//                .endPartition()
//
//                .endTopic()
//                .topic("B", false)
//                .addToConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
//                .partition(0)
//                .replicaOn(0, 1, 2)
//                //.leader(1)
//                .isr(0, 2)
//                .endPartition()
//                .endTopic()
//
//                .addBroker(3);
//
//        KafkaSorted kafkaSorted = new KafkaSorted(ksb.ac());
//
//        for (Integer brokerId : ksb.brokers.keySet()) {
//            Async async = context.async();
//
//            kafkaSorted.canRoll(brokerId).setHandler(ar -> {
//                if (ar.failed()) {
//                    context.fail(ar.cause());
//                } else {
//                    context.assertTrue(ar.result(),
//                            "broker " + brokerId + " should be rollable, being minisr = 1 and having two brokers in its isr");
//                }
//                async.complete();
//            });
//        }
//    }

    @Test
    public void noMinIsr(TestContext context) {
        KSB ksb = new KSB().topic("A", false)

                .partition(0)
                .replicaOn(0, 1, 2)
                .leader(0)
                .isr(1, 2)
                .endPartition()

                .endTopic()
                .topic("B", false)

                .partition(0)
                .replicaOn(0, 1, 2)
                .leader(1)
                .isr(0, 2)
                .endPartition()
                .endTopic()

                .addBroker(3);

        KafkaAvailability kafkaAvailability = new KafkaAvailability(ksb.ac());

        for (Integer brokerId : ksb.brokers.keySet()) {
            Async async = context.async();

            kafkaAvailability.canRoll(brokerId).setHandler(ar -> {
                if (ar.failed()) {
                    context.fail(ar.cause());
                } else {
                    context.assertTrue(ar.result(),
                            "broker " + brokerId + " should be rollable, being minisr = 1 and having two brokers in its isr");
                }
                async.complete();
            });
        }
    }

    // TODO Test with no min.in.sync.replicas config
    // TODO when AC throws various exceptions (e.g. UnknownTopicOrPartitionException)

}
