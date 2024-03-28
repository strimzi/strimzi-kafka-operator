/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaList;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.operator.cluster.operator.resource.kubernetes.CrdOperator;
import io.strimzi.test.mockkube3.MockKube3;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ReconnectingWatcherMockTest {
    private static final Logger LOGGER = LogManager.getLogger(ReconnectingWatcherMockTest.class);

    private static final String NAMESPACE = "my-namespace";
    private static final String NAMESPACE2 = "my-namespace2";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final String CLUSTER_NAME2 = "my-cluster2";

    private static KubernetesClient client;
    private static MockKube3 mockKube;

    private Vertx vertx;

    @BeforeAll
    public static void beforeAll() {
        // Configure the Kubernetes Mock
        mockKube = new MockKube3.MockKube3Builder()
                .withKafkaCrd()
                .withNamespaces(NAMESPACE, NAMESPACE2)
                .build();
        mockKube.start();
        client = mockKube.client();
    }

    @AfterAll
    public static void afterAll() {
        mockKube.stop();
    }

    @BeforeEach
    public void beforeEach() {
        vertx = Vertx.vertx();
    }

    @AfterEach
    public void afterEach() {
        vertx.close();
    }

    @Test
    public void testWatch() throws InterruptedException {
        CrdOperator<KubernetesClient, Kafka, KafkaList> kafkaOps = new CrdOperator<>(vertx, client, Kafka.class, KafkaList.class, Kafka.RESOURCE_KIND);

        CountDownLatch addedLatch = new CountDownLatch(1);
        CountDownLatch modifiedLatch = new CountDownLatch(1);
        CountDownLatch deletedLatch = new CountDownLatch(1);
        AtomicInteger eventCounter = new AtomicInteger(0);

        ReconnectingWatcher<Kafka> watcher = new ReconnectingWatcher<>(kafkaOps, Kafka.RESOURCE_KIND, NAMESPACE, null, (a, r) -> {
            LOGGER.info("Received event {} about resource {} in namespace {}", a, r.getMetadata().getName(), r.getMetadata().getNamespace());

            switch (a)  {
                case ADDED -> {
                    addedLatch.countDown();
                    eventCounter.incrementAndGet();
                }
                case MODIFIED -> {
                    modifiedLatch.countDown();
                    eventCounter.incrementAndGet();
                }
                case DELETED -> {
                    deletedLatch.countDown();
                    eventCounter.incrementAndGet();
                }
            }
        });

        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(1)
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("plain")
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                                .build())
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endKafka()
                .endSpec()
                .build();

        Crds.kafkaOperation(client).inNamespace(NAMESPACE).resource(kafka).create();
        Crds.kafkaOperation(client).inNamespace(NAMESPACE2).resource(kafka).create();
        boolean latched = addedLatch.await(5_000, TimeUnit.MILLISECONDS);
        assertThat(latched, is(true));

        Crds.kafkaOperation(client).inNamespace(NAMESPACE).withName(CLUSTER_NAME).edit(k -> new KafkaBuilder(k).editSpec().editKafka().withReplicas(3).endKafka().endSpec().build());
        Crds.kafkaOperation(client).inNamespace(NAMESPACE2).withName(CLUSTER_NAME).edit(k -> new KafkaBuilder(k).editSpec().editKafka().withReplicas(3).endKafka().endSpec().build());
        latched = modifiedLatch.await(5_000, TimeUnit.MILLISECONDS);
        assertThat(latched, is(true));

        Crds.kafkaOperation(client).inNamespace(NAMESPACE).withName(CLUSTER_NAME).delete();
        Crds.kafkaOperation(client).inNamespace(NAMESPACE2).withName(CLUSTER_NAME).delete();
        latched = deletedLatch.await(5_000, TimeUnit.MILLISECONDS);
        assertThat(latched, is(true));

        assertThat(eventCounter.get(), is(3));

        watcher.close();
    }

    @Test
    public void testWatchAllNamespaces() throws InterruptedException {
        CrdOperator<KubernetesClient, Kafka, KafkaList> kafkaOps = new CrdOperator<>(vertx, client, Kafka.class, KafkaList.class, Kafka.RESOURCE_KIND);

        CountDownLatch addedLatch = new CountDownLatch(2);
        CountDownLatch modifiedLatch = new CountDownLatch(2);
        CountDownLatch deletedLatch = new CountDownLatch(2);
        AtomicInteger eventCounter = new AtomicInteger(0);

        ReconnectingWatcher<Kafka> watcher = new ReconnectingWatcher<>(kafkaOps, Kafka.RESOURCE_KIND, "*", null, (a, r) -> {
            LOGGER.info("Received event {} about resource {} in namespace {}", a, r.getMetadata().getName(), r.getMetadata().getNamespace());

            switch (a)  {
                case ADDED -> {
                    addedLatch.countDown();
                    eventCounter.incrementAndGet();
                }
                case MODIFIED -> {
                    modifiedLatch.countDown();
                    eventCounter.incrementAndGet();
                }
                case DELETED -> {
                    deletedLatch.countDown();
                    eventCounter.incrementAndGet();
                }
            }
        });

        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(1)
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("plain")
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                                .build())
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endKafka()
                .endSpec()
                .build();

        Crds.kafkaOperation(client).inNamespace(NAMESPACE).resource(kafka).create();
        Crds.kafkaOperation(client).inNamespace(NAMESPACE2).resource(kafka).create();
        boolean latched = addedLatch.await(5_000, TimeUnit.MILLISECONDS);
        assertThat(latched, is(true));

        Crds.kafkaOperation(client).inNamespace(NAMESPACE).withName(CLUSTER_NAME).edit(k -> new KafkaBuilder(k).editSpec().editKafka().withReplicas(3).endKafka().endSpec().build());
        Crds.kafkaOperation(client).inNamespace(NAMESPACE2).withName(CLUSTER_NAME).edit(k -> new KafkaBuilder(k).editSpec().editKafka().withReplicas(3).endKafka().endSpec().build());
        latched = modifiedLatch.await(5_000, TimeUnit.MILLISECONDS);
        assertThat(latched, is(true));

        Crds.kafkaOperation(client).inNamespace(NAMESPACE).withName(CLUSTER_NAME).delete();
        Crds.kafkaOperation(client).inNamespace(NAMESPACE2).withName(CLUSTER_NAME).delete();
        latched = deletedLatch.await(5_000, TimeUnit.MILLISECONDS);
        assertThat(latched, is(true));

        assertThat(eventCounter.get(), is(6));

        watcher.close();
    }

    @Test
    public void testWatchWithSelector() throws InterruptedException {
        CrdOperator<KubernetesClient, Kafka, KafkaList> kafkaOps = new CrdOperator<>(vertx, client, Kafka.class, KafkaList.class, Kafka.RESOURCE_KIND);

        CountDownLatch addedLatch = new CountDownLatch(1);
        CountDownLatch modifiedLatch = new CountDownLatch(1);
        CountDownLatch deletedLatch = new CountDownLatch(1);
        AtomicInteger addedCounter = new AtomicInteger(0);
        AtomicInteger modifiedCounter = new AtomicInteger(0);
        AtomicInteger deletedCounter = new AtomicInteger(0);

        ReconnectingWatcher<Kafka> watcher = new ReconnectingWatcher<>(kafkaOps, Kafka.RESOURCE_KIND, NAMESPACE, new LabelSelectorBuilder().withMatchLabels(Map.of("selector", "matching")).build(), (a, r) -> {
            LOGGER.info("Received event {} about resource {} in namespace {}", a, r.getMetadata().getName(), r.getMetadata().getNamespace());

            switch (a)  {
                case ADDED -> {
                    addedLatch.countDown();
                    addedCounter.incrementAndGet();
                }
                case MODIFIED -> {
                    modifiedLatch.countDown();
                    modifiedCounter.incrementAndGet();
                }
                case DELETED -> {
                    deletedLatch.countDown();
                    deletedCounter.incrementAndGet();
                }
            }
        });

        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withLabels(Map.of("selector", "matching"))
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(1)
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("plain")
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                                .build())
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endKafka()
                .endSpec()
                .build();

        Kafka kafka2 = new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME2)
                    .withLabels(Map.of("selector", "not-matching"))
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(1)
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("plain")
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                                .build())
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endKafka()
                .endSpec()
                .build();

        Crds.kafkaOperation(client).inNamespace(NAMESPACE).resource(kafka).create();
        Crds.kafkaOperation(client).inNamespace(NAMESPACE).resource(kafka2).create();
        boolean latched = addedLatch.await(5_000, TimeUnit.MILLISECONDS);
        assertThat(latched, is(true));

        Crds.kafkaOperation(client).inNamespace(NAMESPACE).withName(CLUSTER_NAME).edit(k -> new KafkaBuilder(k).editSpec().editKafka().withReplicas(3).endKafka().endSpec().build());
        Crds.kafkaOperation(client).inNamespace(NAMESPACE).withName(CLUSTER_NAME2).edit(k -> new KafkaBuilder(k).editSpec().editKafka().withReplicas(3).endKafka().endSpec().build());
        latched = modifiedLatch.await(5_000, TimeUnit.MILLISECONDS);
        assertThat(latched, is(true));

        Crds.kafkaOperation(client).inNamespace(NAMESPACE).withName(CLUSTER_NAME).delete();
        Crds.kafkaOperation(client).inNamespace(NAMESPACE).withName(CLUSTER_NAME2).delete();
        latched = deletedLatch.await(5_000, TimeUnit.MILLISECONDS);
        assertThat(latched, is(true));

        assertThat(addedCounter.get(), is(1));
        assertThat(modifiedCounter.get(), is(1));
        assertThat(deletedCounter.get(), is(1));

        watcher.close();
    }
}
