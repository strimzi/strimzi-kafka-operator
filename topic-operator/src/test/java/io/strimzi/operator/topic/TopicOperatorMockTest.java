/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.debezium.kafka.KafkaCluster;
import io.debezium.kafka.ZookeeperServer;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaTopicList;
import io.strimzi.api.kafka.model.DoneableKafkaTopic;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaTopicBuilder;
import io.strimzi.test.mockkube.MockKube;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static io.strimzi.test.TestUtils.waitFor;
import static java.util.Arrays.asList;

@RunWith(VertxUnitRunner.class)
public class TopicOperatorMockTest {

    private static final Logger LOGGER = LogManager.getLogger(TopicOperatorMockTest.class);

    private KubernetesClient kubeClient;
    private Session session;
    private KafkaCluster kafkaCluster;
    private Vertx vertx;
    private String deploymentId;
    private AdminClient adminClient;
    private TopicConfigsWatcher topicsConfigWatcher;
    private ZkTopicWatcher topicWatcher;
    private ZkTopicsWatcher topicsWatcher;

    // TODO this is all in common with TOIT, so factor out a common base class

    @After
    public void tearDown() {
        if (vertx != null && deploymentId != null) {
            vertx.undeploy(deploymentId);
        }
        if (adminClient != null) {
            adminClient.close();
        }
        if (kafkaCluster != null) {
            kafkaCluster.shutdown();
        }
    }

    @Before
    public void createMockKube(TestContext context) throws Exception {
        Assume.assumeTrue("This test is flaky on Travis, for unknown reasons",
                System.getenv("TRAVIS") == null);
        vertx = Vertx.vertx();
        MockKube mockKube = new MockKube();
        mockKube.withCustomResourceDefinition(Crds.topic(),
                        KafkaTopic.class, KafkaTopicList.class, DoneableKafkaTopic.class);
        kubeClient = mockKube.build();

        kafkaCluster = new KafkaCluster();
        kafkaCluster.addBrokers(1);
        kafkaCluster.deleteDataPriorToStartup(true);
        kafkaCluster.deleteDataUponShutdown(true);
        kafkaCluster.usingDirectory(Files.createTempDirectory("operator-integration-test").toFile());
        kafkaCluster.startup();

        Properties p = new Properties();
        p.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.brokerList());
        adminClient = AdminClient.create(p);

        Map<String, String> m = new HashMap();
        m.put(io.strimzi.operator.topic.Config.KAFKA_BOOTSTRAP_SERVERS.key, kafkaCluster.brokerList());
        m.put(io.strimzi.operator.topic.Config.ZOOKEEPER_CONNECT.key, "localhost:" + zkPort(kafkaCluster));
        m.put(io.strimzi.operator.topic.Config.ZOOKEEPER_CONNECTION_TIMEOUT_MS.key, "30000");
        m.put(io.strimzi.operator.topic.Config.NAMESPACE.key, "myproject");
        m.put(io.strimzi.operator.topic.Config.FULL_RECONCILIATION_INTERVAL_MS.key, "10000");
        session = new Session(kubeClient, new io.strimzi.operator.topic.Config(m));

        Async async = context.async();
        vertx.deployVerticle(session, ar -> {
            if (ar.succeeded()) {
                deploymentId = ar.result();
                topicsConfigWatcher = session.topicConfigsWatcher;
                topicWatcher = session.topicWatcher;
                topicsWatcher = session.topicsWatcher;
                async.complete();
            } else {
                ar.cause().printStackTrace();
                context.fail("Failed to deploy session");
            }
        });
        async.awaitSuccess();

        int timeout = 30_000;

        waitFor("Topic watcher not started",  1_000, timeout,
            () -> this.topicWatcher.started());
        waitFor("Topic configs watcher not started", 1_000, timeout,
            () -> this.topicsConfigWatcher.started());
        waitFor("Topic watcher not started", 1_000, timeout,
            () -> this.topicsWatcher.started());
        //waitFor(context, () -> this.topicsConfigWatcher.started(), timeout, "Topic configs watcher not started");
        //waitFor(context, () -> this.topicWatcher.started(), timeout, "Topic watcher not started");
    }

    private static int zkPort(KafkaCluster cluster) {
        // TODO Method was added in DBZ-540, so no need for reflection once
        // dependency gets upgraded
        try {
            Field zkServerField = KafkaCluster.class.getDeclaredField("zkServer");
            zkServerField.setAccessible(true);
            return ((ZookeeperServer) zkServerField.get(cluster)).getPort();
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private void createInKube(KafkaTopic topic) {
        Crds.topicOperation(kubeClient).create(topic);
    }

    private void updateInKube(KafkaTopic topic) {
        LOGGER.info("Updating topic {} in kube", topic.getMetadata().getName());
        Crds.topicOperation(kubeClient).withName(topic.getMetadata().getName()).patch(topic);
    }

    @Test
    public void testCreatedWithoutTopicNameInKube(TestContext context) {
        LOGGER.info("Test started");

        int retention = 100_000_000;
        KafkaTopic kt = new KafkaTopicBuilder()
                .withNewMetadata()
                .withName("my-topic")
                .addToLabels("strimzi.io/kind", "topic")
                .endMetadata()
                .withNewSpec()
                .withPartitions(1)
                .withReplicas(1)
                .addToConfig("retention.bytes", retention)
                .endSpec().build();

        testCreatedInKube(context, kt);
    }

    void testCreatedInKube(TestContext context, KafkaTopic kt) {
        String kubeName = kt.getMetadata().getName();
        String kafkaName = kt.getSpec().getTopicName() != null ? kt.getSpec().getTopicName() : kubeName;
        int retention = (Integer) kt.getSpec().getConfig().get("retention.bytes");

        createInKube(kt);

        // Check created in Kafka
        waitUntilTopicExistsInKafka(kafkaName);
        LOGGER.info("Topic has been created");
        Topic fromKafka = getFromKafka(context, kafkaName);
        context.assertEquals(kafkaName, fromKafka.getTopicName().toString());
        //context.assertEquals(kubeName, fromKafka.getResourceName().toString());
        // Reconcile after no changes
        reconcile(context);
        // Check things still the same
        Topic fromKafka2 = getFromKafka(context, kafkaName);
        context.assertEquals(fromKafka, fromKafka2);

        // Config change + reconcile
        updateInKube(new KafkaTopicBuilder(kt).editSpec().addToConfig("retention.bytes", retention + 1).endSpec().build());
        waitUntilTopicInKafka(kafkaName, config -> Integer.toString(retention + 1).equals(config.get("retention.bytes").value()));
        // Another reconciliation
        reconcile(context);

        // Check things still the same
        fromKafka2 = getFromKafka(context, kafkaName);
        context.assertEquals(new Topic.Builder(fromKafka)
                            .withConfigEntry("retention.bytes", Integer.toString(retention + 1))
                        .build(),
                fromKafka2);

        // Reconcile after change #partitions change
        // Check things still the same
        // Try to add a matching spec.topicName
        // Check things still the same
        // Try to change spec.topicName
        // Check error
        // Try to change spec.topicName back
        // Check things still the same (recover from error)
        // Try to remove spec.topicName
        // Check things still the same
    }

    Topic getFromKafka(TestContext context, String topicName) {
        AtomicReference<Topic> ref = new AtomicReference<>();
        Async async = context.async();
        Future<TopicMetadata> kafkaMetadata = Future.future();
        session.kafka.topicMetadata(new TopicName(topicName), kafkaMetadata.completer());
        kafkaMetadata.map(metadata -> TopicSerialization.fromTopicMetadata(metadata)).setHandler(fromKafka -> {
            if (fromKafka.succeeded()) {
                ref.set(fromKafka.result());
            } else {
                context.fail(fromKafka.cause());
            }
            async.complete();
        });
        async.await();
        return ref.get();
    }

    private Config waitUntilTopicExistsInKafka(String topicName) {
        return waitUntilTopicInKafka(topicName, desc -> desc != null);
    }

    private Config waitUntilTopicInKafka(String topicName, Predicate<Config> p) {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        AtomicReference<Config> ref = new AtomicReference<>();
        waitFor("Creation of topic " + topicName, 1_000, 60_000, () -> {
            try {
                Map<ConfigResource, Config> descriptionMap = adminClient.describeConfigs(asList(configResource)).all().get();
                Config desc = descriptionMap.get(configResource);
                if (p.test(desc)) {
                    ref.set(desc);
                    return true;
                }
                return false;
            } catch (Exception e) {
                return false;
            }
        });
        return ref.get();
    }

    void reconcile(TestContext context) {
        Async async = context.async();
        session.topicOperator.reconcileAllTopics("test").setHandler(ar -> {
            if (!ar.succeeded()) {
                context.fail(ar.cause());
            }
            async.complete();
        });
        async.await();
    }


    @Test
    public void testCreatedWithSameTopicNameInKube(TestContext context) {

        int retention = 100_000_000;
        KafkaTopic kt = new KafkaTopicBuilder()
                .withNewMetadata()
                .withName("my-topic")
                .addToLabels("strimzi.io/kind", "topic")
                .endMetadata()
                .withNewSpec()
                .withTopicName("my-topic") // the same as metadata.name
                .withPartitions(1)
                .withReplicas(1)
                .addToConfig("retention.bytes", retention)
                .endSpec().build();

        testCreatedInKube(context, kt);
    }

    @Test
    public void testCreatedWithDifferentTopicNameInKube(TestContext context) {
        int retention = 100_000_000;
        KafkaTopic kt = new KafkaTopicBuilder()
                .withNewMetadata()
                .withName("my-topic")
                .addToLabels("strimzi.io/kind", "topic")
                .endMetadata()
                .withNewSpec()
                    .withTopicName("DIFFERENT") // different to metadata.name
                    .withPartitions(1)
                    .withReplicas(1)
                    .addToConfig("retention.bytes", retention)
                .endSpec().build();

        testCreatedInKube(context, kt);
    }

}
