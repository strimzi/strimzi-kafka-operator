/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.debezium.kafka.KafkaCluster;
import io.debezium.kafka.ZookeeperServer;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.DoneableKafkaTopic;
import io.strimzi.api.kafka.KafkaTopicList;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaTopicBuilder;
import io.strimzi.test.Namespace;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Namespace(TopicOperatorIT.NAMESPACE)
@RunWith(VertxUnitRunner.class)
public class TopicOperatorIT {

    private static final Logger LOGGER = LogManager.getLogger(TopicOperatorIT.class);

    @ClassRule
    public static KubeClusterResource testCluster = new KubeClusterResource();
    private static String oldNamespace;

    private final LabelPredicate resourcePredicate = LabelPredicate.fromString(
            "strimzi.io/kind=topic");

    public static final String NAMESPACE = "topic-operator-it";

    private final Vertx vertx = Vertx.vertx();
    private KafkaCluster kafkaCluster;
    private volatile AdminClient adminClient;
    private KubernetesClient kubeClient;
    private volatile ZkTopicsWatcher topicsWatcher;
    private Thread kafkaHook = new Thread() {
        @Override
        public void run() {
            if (kafkaCluster != null) {
                kafkaCluster.shutdown();
            }
        }
    };
    private final long timeout = 120_000L;
    private volatile TopicConfigsWatcher topicsConfigWatcher;
    private volatile ZkTopicWatcher topicWatcher;

    private volatile String deploymentId;
    private Set<String> preExistingEvents;

    private Session session;

    @BeforeClass
    public static void setupKubeCluster() {
        testCluster.client().clientWithAdmin()
                .createNamespace(NAMESPACE);
        oldNamespace = testCluster.client().namespace(NAMESPACE);
        testCluster.client().clientWithAdmin()
                .create("../examples/install/topic-operator/02-role.yaml")
                .create(TestUtils.CRD_TOPIC)
                .create("src/test/resources/TopicOperatorIT-rbac.yaml");
    }

    @AfterClass
    public static void teardownKubeCluster() {
        testCluster.client().clientWithAdmin()
                .delete("src/test/resources/TopicOperatorIT-rbac.yaml")
                .delete(TestUtils.CRD_TOPIC)
                .delete("../examples/install/topic-operator/02-role.yaml")
                .deleteNamespace(NAMESPACE);
        testCluster.client().clientWithAdmin().namespace(oldNamespace);
    }

    @Before
    public void setup(TestContext context) throws Exception {
        LOGGER.info("Setting up test");
        Runtime.getRuntime().addShutdownHook(kafkaHook);
        kafkaCluster = new KafkaCluster();
        kafkaCluster.addBrokers(1);
        kafkaCluster.deleteDataPriorToStartup(true);
        kafkaCluster.deleteDataUponShutdown(true);
        kafkaCluster.usingDirectory(Files.createTempDirectory("operator-integration-test").toFile());
        kafkaCluster.startup();

        kubeClient = new DefaultKubernetesClient().inNamespace(NAMESPACE);
        LOGGER.info("Using namespace {}", NAMESPACE);
        Map<String, String> m = new HashMap();
        m.put(Config.KAFKA_BOOTSTRAP_SERVERS.key, kafkaCluster.brokerList());
        m.put(Config.ZOOKEEPER_CONNECT.key, "localhost:" + zkPort(kafkaCluster));
        m.put(Config.NAMESPACE.key, NAMESPACE);
        session = new Session(kubeClient, new Config(m));

        Async async = context.async();
        vertx.deployVerticle(session, ar -> {
            if (ar.succeeded()) {
                deploymentId = ar.result();
                adminClient = session.adminClient;
                topicsConfigWatcher = session.topicConfigsWatcher;
                topicWatcher = session.topicWatcher;
                topicsWatcher = session.topicsWatcher;
                async.complete();
            } else {
                context.fail("Failed to deploy session");
            }
        });
        async.await();

        waitFor(context, () -> this.topicsWatcher.started(), timeout, "Topics watcher not started");
        waitFor(context, () -> this.topicsConfigWatcher.started(), timeout, "Topic configs watcher not started");
        waitFor(context, () -> this.topicWatcher.started(), timeout, "Topic watcher not started");

        // We can't delete events, so record the events which exist at the start of the test
        // and then waitForEvents() can ignore those
        preExistingEvents = kubeClient.events().inNamespace(NAMESPACE).withLabels(resourcePredicate.labels()).list().
                getItems().stream().
                map(evt -> evt.getMetadata().getUid()).
                collect(Collectors.toSet());

        LOGGER.info("Finished setting up test");
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

    @After
    public void teardown(TestContext context) {
        LOGGER.info("Tearing down test");

        operation().inNamespace(NAMESPACE).delete();

        Async async = context.async();
        if (deploymentId != null) {
            vertx.undeploy(deploymentId, ar -> {
                deploymentId = null;
                adminClient = null;
                topicsConfigWatcher = null;
                topicWatcher = null;
                topicsWatcher = null;
                if (ar.failed()) {
                    LOGGER.error("Error undeploying session", ar.cause());
                    context.fail("Error undeploying session");
                }
                async.complete();
            });
        }
        async.await();
        if (kafkaCluster != null) {
            kafkaCluster.shutdown();
        }
        Runtime.getRuntime().removeShutdownHook(kafkaHook);
        LOGGER.info("Finished tearing down test");
    }


    private KafkaTopic createKafkaTopicResource(TestContext context, KafkaTopic topicResource) {
        String topicName = new TopicName(topicResource).toString();
        // Create a Topic Resource
        operation().inNamespace(NAMESPACE).create(topicResource);

        // Wait for the topic to be created
        waitFor(context, () -> {
            try {
                adminClient.describeTopics(singletonList(topicName)).values().get(topicName).get();
                return true;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                    return false;
                } else {
                    throw new RuntimeException(e);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, timeout, "Expected topic to be created by now");
        return topicResource;
    }

    private KafkaTopic createKafkaTopicResource(TestContext context, String topicName) {
        Topic topic = new Topic.Builder(topicName, 1, (short) 1, emptyMap()).build();
        KafkaTopic topicResource = TopicSerialization.toTopicResource(topic, resourcePredicate);
        return createKafkaTopicResource(context, topicResource);
    }

    private String createTopic(TestContext context, String topicName) throws InterruptedException, ExecutionException {
        LOGGER.info("Creating topic {}", topicName);
        // Create a topic
        String resourceName = new TopicName(topicName).asMapName().toString();
        CreateTopicsResult crt = adminClient.createTopics(singletonList(new NewTopic(topicName, 1, (short) 1)));
        crt.all().get();

        // Wait for the resource to be created
        waitFor(context, () -> {
            KafkaTopic topic = operation().inNamespace(NAMESPACE).withName(resourceName).get();
            LOGGER.info("Polled topic {} waiting for creation", resourceName);
            return topic != null;
        }, timeout, "Expected the topic to have been created by now");

        LOGGER.info("topic {} has been created", resourceName);
        return resourceName;
    }

    private void alterTopicConfig(TestContext context, String topicName, String resourceName) throws InterruptedException, ExecutionException {
        // Get the topic config
        ConfigResource configResource = topicConfigResource(topicName);
        org.apache.kafka.clients.admin.Config config = getTopicConfig(configResource);

        String key = "compression.type";

        Map<String, ConfigEntry> m = new HashMap<>();
        for (ConfigEntry entry: config.entries()) {
            m.put(entry.name(), entry);
        }
        final String changedValue;
        if ("snappy".equals(m.get(key).value())) {
            changedValue = "lz4";
        } else {
            changedValue = "snappy";
        }
        m.put(key, new ConfigEntry(key, changedValue));
        LOGGER.info("Changing topic config {} to {}", key, changedValue);

        // Update the topic config
        AlterConfigsResult cgf = adminClient.alterConfigs(singletonMap(configResource,
                new org.apache.kafka.clients.admin.Config(m.values())));
        cgf.all().get();

        // Wait for the resource to be modified
        waitFor(context, () -> {
            KafkaTopic topic = operation().inNamespace(NAMESPACE).withName(resourceName).get();
            LOGGER.info("Polled topic {}, waiting for config change", resourceName);
            String gotValue = TopicSerialization.fromTopicResource(topic).getConfig().get(key);
            LOGGER.info("Got value {}", gotValue);
            return changedValue.equals(gotValue);
        }, timeout, "Expected the topic to have been deleted by now");
    }

    private void alterTopicNumPartitions(TestContext context, String topicName, String resourceName) throws InterruptedException, ExecutionException {
        int changedValue = 2;

        NewPartitions newPartitions = NewPartitions.increaseTo(changedValue);
        Map<String, NewPartitions> map = new HashMap<>(1);
        map.put(topicName, newPartitions);

        CreatePartitionsResult createPartitionsResult = adminClient.createPartitions(map);
        createPartitionsResult.all().get();

        // Wait for the resource to be modified
        waitFor(context, () -> {
            KafkaTopic topic = operation().inNamespace(NAMESPACE).withName(resourceName).get();
            LOGGER.info("Polled topic {}, waiting for partitions change", resourceName);
            int gotValue = TopicSerialization.fromTopicResource(topic).getNumPartitions();
            LOGGER.info("Got value {}", gotValue);
            return changedValue == gotValue;
        }, timeout, "Expected the topic to have been deleted by now");
    }

    private org.apache.kafka.clients.admin.Config getTopicConfig(ConfigResource configResource) {
        try {
            return adminClient.describeConfigs(singletonList(configResource)).values().get(configResource).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private ConfigResource topicConfigResource(String topicName) {
        return new ConfigResource(ConfigResource.Type.TOPIC, topicName);
    }

    private void createAndAlterTopicConfig(TestContext context, String topicName) throws InterruptedException, ExecutionException {
        String resourceName = createTopic(context, topicName);
        alterTopicConfig(context, topicName, resourceName);
    }

    private void deleteTopic(TestContext context, String topicName, String resourceName) throws InterruptedException, ExecutionException {
        LOGGER.info("Deleting topic {} (KafkaTopic {})", topicName, resourceName);
        // Now we can delete the topic
        DeleteTopicsResult dlt = adminClient.deleteTopics(singletonList(topicName));
        dlt.all().get();
        LOGGER.info("Deleted topic {}", topicName);

        // Wait for the resource to be deleted
        waitFor(context, () -> {
            KafkaTopic topic = operation().inNamespace(NAMESPACE).withName(resourceName).get();
            LOGGER.info("Polled topic {}, got {}, waiting for deletion", resourceName, topic);
            return topic == null;
        }, timeout, "Expected the topic to have been deleted by now");
    }

    private void createAndDeleteTopic(TestContext context, String topicName) throws InterruptedException, ExecutionException {
        String resourceName = createTopic(context, topicName);
        deleteTopic(context, topicName, resourceName);
    }

    private void createAndAlterNumPartitions(TestContext context, String topicName) throws InterruptedException, ExecutionException {
        String resourceName = createTopic(context, topicName);
        alterTopicNumPartitions(context, topicName, resourceName);
    }


    private void waitFor(TestContext context, BooleanSupplier ready, long timeout, String message) {
        Async async = context.async();
        long t0 = System.currentTimeMillis();
        Future<Void> fut = Future.future();
        vertx.setPeriodic(3_000L, timerId -> {
            // Wait for a resource to be created
            boolean isFinished;
            try {
                isFinished = ready.getAsBoolean();
                if (isFinished) {
                    fut.complete();
                }
            } catch (Throwable e) {
                fut.fail(e);
                isFinished = true;
            }
            if (isFinished) {
                async.complete();
                vertx.cancelTimer(timerId);
            }
            long timeLeft = timeout - (System.currentTimeMillis() - t0);
            if (timeLeft <= 0) {
                vertx.cancelTimer(timerId);
                context.fail(message);
            }
        });
        async.await(timeout);
        if (fut.failed()) {
            context.fail(fut.cause());
        }
    }

    private void waitForEvent(TestContext context, KafkaTopic kafkaTopic, String expectedMessage, TopicOperator.EventType expectedType) {
        waitFor(context, () -> {
            List<Event> items = kubeClient.events().inNamespace(NAMESPACE).withLabels(resourcePredicate.labels()).list().getItems();
            List<Event> filtered = items.stream().
                    filter(evt -> !preExistingEvents.contains(evt.getMetadata().getUid())
                    && "KafkaTopic".equals(evt.getInvolvedObject().getKind())
                    && kafkaTopic.getMetadata().getName().equals(evt.getInvolvedObject().getName())).
                    collect(Collectors.toList());
            LOGGER.debug("Waiting for events: {}", filtered.stream().map(evt -> evt.getMessage()).collect(Collectors.toList()));
            if (!filtered.isEmpty()) {
                assertEquals(1, filtered.size());
                Event event = filtered.get(0);

                assertEquals(expectedMessage, event.getMessage());
                assertEquals(expectedType.name, event.getType());
                assertNotNull(event.getInvolvedObject());
                assertEquals("KafkaTopic", event.getInvolvedObject().getKind());
                assertEquals(kafkaTopic.getMetadata().getName(), event.getInvolvedObject().getName());
                return true;
            } else {
                return false;
            }
        }, timeout, "Expected an error event");
    }


    @Test
    public void testTopicAdded(TestContext context) throws Exception {
        createTopic(context, "test-topic-added");
    }

    @Test
    public void testTopicAddedWithEncodableName(TestContext context) throws Exception {
        createTopic(context, "thest-TOPIC_ADDED");
    }

    @Test
    public void testTopicDeleted(TestContext context) throws Exception {
        createAndDeleteTopic(context, "test-topic-deleted");
    }

    @Test
    public void testTopicDeletedWithEncodableName(TestContext context) throws Exception {
        createAndDeleteTopic(context, "test-TOPIC_DELETED");
    }

    @Test
    public void testTopicConfigChanged(TestContext context) throws Exception {
        createAndAlterTopicConfig(context, "test-topic-config-changed");
    }

    @Test
    public void testTopicConfigChangedWithEncodableName(TestContext context) throws Exception {
        createAndAlterTopicConfig(context, "test-TOPIC_CONFIG_CHANGED");
    }

    @Test
    public void testTopicNumPartitionsChanged(TestContext context) throws Exception {
        createAndAlterNumPartitions(context, "test-topic-partitions-changed");
    }

    @Test
    @Ignore
    public void testTopicNumReplicasChanged(TestContext context) {
        context.fail("Implement this");
    }

    @Test
    public void testKafkaTopicAdded(TestContext context) {
        String topicName = "test-kafkatopic-created";
        createKafkaTopicResource(context, topicName);
    }

    @Test
    public void testKafkaTopicAddedWithBadData(TestContext context) {
        String topicName = "test-resource-created-with-bad-data";
        Topic topic = new Topic.Builder(topicName, 1, (short) 1, emptyMap()).build();
        KafkaTopic kafkaTopic = TopicSerialization.toTopicResource(topic, resourcePredicate);
        kafkaTopic.getSpec().setPartitions(-1);

        // Create a Topic Resource
        operation().inNamespace(NAMESPACE).create(kafkaTopic);

        // Wait for the warning event
        waitForEvent(context, kafkaTopic,
                "KafkaTopic test-resource-created-with-bad-data has an invalid spec section: KafkaTopic's spec.partitions should be strictly greater than 0",
                TopicOperator.EventType.WARNING);

    }

    @Test
    public void testKafkaTopicDeleted(TestContext context) {
        // create the Topic Resource
        String topicName = "test-kafkatopic-deleted";
        KafkaTopic topicResource = createKafkaTopicResource(context, topicName);

        // can now delete the topicResource
        operation().inNamespace(NAMESPACE).withName(topicResource.getMetadata().getName()).delete();

        // Wait for the topic to be deleted
        waitFor(context, () -> {
            try {
                adminClient.describeTopics(singletonList(topicName)).values().get(topicName).get();
                return false;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                    return true;
                } else {
                    throw new RuntimeException(e);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, timeout, "Expected topic to be deleted by now");

    }


    @Test
    public void testKafkaTopicModifiedRetentionChanged(TestContext context) throws Exception {
        // create the topic
        String topicName = "test-kafkatopic-modified-retention-changed";
        KafkaTopic topicResource = createKafkaTopicResource(context, topicName);

        // now change the topic resource
        KafkaTopic changedTopic = new KafkaTopicBuilder(operation().inNamespace(NAMESPACE).withName(topicResource.getMetadata().getName()).get())
            .editOrNewSpec().addToConfig("retention.ms", 12341234).endSpec().build();
        operation().inNamespace(NAMESPACE).withName(topicResource.getMetadata().getName()).replace(changedTopic);

        // Wait for that to be reflected in the kafka topic
        waitFor(context, () -> {
            ConfigResource configResource = topicConfigResource(topicName);
            org.apache.kafka.clients.admin.Config config = getTopicConfig(configResource);
            String retention = config.get("retention.ms").value();
            LOGGER.debug("retention of {}, waiting for 12341234", retention);
            return "12341234".equals(retention);
        },  timeout, "Expected the topic to be updated");
    }

    @Test
    public void testKafkaTopicModifiedWithBadData(TestContext context) throws Exception {
        // create the topicResource
        String topicName = "test-kafkatopic-modified-with-bad-data";
        KafkaTopic topicResource = createKafkaTopicResource(context, topicName);

        // now change the topicResource
        KafkaTopic changedTopic = new KafkaTopicBuilder(operation().inNamespace(NAMESPACE).withName(topicResource.getMetadata().getName()).get())
                .editOrNewSpec().withPartitions(-1).endSpec().build();
        operation().inNamespace(NAMESPACE).withName(topicResource.getMetadata().getName()).replace(changedTopic);

        // Wait for that to be reflected in the topic
        waitForEvent(context, topicResource,
                "KafkaTopic test-kafkatopic-modified-with-bad-data has an invalid spec section: KafkaTopic's spec.partitions should be strictly greater than 0",
                TopicOperator.EventType.WARNING);
    }

    @Test
    public void testKafkaTopicModifiedNameChanged(TestContext context) throws Exception {
        // create the topicResource
        String topicName = "test-kafkatopic-modified-name-changed";
        KafkaTopic topicResource = createKafkaTopicResource(context, topicName);

        // now change the topicResource
        String changedName = topicName.toUpperCase(Locale.ENGLISH);
        LOGGER.info("Changing Topic Resource spec.topicName from {} to {}", topicName, changedName);
        KafkaTopic changedTopic = new KafkaTopicBuilder(operation().inNamespace(NAMESPACE).withName(topicResource.getMetadata().getName()).get())
                .editOrNewSpec().withTopicName(changedName).endSpec().build();
        operation().inNamespace(NAMESPACE).withName(topicResource.getMetadata().getName()).replace(changedTopic);

        // We expect this to cause a warning event
        waitForEvent(context, topicResource,
                "Kafka topics cannot be renamed, but KafkaTopic's spec.topicName has changed.",
                TopicOperator.EventType.WARNING);

    }

    @Test
    public void testCreateTwoResourcesManagingOneTopic(TestContext context) {
        String topicName = "two-resources-one-topic";
        Topic topic = new Topic.Builder(topicName, 1, (short) 1, emptyMap()).build();
        KafkaTopic topicResource = TopicSerialization.toTopicResource(topic, resourcePredicate);
        KafkaTopic topicResource2 = new KafkaTopicBuilder(topicResource).withMetadata(new ObjectMetaBuilder(topicResource.getMetadata()).withName(topicName + "-1").build()).build();
        // create one
        createKafkaTopicResource(context, topicResource2);
        // create another
        operation().inNamespace(NAMESPACE).create(topicResource);

        waitForEvent(context, topicResource,
                "Failure processing KafkaTopic watch event ADDED on map two-resources-one-topic with labels {strimzi.io/kind=topic}: " +
                        "Topic 'two-resources-one-topic' is already managed via KafkaTopic 'two-resources-one-topic-1' it cannot also be managed via the KafkaTopic 'two-resources-one-topic'",
                TopicOperator.EventType.WARNING);
    }

    private MixedOperation<KafkaTopic, KafkaTopicList, DoneableKafkaTopic, Resource<KafkaTopic, DoneableKafkaTopic>> operation() {
        return kubeClient.customResources(Crds.topic(), KafkaTopic.class, KafkaTopicList.class, DoneableKafkaTopic.class);
    }

    @Test
    public void testReconcile(TestContext context) {
        String topicName = "test-reconcile";

        Topic topic = new Topic.Builder(topicName, 1, (short) 1, emptyMap()).build();
        KafkaTopic topicResource = TopicSerialization.toTopicResource(topic, resourcePredicate);
        String resourceName = topicResource.getMetadata().getName();

        operation().inNamespace(NAMESPACE).create(topicResource);

        // Wait for the resource to be created
        waitFor(context, () -> {
            KafkaTopic createdResource = operation().inNamespace(NAMESPACE).withName(resourceName).get();
            LOGGER.info("Polled kafkatopic {} waiting for creation", resourceName);

            // modify resource
            if (createdResource != null) {
                createdResource.getSpec().setPartitions(2);
                operation().inNamespace(NAMESPACE).withName(resourceName).patch(createdResource);
            }

            return createdResource != null;
        }, timeout, "Expected the kafkatopic to have been created by now");

        // trigger an immediate reconcile, while topic operator is dealing with resource modification
        session.topicOperator.reconcileAllTopics("periodic");

        // Wait for the topic to be created
        waitFor(context, () -> {
            try {
                adminClient.describeTopics(singletonList(topicName)).values().get(topicName).get();
                return true;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                    return false;
                } else {
                    throw new RuntimeException(e);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, timeout, "Expected topic to be created by now");
    }

    // TODO: What happens if we create and then change labels to the resource predicate isn't matched any more
    //       What then happens if we change labels back?

}
