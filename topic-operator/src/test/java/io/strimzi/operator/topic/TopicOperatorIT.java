/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.debezium.kafka.KafkaCluster;
import io.debezium.kafka.ZookeeperServer;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaTopicList;
import io.strimzi.api.kafka.model.DoneableKafkaTopic;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaTopicBuilder;
import io.strimzi.operator.common.Util;
import io.strimzi.test.BaseITST;
import io.strimzi.test.TestUtils;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(VertxUnitRunner.class)
public class TopicOperatorIT extends BaseITST {

    private static final Logger LOGGER = LogManager.getLogger(TopicOperatorIT.class);

    private static String oldNamespace;

    private final Labels labels = Labels.fromString(
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
    private final long timeout = 30_000L;
    private volatile TopicConfigsWatcher topicsConfigWatcher;
    private volatile ZkTopicWatcher topicWatcher;

    private volatile String deploymentId;
    private Set<String> preExistingEvents;

    private Session session;

    @BeforeClass
    public static void setupKubeCluster() {
        CLUSTER.cmdClient().clientWithAdmin()
                .createNamespace(NAMESPACE);
        oldNamespace = setNamespace(NAMESPACE);
        CLUSTER.cmdClient().clientWithAdmin()
                .create("../install/topic-operator/02-Role-strimzi-topic-operator.yaml")
                .create(TestUtils.CRD_TOPIC)
                .create("src/test/resources/TopicOperatorIT-rbac.yaml");
    }

    @AfterClass
    public static void teardownKubeCluster() {
        CLUSTER.cmdClient().clientWithAdmin()
                .delete("src/test/resources/TopicOperatorIT-rbac.yaml")
                .delete(TestUtils.CRD_TOPIC)
                .delete("../install/topic-operator/02-Role-strimzi-topic-operator.yaml")
                .deleteNamespace(NAMESPACE);
        CLUSTER.cmdClient().clientWithAdmin().namespace(oldNamespace);
    }

    @Before
    public void setup(TestContext context) throws Exception {
        LOGGER.info("Setting up test");
        CLUSTER.before();
        Runtime.getRuntime().addShutdownHook(kafkaHook);
        int counts = 3;
        do {
            try {
                kafkaCluster = new KafkaCluster();
                kafkaCluster.addBrokers(1);
                kafkaCluster.deleteDataPriorToStartup(true);
                kafkaCluster.deleteDataUponShutdown(true);
                kafkaCluster.usingDirectory(Files.createTempDirectory("operator-integration-test").toFile());
                kafkaCluster.startup();
                break;
            } catch (kafka.zookeeper.ZooKeeperClientTimeoutException e) {
                if (counts == 0) {
                    throw e;
                }
                counts--;
            }
        } while (true);

        Properties p = new Properties();
        p.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.brokerList());
        adminClient = AdminClient.create(p);

        kubeClient = BaseITST.kubeClient().getClient();
        Crds.registerCustomKinds();
        LOGGER.info("Using namespace {}", NAMESPACE);
        startTopicOperator(context);

        // We can't delete events, so record the events which exist at the start of the test
        // and then waitForEvents() can ignore those
        preExistingEvents = kubeClient.events().inNamespace(NAMESPACE).withLabels(labels.labels()).list().
                getItems().stream().
                map(evt -> evt.getMetadata().getUid()).
                collect(Collectors.toSet());

        LOGGER.info("Finished setting up test");
    }

    void startTopicOperator(TestContext context) {
        LOGGER.info("Starting Topic Operator");
        Map<String, String> m = new HashMap();
        m.put(Config.KAFKA_BOOTSTRAP_SERVERS.key, kafkaCluster.brokerList());
        m.put(Config.ZOOKEEPER_CONNECT.key, "localhost:" + zkPort(kafkaCluster));
        m.put(Config.ZOOKEEPER_CONNECTION_TIMEOUT_MS.key, "30000");
        m.put(Config.NAMESPACE.key, NAMESPACE);
        m.put(Config.TC_RESOURCE_LABELS, "strimzi.io/kind=topic");
        session = new Session(kubeClient, new Config(m));

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
        async.await();
        waitFor(context, () -> this.topicWatcher.started(), timeout, "Topic watcher not started");
        waitFor(context, () -> this.topicsConfigWatcher.started(), timeout, "Topic configs watcher not started");
        waitFor(context, () -> this.topicWatcher.started(), timeout, "Topic watcher not started");
        LOGGER.info("Started Topic Operator");
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

        if (kubeClient != null) {
            List<KafkaTopic> items = operation().inNamespace(NAMESPACE).list().getItems();
            operation().inNamespace(NAMESPACE).delete();
            // Wait for the operator to delete all the existing topics in Kafka
            for (KafkaTopic item : items) {
                waitForTopicInKafka(context, new TopicName(item).toString(), false);
            }
        }

        stopTopicOperator(context);

        adminClient.close();
        if (kafkaCluster != null) {
            kafkaCluster.shutdown();
        }
        Runtime.getRuntime().removeShutdownHook(kafkaHook);
        LOGGER.info("Finished tearing down test");
    }

    void stopTopicOperator(TestContext context) {
        LOGGER.info("Stopping Topic Operator");
        Async async = context.async();
        if (deploymentId != null) {
            vertx.undeploy(deploymentId, ar -> {
                deploymentId = null;
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
        LOGGER.info("Stopped Topic Operator");
    }


    private KafkaTopic createKafkaTopicResource(TestContext context, KafkaTopic topicResource) {
        String topicName = new TopicName(topicResource).toString();
        // Create a Topic Resource
        operation().inNamespace(NAMESPACE).create(topicResource);

        // Wait for the topic to be created
        waitForTopicInKafka(context, topicName);
        return topicResource;
    }

    private KafkaTopic createKafkaTopicResource(TestContext context, String topicName) {
        Topic topic = new Topic.Builder(topicName, 1, (short) 1, emptyMap()).build();
        KafkaTopic topicResource = TopicSerialization.toTopicResource(topic, labels);
        return createKafkaTopicResource(context, topicResource);
    }

    private String createTopic(TestContext context, String topicName) throws InterruptedException, ExecutionException {
        LOGGER.info("Creating topic {}", topicName);
        // Create a topic
        String resourceName = new TopicName(topicName).asKubeName().toString();
        CreateTopicsResult crt = adminClient.createTopics(singletonList(new NewTopic(topicName, 1, (short) 1)));
        crt.all().get();

        // Wait for the resource to be created
        waitForTopicInKube(context, resourceName);

        LOGGER.info("topic {} has been created", resourceName);
        return resourceName;
    }

    private void waitForTopicInKube(TestContext context, String resourceName) {
        waitForTopicInKube(context, resourceName, true);
    }

    private void waitForTopicInKube(TestContext context, String resourceName, boolean exist) {
        waitFor(context, () -> {
            KafkaTopic topic = operation().inNamespace(NAMESPACE).withName(resourceName).get();
            LOGGER.info("Polled topic {} waiting for " + (exist ? "existence" : "non-existence"), resourceName);
            return topic != null == exist;
        }, timeout, "Expected the KafkaTopic '" + resourceName + "' to " + (exist ? "exist" : "not exist") + " in Kubernetes by now");
    }

    private void alterTopicConfigInKafkaAndAwaitReconciliation(TestContext context, String topicName, String resourceName) throws InterruptedException, ExecutionException {
        String key = "compression.type";
        final String changedValue = alterTopicConfigInKafka(topicName, key,
            value -> "snappy".equals(value) ? "lz4" : "snappy");
        awaitTopicConfigInKube(context, resourceName, key, changedValue);
    }

    private void awaitTopicConfigInKube(TestContext context, String resourceName, String key, String expectedValue) {

        // Wait for the resource to be modified
        waitFor(context, () -> {
            KafkaTopic topic = operation().inNamespace(NAMESPACE).withName(resourceName).get();
            LOGGER.info("Polled topic {}, waiting for config change", resourceName);
            String gotValue = TopicSerialization.fromTopicResource(topic).getConfig().get(key);
            LOGGER.info("Expecting value {}, got value {}", expectedValue, gotValue);
            return expectedValue.equals(gotValue);
        }, timeout, "Expected the config of topic " + resourceName + " to have " + key + "=" + expectedValue + " in Kube by now");
    }

    private String alterTopicConfigInKafka(String topicName, String key, Function<String, String> mutator) throws InterruptedException, ExecutionException {
        // Get the topic config
        ConfigResource configResource = topicConfigResource(topicName);
        org.apache.kafka.clients.admin.Config config = getTopicConfig(configResource);

        Map<String, ConfigEntry> m = new HashMap<>();
        for (ConfigEntry entry: config.entries()) {
            m.put(entry.name(), entry);
        }
        final String changedValue = mutator.apply(m.get(key).value());
        m.put(key, new ConfigEntry(key, changedValue));
        LOGGER.info("Changing topic config {} to {}", key, changedValue);

        // Update the topic config
        AlterConfigsResult cgf = adminClient.alterConfigs(singletonMap(configResource,
                new org.apache.kafka.clients.admin.Config(m.values())));
        cgf.all().get();
        return changedValue;
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
            LOGGER.info("Expected value {}, got value {}", changedValue, gotValue);
            return changedValue == gotValue;
        }, timeout, "Expected the topic " + topicName + "to have " + changedValue + " partitions by now");
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
        alterTopicConfigInKafkaAndAwaitReconciliation(context, topicName, resourceName);
    }

    private void deleteTopicInKafkaAndAwaitReconciliation(TestContext context, String topicName, String resourceName) throws InterruptedException, ExecutionException {
        deleteTopicInKafka(topicName, resourceName);

        // Wait for the resource to be deleted
        waitFor(context, () -> {
            KafkaTopic topic = operation().inNamespace(NAMESPACE).withName(resourceName).get();
            LOGGER.info("Polled topic {}, got {}, waiting for deletion", resourceName, topic);
            return topic == null;
        }, timeout, "Expected the topic " + topicName + " to have been deleted by now");
    }

    private void deleteTopicInKafka(String topicName, String resourceName) throws InterruptedException, ExecutionException {
        LOGGER.info("Deleting topic {} (KafkaTopic {})", topicName, resourceName);
        // Now we can delete the topic
        DeleteTopicsResult dlt = adminClient.deleteTopics(singletonList(topicName));
        dlt.all().get();
        LOGGER.info("Deleted topic {}", topicName);
    }

    private void createAndDeleteTopic(TestContext context, String topicName) throws InterruptedException, ExecutionException {
        String resourceName = createTopic(context, topicName);
        deleteTopicInKafkaAndAwaitReconciliation(context, topicName, resourceName);
    }

    private void createAndAlterNumPartitions(TestContext context, String topicName) throws InterruptedException, ExecutionException {
        String resourceName = createTopic(context, topicName);
        alterTopicNumPartitions(context, topicName, resourceName);
    }

    private void waitFor(TestContext context, BooleanSupplier ready, long timeout, String message) {
        Async async = context.async();
        Util.waitFor(vertx, message, 3_000, timeout, ready).setHandler(ar -> {
            if (ar.failed()) {
                context.fail(ar.cause());
            }
            async.complete();
        });
        async.awaitSuccess();
    }

    private void waitForEvent(TestContext context, KafkaTopic kafkaTopic, String expectedMessage, TopicOperator.EventType expectedType) {
        waitFor(context, () -> {
            List<Event> items = kubeClient.events().inNamespace(NAMESPACE).withLabels(labels.labels()).list().getItems();
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
                assertNotNull(event.getLastTimestamp());
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
    public void testKafkaTopicAddedWithHighReplicas(TestContext context) {
        String topicName = "test-resource-created-with-higher-partition";
        Topic topic = new Topic.Builder(topicName, 1, (short) 1, emptyMap()).build();
        KafkaTopic kafkaTopic = TopicSerialization.toTopicResource(topic, labels);
        kafkaTopic.getSpec().setReplicas(42);

        // Create a Topic Resource
        try {
            operation().inNamespace(NAMESPACE).create(kafkaTopic);
        } catch (InvalidReplicationFactorException e) {
            assertTrue(e.getMessage().contains("Replication factor: 42 larger than available brokers"));
        }
    }

    @Test
    public void testKafkaTopicAddedWithBadData(TestContext context) {
        String topicName = "test-resource-created-with-bad-data";
        Topic topic = new Topic.Builder(topicName, 1, (short) 1, emptyMap()).build();
        KafkaTopic kafkaTopic = TopicSerialization.toTopicResource(topic, labels);
        kafkaTopic.getSpec().setPartitions(-1);

        // Create a Topic Resource
        try {
            operation().inNamespace(NAMESPACE).create(kafkaTopic);
        } catch (KubernetesClientException e) {
            assertTrue(e.getMessage().contains("spec.partitions in body should be greater than or equal to 1"));
        }
    }

    @Test
    public void testKafkaTopicDeleted(TestContext context) {
        // create the Topic Resource
        String topicName = "test-kafkatopic-deleted";
        KafkaTopic topicResource = createKafkaTopicResource(context, topicName);
        deleteInKubeAndAwaitReconciliation(context, topicName, topicResource);

    }

    void deleteInKubeAndAwaitReconciliation(TestContext context, String topicName, KafkaTopic topicResource) {
        deleteInKube(topicResource.getMetadata().getName());

        // Wait for the topic to be deleted
        waitFor(context, () -> {
            try {
                adminClient.describeTopics(singletonList(topicName)).values().get(topicName).get();
                return false;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UnknownTopicOrPartitionException
                        || e.getCause() instanceof InvalidTopicException) {
                    return true;
                } else {
                    throw new RuntimeException(e);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, timeout, "Expected topic to be deleted by now");
    }

    private void deleteInKube(String resourceName) {
        // can now delete the topicResource
        operation().inNamespace(NAMESPACE).withName(resourceName).delete();
    }


    @Test
    public void testKafkaTopicModifiedRetentionChanged(TestContext context) throws Exception {
        // create the topic
        String topicName = "test-kafkatopic-modified-retention-changed";
        KafkaTopic topicResource = createKafkaTopicResource(context, topicName);
        String expectedValue = alterTopicConfigInKube(topicResource.getMetadata().getName(),
                "retention.ms", currentValue -> Integer.toString(Integer.parseInt(currentValue) + 1));
        awaitTopicConfigInKafka(context, topicName, "retention.ms", expectedValue);
    }

    void awaitTopicConfigInKafka(TestContext context, String topicName, String key, String expectedValue) {
        // Wait for that to be reflected in the kafka topic
        waitFor(context, () -> {
            ConfigResource configResource = topicConfigResource(topicName);
            org.apache.kafka.clients.admin.Config config = getTopicConfig(configResource);
            String retention = config.get("retention.ms").value();
            LOGGER.debug("retention of {}, waiting for 12341234", retention);
            return expectedValue.equals(retention);
        },  timeout, "Expected the topic " + topicName + " to have retention.ms=" + expectedValue + " in Kafka");
    }

    String alterTopicConfigInKube(String resourceName, String key, Function<String, String> mutator) {
        // now change the topic resource
        Object retention = operation().inNamespace(NAMESPACE).withName(resourceName).get().getSpec().getConfig().getOrDefault(key, "12341233");
        String currentValue = retention instanceof Integer ? retention.toString() : (String) retention;
        String newValue = mutator.apply(currentValue);
        KafkaTopic changedTopic = new KafkaTopicBuilder(operation().inNamespace(NAMESPACE).withName(resourceName).get())
            .editOrNewSpec().addToConfig(key, newValue).endSpec().build();
        operation().inNamespace(NAMESPACE).withName(resourceName).replace(changedTopic);
        return newValue;
    }

    @Test
    public void testKafkaTopicModifiedWithBadData(TestContext context) throws Exception {
        // create the topicResource
        String topicName = "test-kafkatopic-modified-with-bad-data";
        KafkaTopic topicResource = createKafkaTopicResource(context, topicName);

        // now change the topicResource
        KafkaTopic changedTopic = new KafkaTopicBuilder(operation().inNamespace(NAMESPACE).withName(topicResource.getMetadata().getName()).get())
                .editOrNewSpec().withPartitions(-1).endSpec().build();
        try {
            operation().inNamespace(NAMESPACE).withName(topicResource.getMetadata().getName()).replace(changedTopic);
        } catch (KubernetesClientException e) {
            assertTrue(e.getMessage().contains("spec.partitions in body should be greater than or equal to 1"));
        }
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
        KafkaTopic topicResource = TopicSerialization.toTopicResource(topic, labels);
        KafkaTopic topicResource2 = new KafkaTopicBuilder(topicResource).withMetadata(new ObjectMetaBuilder(topicResource.getMetadata()).withName(topicName + "-1").build()).build();
        // create one
        createKafkaTopicResource(context, topicResource2);
        // create another
        operation().inNamespace(NAMESPACE).create(topicResource);

        waitForEvent(context, topicResource,
                "Failure processing KafkaTopic watch event ADDED on resource two-resources-one-topic with labels {strimzi.io/kind=topic}: " +
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
        KafkaTopic topicResource = TopicSerialization.toTopicResource(topic, labels);
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
        waitForTopicInKafka(context, topicName);
    }

    void waitForTopicInKafka(TestContext context, String topicName) {
        waitForTopicInKafka(context, topicName, true);
    }

    void waitForTopicInKafka(TestContext context, String topicName, boolean exist) {
        waitFor(context, () -> {
            try {
                adminClient.describeTopics(singletonList(topicName)).values().get(topicName).get();
                return exist;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UnknownTopicOrPartitionException
                        || e.getCause() instanceof InvalidTopicException) {
                    return !exist;
                } else {
                    throw new RuntimeException(e);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, timeout, "Expected topic '" + topicName + "' to " + (exist ? "exist" : "not exist") + " in Kafka by now");
    }

    // TODO: What happens if we create and then change labels to the resource predicate isn't matched any more
    //       What then happens if we change labels back?


    @Test
    public void testKafkaTopicWithOwnerRef(TestContext context) {
        String topicName = "test-kafka-topic-with-owner-ref-1";

        // this CM is created to be the owner of the KafkaTopic we're about to create.
        String cmName = "hodor";
        HashMap<String, String> cmData = new HashMap<>();
        cmData.put("strimzi", "rulez");
        kubeClient.configMaps().inNamespace(NAMESPACE).create(new ConfigMapBuilder().withNewMetadata().withName(cmName)
                .withNamespace(NAMESPACE).endMetadata().withApiVersion("v1").withData(cmData).build());
        String uid = kubeClient.configMaps().inNamespace(NAMESPACE).withName(cmName).get().getMetadata().getUid();

        ObjectMeta metadata = new ObjectMeta();
        OwnerReference or = new OwnerReferenceBuilder().withName(cmName)
                .withApiVersion("v1")
                .withController(false)
                .withBlockOwnerDeletion(false)
                .withUid(uid)
                .withKind("ConfigMap")
                .build();

        metadata.getOwnerReferences().add(or);
        Map<String, String> annos = new HashMap<>();
        annos.put("iam", "groot");
        Map<String, String> lbls = new HashMap<>();
        lbls.put("iam", "root");
        metadata.setAnnotations(annos);
        metadata.setLabels(lbls);

        // create topic and test OR, labels, annotations
        Topic topic = new Topic.Builder(topicName, 1, (short) 1, emptyMap(), metadata).build();
        KafkaTopic topicResource = TopicSerialization.toTopicResource(topic, labels);
        createKafkaTopicResource(context, topicResource);
        assertEquals(1, operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getOwnerReferences().size());
        assertEquals(uid, operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getOwnerReferences().get(0).getUid());
        assertEquals(1, operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getAnnotations().size());
        assertEquals("groot", operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getAnnotations().get("iam"));
        assertEquals(2, operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getLabels().size());
        assertEquals("root", operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getLabels().get("iam"));

        // edit kafka topic
        topicName = "test-kafka-topic-with-owner-ref-2";
        Topic topic2 = new Topic.Builder(topicName, 1, (short) 1, emptyMap(), metadata).build();
        KafkaTopic topicResource2 = TopicSerialization.toTopicResource(topic2, labels);
        topicResource = TopicSerialization.toTopicResource(topic2, labels);
        topicResource.getMetadata().getAnnotations().put("han", "solo");
        createKafkaTopicResource(context, topicResource2);
        assertEquals(uid, operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getOwnerReferences().get(0).getUid());
        assertEquals(2, operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getAnnotations().size());
        assertEquals("groot", operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getAnnotations().get("iam"));
        assertEquals("solo", operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getAnnotations().get("han"));

        // edit k8s topic
        topicName = "test-kafka-topic-with-owner-ref-3";
        Topic topic3 = new Topic.Builder(topicName, 1, (short) 1, emptyMap(), metadata).build();
        topic3.getMetadata().getLabels().put("stan", "lee");
        topicResource = TopicSerialization.toTopicResource(topic3, labels);
        createKafkaTopicResource(context, topicResource);
        assertEquals(uid, operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getOwnerReferences().get(0).getUid());
        assertEquals(3, operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getLabels().size());
        assertEquals("lee", operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getLabels().get("stan"));
        assertEquals("root", operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getLabels().get("iam"));
    }

    /**
     * Validates that when TO starts it reconciles properly based on what's been previously observed
     * 1. Create topic A in Kube and reconcile
     * 2. Stop TO
     * 3. Create topic X in Kafka, topic Y in Kube
     * 4. Start TO
     * 5. Verify topics A, X and Y exist on both sides
     */
    @Test
    public void testReconciliationOnStartup(TestContext testContext) throws ExecutionException, InterruptedException {
        // 1. Create topic A in Kube and reconcile
        String topicNameZ = "topic-z";
        {
            Topic topicZ = new Topic.Builder(topicNameZ, 1, (short) 1, emptyMap()).build();
            KafkaTopic topicResourceZ = TopicSerialization.toTopicResource(topicZ, labels);
            String resourceNameZ = topicResourceZ.getMetadata().getName();
            operation().inNamespace(NAMESPACE).create(topicResourceZ);
            waitForTopicInKafka(testContext, topicNameZ);
        }

        String topicNameA = "topic-a";
        {
            Topic topicA = new Topic.Builder(topicNameA, 1, (short) 1, emptyMap()).build();
            KafkaTopic topicResourceA = TopicSerialization.toTopicResource(topicA, labels);
            String resourceNameA = topicResourceA.getMetadata().getName();
            operation().inNamespace(NAMESPACE).create(topicResourceA);
            waitForTopicInKafka(testContext, topicNameA);
        }
        String topicNameB = "topic-b";
        String resourceNameB;
        {
            Topic topicB = new Topic.Builder(topicNameB, 1, (short) 1, emptyMap()).build();
            KafkaTopic topicResourceB = TopicSerialization.toTopicResource(topicB, labels);
            resourceNameB = topicResourceB.getMetadata().getName();
            operation().inNamespace(NAMESPACE).create(topicResourceB);
            waitForTopicInKafka(testContext, topicNameB);
        }
        String topicNameC = "topic-c";
        {
            Topic topicC = new Topic.Builder(topicNameC, 1, (short) 1, emptyMap()).build();
            KafkaTopic topicResourceC = TopicSerialization.toTopicResource(topicC, labels);
            String resourceNameC = topicResourceC.getMetadata().getName();
            operation().inNamespace(NAMESPACE).create(topicResourceC);
            waitForTopicInKafka(testContext, topicNameC);
        }

        // 2. Stop TO
        stopTopicOperator(testContext);

        // 3. Modify topic A in kubernetes and topic Z in Kafka
        String alteredConfigA = alterTopicConfigInKafka(topicNameA,
                "retention.ms", currentValue -> Integer.toString(Integer.parseInt(currentValue) + 1));
        String alteredConfigZ = alterTopicConfigInKube(topicNameZ,
                "retention.ms", currentValue -> Integer.toString(Integer.parseInt(currentValue) + 1));

        // 3. Delete topic B in Kafka, delete topic C in Kubernetes
        deleteTopicInKafka(topicNameB, resourceNameB);
        deleteInKube(topicNameC);

        // 3. Create topic X in Kafka, topic Y in Kubernetes
        String topicNameX = "topic-x";
        {
            String resourceName = new TopicName(topicNameX).asKubeName().toString();
            CreateTopicsResult crt = adminClient.createTopics(singletonList(new NewTopic(topicNameX, 1, (short) 1)));
            crt.all().get();
        }

        String topicNameY = "topic-y";
        {
            Topic topicY = new Topic.Builder(topicNameY, 1, (short) 1, emptyMap()).build();
            KafkaTopic topicResourceY = TopicSerialization.toTopicResource(topicY, labels);
            String resourceNameY = topicResourceY.getMetadata().getName();
            operation().inNamespace(NAMESPACE).create(topicResourceY);
        }

        // 4. Start TO
        startTopicOperator(testContext);

        // 5. Verify topics A, X and Y exist on both sides
        waitForTopicInKafka(testContext, topicNameA);
        waitForTopicInKafka(testContext, topicNameX);
        waitForTopicInKafka(testContext, topicNameY);
        waitForTopicInKube(testContext, topicNameA);
        waitForTopicInKube(testContext, topicNameX);
        waitForTopicInKube(testContext, topicNameY);

        // 5. Verify topics B and C deleted on both sides
        waitForTopicInKube(testContext, topicNameB, false);
        waitForTopicInKube(testContext, topicNameC, false);
        waitForTopicInKafka(testContext, topicNameB, false);
        waitForTopicInKafka(testContext, topicNameC, false);

        // 5. Verify topics A and Z were changed.
        awaitTopicConfigInKube(testContext, topicNameA, "retention.ms", alteredConfigA);
        awaitTopicConfigInKube(testContext, topicNameZ, "retention.ms", alteredConfigZ);
        awaitTopicConfigInKafka(testContext, topicNameA, "retention.ms", alteredConfigA);
        awaitTopicConfigInKafka(testContext, topicNameZ, "retention.ms", alteredConfigZ);

    }

}
