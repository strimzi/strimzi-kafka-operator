/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.debezium.kafka.KafkaCluster;
import io.debezium.kafka.ZookeeperServer;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaTopicList;
import io.strimzi.api.kafka.model.DoneableKafkaTopic;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaTopicBuilder;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.operator.common.Util;
import io.strimzi.test.BaseITST;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeCluster;
import io.strimzi.test.k8s.NoClusterException;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import kafka.server.KafkaConfig$;
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
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("checkstyle:ClassFanOutComplexity")
public abstract class TopicOperatorBaseIT extends BaseITST {

    private static final Logger LOGGER = LogManager.getLogger(TopicOperatorBaseIT.class);

    protected static String oldNamespace;

    protected final Labels labels = Labels.fromString(
            "strimzi.io/kind=topic");

    public static final String NAMESPACE = "topic-operator-it";

    protected final Vertx vertx = Vertx.vertx();
    protected KafkaCluster kafkaCluster;
    protected volatile AdminClient adminClient;
    protected KubernetesClient kubeClient;
    protected Thread kafkaHook = new Thread() {
        @Override
        public void run() {
            if (kafkaCluster != null) {
                kafkaCluster.shutdown();
            }
        }
    };
    protected final long timeout = 600_000L;

    @Rule
    public Timeout timeoutRule = new Timeout(10, TimeUnit.MINUTES);


    protected volatile String deploymentId;
    protected Set<String> preExistingEvents;

    protected Session session;

    @BeforeClass
    public static void setupKubeCluster() throws IOException {
        try {
            KubeCluster.bootstrap();
        } catch (NoClusterException e) {
            Assume.assumeTrue(e.getMessage(), false);
        }
        cmdKubeClient()
                .createNamespace(NAMESPACE);
        oldNamespace = setNamespace(NAMESPACE);
        LOGGER.info("#### Creating " + "../install/topic-operator/02-Role-strimzi-topic-operator.yaml");
        LOGGER.info(new String(Files.readAllBytes(new File("../install/topic-operator/02-Role-strimzi-topic-operator.yaml").toPath())));
        cmdKubeClient().create("../install/topic-operator/02-Role-strimzi-topic-operator.yaml");
        LOGGER.info("#### Creating " + TestUtils.CRD_TOPIC);
        LOGGER.info(new String(Files.readAllBytes(new File(TestUtils.CRD_TOPIC).toPath())));
        cmdKubeClient().create(TestUtils.CRD_TOPIC);
        LOGGER.info("#### Creating " + "src/test/resources/TopicOperatorIT-rbac.yaml");
        LOGGER.info(new String(Files.readAllBytes(new File("src/test/resources/TopicOperatorIT-rbac.yaml").toPath())));

        cmdKubeClient().create("src/test/resources/TopicOperatorIT-rbac.yaml");
    }

    @AfterClass
    public static void teardownKubeCluster() {
        if (oldNamespace != null) {
            cmdKubeClient()
                    .delete("src/test/resources/TopicOperatorIT-rbac.yaml")
                    .delete(TestUtils.CRD_TOPIC)
                    .delete("../install/topic-operator/02-Role-strimzi-topic-operator.yaml")
                    .deleteNamespace(NAMESPACE);
            cmdKubeClient().namespace(oldNamespace);
        }
    }

    @Before
    public void setup(TestContext context) throws Exception {
        LOGGER.info("Setting up test");
        kubeCluster().before();
        Runtime.getRuntime().addShutdownHook(kafkaHook);
        int counts = 3;
        do {
            try {
                kafkaCluster = new KafkaCluster();
                kafkaCluster.addBrokers(numKafkaBrokers());
                kafkaCluster.deleteDataPriorToStartup(true);
                kafkaCluster.deleteDataUponShutdown(true);
                kafkaCluster.usingDirectory(Files.createTempDirectory("operator-integration-test").toFile());
                kafkaCluster.withKafkaConfiguration(kafkaClusterConfig());
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

    /**
     * @return The number of Kafka brokers in the Kafka cluster
     */
    protected abstract int numKafkaBrokers();

    /**
     * @return The Kafka broker config to be used for the Kafka cluster.
     */
    protected abstract Properties kafkaClusterConfig();

    @After
    public void teardown(TestContext context) throws InterruptedException {
        LOGGER.info("Tearing down test");

        boolean deletionEnabled = "true".equals(kafkaClusterConfig().getOrDefault(
                KafkaConfig$.MODULE$.DeleteTopicEnableProp(), "true"));

        if (deletionEnabled && kubeClient != null) {
            List<KafkaTopic> items = operation().inNamespace(NAMESPACE).list().getItems();

            // Wait for the operator to delete all the existing topics in Kafka
            for (KafkaTopic item : items) {
                LOGGER.info("Deleting {} from Kube", item.getMetadata().getName());
                operation().inNamespace(NAMESPACE).withName(item.getMetadata().getName()).delete();
                LOGGER.info("Awaiting deletion of {} in Kafka", item.getMetadata().getName());
                waitForTopicInKafka(context, new TopicName(item).toString(), false);
                waitForTopicInKube(context, item.getMetadata().getName(), false);
            }
            Thread.sleep(5_000);
        }

        stopTopicOperator(context);

        if (!deletionEnabled && kubeClient != null) {
            List<KafkaTopic> items = operation().inNamespace(NAMESPACE).list().getItems();

            // Wait for the operator to delete all the existing topics in Kafka
            for (KafkaTopic item : items) {
                operation().inNamespace(NAMESPACE).withName(item.getMetadata().getName()).delete();
                waitForTopicInKube(context, item.getMetadata().getName(), false);
            }
        }

        adminClient.close();
        if (kafkaCluster != null) {
            kafkaCluster.shutdown();
        }
        Runtime.getRuntime().removeShutdownHook(kafkaHook);
        LOGGER.info("Finished tearing down test");
    }

    protected void startTopicOperator(TestContext context) {

        LOGGER.info("Starting Topic Operator");
        session = new Session(kubeClient, new Config(topicOperatorConfig()));

        Async async = context.async();
        vertx.deployVerticle(session, ar -> {
            if (ar.succeeded()) {
                deploymentId = ar.result();
                async.complete();
            } else {
                ar.cause().printStackTrace();
                context.fail("Failed to deploy session");
            }
        });
        async.await();
        LOGGER.info("Started Topic Operator");
    }

    protected Map<String, String> topicOperatorConfig() {
        Map<String, String> m = new HashMap<>();
        m.put(Config.KAFKA_BOOTSTRAP_SERVERS.key, kafkaCluster.brokerList());
        m.put(Config.ZOOKEEPER_CONNECT.key, "localhost:" + zkPort(kafkaCluster));
        m.put(Config.ZOOKEEPER_CONNECTION_TIMEOUT_MS.key, "30000");
        m.put(Config.NAMESPACE.key, NAMESPACE);
        m.put(Config.TC_RESOURCE_LABELS, "strimzi.io/kind=topic");
        m.put(Config.FULL_RECONCILIATION_INTERVAL_MS.key, "20000");
        return m;
    }

    protected static int zkPort(KafkaCluster cluster) {
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

    protected void stopTopicOperator(TestContext context) {
        LOGGER.info("Stopping Topic Operator");
        Async async = context.async();
        if (deploymentId != null) {
            vertx.undeploy(deploymentId, ar -> {
                deploymentId = null;
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

    protected KafkaTopic createKafkaTopicResource(TestContext context, KafkaTopic topicResource) {
        String topicName = new TopicName(topicResource).toString();
        // Create a Topic Resource
        operation().inNamespace(NAMESPACE).create(topicResource);

        // Wait for the topic to be created
        waitForTopicInKafka(context, topicName);
        assertStatusReady(context, topicResource.getMetadata().getName());
        return topicResource;
    }

    protected void assertStatusReady(TestContext testContext, String topicName) {
        waitFor(testContext, () -> {
            KafkaTopic kafkaTopic = operation().inNamespace(NAMESPACE).withName(topicName).get();
            if (kafkaTopic != null) {
                if (kafkaTopic.getStatus() != null
                        && kafkaTopic.getStatus().getConditions() != null) {
                    List<Condition> conditions = kafkaTopic.getStatus().getConditions();
                    testContext.assertTrue(conditions.size() > 0);
                    if (conditions.stream().anyMatch(condition ->
                            "Ready".equals(condition.getType()) &&
                                    "True".equals(condition.getStatus()))) {
                        return true;
                    } else {
                        LOGGER.info(conditions);
                    }
                }
            } else {
                LOGGER.info("{} does not exist", topicName);
            }
            return false;
        }, 60000, "status ready for topic " + topicName);
    }

    protected void assertStatusNotReady(TestContext testContext, String topicName, String message) {
        waitFor(testContext, () -> {
            KafkaTopic kafkaTopic = operation().inNamespace(NAMESPACE).withName(topicName).get();
            if (kafkaTopic != null) {
                if (kafkaTopic.getStatus() != null
                        && kafkaTopic.getStatus().getConditions() != null) {
                    List<Condition> conditions = kafkaTopic.getStatus().getConditions();
                    testContext.assertTrue(conditions.size() > 0);
                    Optional<Condition> unreadyCondition = conditions.stream().filter(condition ->
                            "NotReady".equals(condition.getType()) &&
                            "True".equals(condition.getStatus())).findFirst();
                    if (unreadyCondition.isPresent()) {
                        assertEquals(message, unreadyCondition.get().getMessage());
                        return true;
                    } else {
                        LOGGER.info(conditions);
                    }
                }
            } else {
                LOGGER.info("{} does not exist", topicName);
            }
            return false;
        }, 60000, "status ready");
    }

    protected KafkaTopic createKafkaTopicResource(TestContext context, String topicName) {
        Topic topic = new Topic.Builder(topicName, 1, (short) 1, emptyMap()).build();
        KafkaTopic topicResource = TopicSerialization.toTopicResource(topic, labels);
        return createKafkaTopicResource(context, topicResource);
    }

    /**
     * Create a topic in Kafka with a single partition and RF=1.
     * @param context The test context.
     * @param topicName The name of the topic.
     * @return The name of the KafkaTopic resource that was created in Kube.
     * @throws InterruptedException
     * @throws ExecutionException
     */
    protected String createTopic(TestContext context, String topicName) throws InterruptedException, ExecutionException {
        return createTopic(context, topicName, new NewTopic(topicName, 1, (short) 1));
    }

    /**
     * Create a topic in Kafka with a single partition and the given replica assignments
     * @param context The test context.
     * @param topicName The name of the topic.
     * @param replicaAssignments The replica assignments.
     * @return The name of the KafkaTopic resource that was created in Kube.
     * @throws InterruptedException
     * @throws ExecutionException
     */
    protected String createTopic(TestContext context, String topicName, List<Integer> replicaAssignments) throws InterruptedException, ExecutionException {
        return createTopic(context, topicName, new NewTopic(topicName, singletonMap(0, replicaAssignments)));
    }

    private String createTopic(TestContext context, String topicName, NewTopic o) throws InterruptedException, ExecutionException {
        LOGGER.info("Creating topic {}", topicName);
        // Create a topic
        String resourceName = new TopicName(topicName).asKubeName().toString();
        CreateTopicsResult crt = adminClient.createTopics(singletonList(o));
        crt.all().get();

        // Wait for the resource to be created
        waitForTopicInKube(context, resourceName);

        LOGGER.info("topic {} has been created", resourceName);
        return resourceName;
    }

    protected void waitForTopicInKube(TestContext context, String resourceName) {
        waitForTopicInKube(context, resourceName, true);
    }

    protected void waitForTopicInKube(TestContext context, String resourceName, boolean exist) {
        waitFor(context, () -> {
            KafkaTopic topic = operation().inNamespace(NAMESPACE).withName(resourceName).get();
            LOGGER.info("Polled topic {} waiting for " + (exist ? "existence" : "non-existence"), resourceName);
            return topic != null == exist;
        }, timeout, "Expected the KafkaTopic '" + resourceName + "' to " + (exist ? "exist" : "not exist") + " in Kubernetes by now");
    }

    protected void alterTopicConfigInKafkaAndAwaitReconciliation(TestContext context, String topicName, String resourceName) throws InterruptedException, ExecutionException {
        String key = "compression.type";
        final String changedValue = alterTopicConfigInKafka(topicName, key, value -> "snappy".equals(value) ? "lz4" : "snappy");
        awaitTopicConfigInKube(context, resourceName, key, changedValue);
    }

    protected void awaitTopicConfigInKube(TestContext context, String resourceName, String key, String expectedValue) {

        // Wait for the resource to be modified
        waitFor(context, () -> {
            KafkaTopic topic = operation().inNamespace(NAMESPACE).withName(resourceName).get();
            LOGGER.info("Polled topic {}, waiting for config change", resourceName);
            String gotValue = TopicSerialization.fromTopicResource(topic).getConfig().get(key);
            LOGGER.info("Expecting value {}, got value {}", expectedValue, gotValue);
            return expectedValue.equals(gotValue);
        }, timeout, "Expected the config of topic " + resourceName + " to have " + key + "=" + expectedValue + " in Kube by now");
    }

    protected String alterTopicConfigInKafka(String topicName, String key, Function<String, String> mutator) throws InterruptedException, ExecutionException {
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

    protected void alterTopicNumPartitions(TestContext context, String topicName, String resourceName) throws InterruptedException, ExecutionException {
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

    protected org.apache.kafka.clients.admin.Config getTopicConfig(ConfigResource configResource) {
        try {
            return adminClient.describeConfigs(singletonList(configResource)).values().get(configResource).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    protected ConfigResource topicConfigResource(String topicName) {
        return new ConfigResource(ConfigResource.Type.TOPIC, topicName);
    }

    protected void createAndAlterTopicConfig(TestContext context, String topicName) throws InterruptedException, ExecutionException {
        String resourceName = createTopic(context, topicName);
        alterTopicConfigInKafkaAndAwaitReconciliation(context, topicName, resourceName);
    }

    protected void deleteTopicInKafkaAndAwaitReconciliation(TestContext context, String topicName, String resourceName) throws InterruptedException, ExecutionException {
        deleteTopicInKafka(topicName, resourceName);

        // Wait for the resource to be deleted
        waitFor(context, () -> {
            KafkaTopic topic = operation().inNamespace(NAMESPACE).withName(resourceName).get();
            LOGGER.info("Polled topic {}, got {}, waiting for deletion", resourceName, topic);
            return topic == null;
        }, timeout, "Expected the topic " + topicName + " to have been deleted by now");
    }

    protected void deleteTopicInKafka(String topicName, String resourceName) throws InterruptedException, ExecutionException {
        LOGGER.info("Deleting topic {} (KafkaTopic {})", topicName, resourceName);
        // Now we can delete the topic
        DeleteTopicsResult dlt = adminClient.deleteTopics(singletonList(topicName));
        dlt.all().get();
        LOGGER.info("Deleted topic {}", topicName);
    }

    protected void createAndDeleteTopic(TestContext context, String topicName) throws InterruptedException, ExecutionException {
        String resourceName = createTopic(context, topicName);
        deleteTopicInKafkaAndAwaitReconciliation(context, topicName, resourceName);
    }

    protected void createAndAlterNumPartitions(TestContext context, String topicName) throws InterruptedException, ExecutionException {
        String resourceName = createTopic(context, topicName);
        alterTopicNumPartitions(context, topicName, resourceName);
    }

    protected void waitFor(TestContext context, BooleanSupplier ready, long timeout, String message) {
        Async async = context.async();
        Util.waitFor(vertx, message, 3_000, timeout, ready).setHandler(ar -> {
            if (ar.failed()) {
                context.fail(ar.cause());
            }
            async.complete();
        });
        async.awaitSuccess();
    }

    protected void waitForEvent(TestContext context, KafkaTopic kafkaTopic, String expectedMessage, TopicOperator.EventType expectedType) {
        waitFor(context, () -> {
            List<Event> items = kubeClient.events().inNamespace(NAMESPACE).withLabels(labels.labels()).list().getItems();
            List<Event> filtered = items.stream().
                    filter(evt -> !preExistingEvents.contains(evt.getMetadata().getUid())
                            && "KafkaTopic".equals(evt.getInvolvedObject().getKind())
                            && kafkaTopic.getMetadata().getName().equals(evt.getInvolvedObject().getName())).
                    collect(Collectors.toList());
            LOGGER.debug("Waiting for events: {}", filtered.stream().map(evt -> evt.getMessage()).collect(Collectors.toList()));
            return filtered.stream().anyMatch(event ->
                    Objects.equals(expectedMessage, event.getMessage()) &&
                            Objects.equals(expectedType.name, event.getType()) &&
                            event.getInvolvedObject() != null &&
                            event.getLastTimestamp() != null &&
                            Objects.equals("KafkaTopic", event.getInvolvedObject().getKind()) &&
                            Objects.equals(kafkaTopic.getMetadata().getName(), event.getInvolvedObject().getName()));
        }, timeout, "Expected an error event");
    }

    protected MixedOperation<KafkaTopic, KafkaTopicList, DoneableKafkaTopic, Resource<KafkaTopic, DoneableKafkaTopic>> operation() {
        return kubeClient.customResources(Crds.topic(), KafkaTopic.class, KafkaTopicList.class, DoneableKafkaTopic.class);
    }

    protected void waitForTopicInKafka(TestContext context, String topicName) {
        waitForTopicInKafka(context, topicName, true);
    }

    protected void waitForTopicInKafka(TestContext context, String topicName, boolean exist) {
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

    protected void deleteInKubeAndAwaitReconciliation(TestContext context, String topicName, KafkaTopic topicResource) {
        deleteInKube(context, topicResource.getMetadata().getName());

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

    protected void deleteInKube(TestContext context, String resourceName) {
        // can now delete the topicResource
        operation().inNamespace(NAMESPACE).withName(resourceName).delete();
        waitFor(context, () -> {
            return operation().inNamespace(NAMESPACE).withName(resourceName).get() == null;
        }, Long.MAX_VALUE, "verified deletion of KafkaTopic " + resourceName);
    }

    protected void awaitTopicConfigInKafka(TestContext context, String topicName, String key, String expectedValue) {
        // Wait for that to be reflected in the kafka topic
        waitFor(context, () -> {
            ConfigResource configResource = topicConfigResource(topicName);
            org.apache.kafka.clients.admin.Config config = getTopicConfig(configResource);
            String retention = config.get("retention.ms").value();
            LOGGER.debug("retention of {}, waiting for 12341234", retention);
            return expectedValue.equals(retention);
        },  timeout, "Expected the topic " + topicName + " to have retention.ms=" + expectedValue + " in Kafka");
    }

    protected String alterTopicConfigInKube(String resourceName, String key, Function<String, String> mutator) {
        // now change the topic resource
        Object retention = operation().inNamespace(NAMESPACE).withName(resourceName).get().getSpec().getConfig().getOrDefault(key, "12341233");
        String currentValue = retention instanceof Integer ? retention.toString() : (String) retention;
        String newValue = mutator.apply(currentValue);
        KafkaTopic changedTopic = new KafkaTopicBuilder(operation().inNamespace(NAMESPACE).withName(resourceName).get())
                .editOrNewSpec().addToConfig(key, newValue).endSpec().build();
        operation().inNamespace(NAMESPACE).withName(resourceName).replace(changedTopic);
        return newValue;
    }
}

