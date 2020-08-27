/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
import io.strimzi.api.kafka.model.status.KafkaTopicStatus;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.k8s.cluster.KubeCluster;
import io.strimzi.test.k8s.exceptions.NoClusterException;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@SuppressWarnings("checkstyle:ClassFanOutComplexity")
public abstract class TopicOperatorBaseIT {

    private static final Logger LOGGER = LogManager.getLogger(TopicOperatorBaseIT.class);

    private static KubeClusterResource cluster = KubeClusterResource.getInstance();

    protected static String oldNamespace;

    protected final Labels labels = Labels.fromString(io.strimzi.operator.common.model.Labels.STRIMZI_KIND_LABEL + "=topic");

    public static final String NAMESPACE = "topic-operator-it";

    protected static Vertx vertx;
    protected KafkaCluster kafkaCluster;
    protected volatile AdminClient adminClient;
    protected KubernetesClient kubeClient;

    protected volatile String deploymentId;
    protected Set<String> preExistingEvents;

    protected Session session;

    @BeforeAll
    public static void setupKubeCluster() throws IOException {
        VertxOptions options = new VertxOptions().setMetricsOptions(
                new MicrometerMetricsOptions()
                        .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true))
                        .setEnabled(true));
        vertx = Vertx.vertx(options);
        try {
            KubeCluster.bootstrap();
        } catch (NoClusterException e) {
            assumeTrue(false, e.getMessage());
        }
        cmdKubeClient().createNamespace(NAMESPACE);
        oldNamespace = cluster.setNamespace(NAMESPACE);
        LOGGER.info("#### Creating " + "../install/topic-operator/02-Role-strimzi-topic-operator.yaml");
        LOGGER.info(new String(Files.readAllBytes(new File("../install/topic-operator/02-Role-strimzi-topic-operator.yaml").toPath())));
        cmdKubeClient().create(TestUtils.USER_PATH + "/../install/topic-operator/02-Role-strimzi-topic-operator.yaml");
        LOGGER.info("#### Creating " + TestUtils.CRD_TOPIC);
        LOGGER.info(new String(Files.readAllBytes(new File(TestUtils.CRD_TOPIC).toPath())));
        cmdKubeClient().create(TestUtils.CRD_TOPIC);
        LOGGER.info("#### Creating " + "src/test/resources/TopicOperatorIT-rbac.yaml");
        LOGGER.info(new String(Files.readAllBytes(new File("src/test/resources/TopicOperatorIT-rbac.yaml").toPath())));

        cmdKubeClient().create("src/test/resources/TopicOperatorIT-rbac.yaml");
    }

    @AfterAll
    public static void teardownKubeCluster() {
        CountDownLatch latch = new CountDownLatch(1);
        if (oldNamespace != null) {
            cmdKubeClient()
                    .delete("src/test/resources/TopicOperatorIT-rbac.yaml")
                    .delete(TestUtils.CRD_TOPIC)
                    .delete(TestUtils.USER_PATH + "/../install/topic-operator/02-Role-strimzi-topic-operator.yaml")
                    .deleteNamespace(NAMESPACE);
            cmdKubeClient().namespace(oldNamespace);
        }
        vertx.close(result -> {
            latch.countDown();
        });
        try {
            latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.error(e);
        }
    }

    @BeforeEach
    public void setup() throws Exception {
        LOGGER.info("Setting up test");
        cluster.before();
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

        kubeClient = kubeClient().getClient();
        Crds.registerCustomKinds();
        LOGGER.info("Using namespace {}", NAMESPACE);
        startTopicOperator();

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

    @AfterEach
    public void teardown() throws InterruptedException, TimeoutException, ExecutionException {
        CountDownLatch latch = new CountDownLatch(1);
        try {
            LOGGER.info("Tearing down test");

            boolean deletionEnabled = "true".equals(kafkaClusterConfig().getOrDefault(
                    KafkaConfig$.MODULE$.DeleteTopicEnableProp(), "true"));

            if (deletionEnabled && kubeClient != null) {
                List<KafkaTopic> items = operation().inNamespace(NAMESPACE).list().getItems();

                // Wait for the operator to delete all the existing topics in Kafka
                for (KafkaTopic item : items) {
                    LOGGER.info("Deleting {} from Kube", item.getMetadata().getName());
                    operation().inNamespace(NAMESPACE).withName(item.getMetadata().getName()).cascading(true).delete();
                    LOGGER.info("Awaiting deletion of {} in Kafka", item.getMetadata().getName());
                    waitForTopicInKafka(new TopicName(item).toString(), false);
                    waitForTopicInKube(item.getMetadata().getName(), false);
                }
                Thread.sleep(5_000);
            }

            stopTopicOperator();

            if (!deletionEnabled && kubeClient != null) {
                List<KafkaTopic> items = operation().inNamespace(NAMESPACE).list().getItems();

                // Wait for the operator to delete all the existing topics in Kafka
                for (KafkaTopic item : items) {
                    operation().inNamespace(NAMESPACE).withName(item.getMetadata().getName()).cascading(true).delete();
                    waitForTopicInKube(item.getMetadata().getName(), false);
                }
            }
        } finally {

            adminClient.close();
            if (kafkaCluster != null) {
                try {
                    kafkaCluster.shutdown();
                } catch (Exception e) {
                    LOGGER.warn(e);
                }
            }
            LOGGER.info("Finished tearing down test");
            latch.countDown();
        }
        latch.await(30, TimeUnit.SECONDS);
    }

    protected void startTopicOperator() throws InterruptedException, ExecutionException, TimeoutException {

        LOGGER.info("Starting Topic Operator");
        session = new Session(kubeClient, new Config(topicOperatorConfig()));

        CompletableFuture<Void> async = new CompletableFuture<>();
        vertx.deployVerticle(session, ar -> {
            if (ar.succeeded()) {
                deploymentId = ar.result();
                async.complete(null);
            } else {
                async.completeExceptionally(ar.cause());
            }
        });
        async.get(60, TimeUnit.SECONDS);
        LOGGER.info("Started Topic Operator");
    }

    protected Map<String, String> topicOperatorConfig() {
        Map<String, String> m = new HashMap<>();
        m.put(Config.KAFKA_BOOTSTRAP_SERVERS.key, kafkaCluster.brokerList());
        m.put(Config.ZOOKEEPER_CONNECT.key, "localhost:" + zkPort(kafkaCluster));
        m.put(Config.ZOOKEEPER_CONNECTION_TIMEOUT_MS.key, "30000");
        m.put(Config.NAMESPACE.key, NAMESPACE);
        m.put(Config.TC_RESOURCE_LABELS, io.strimzi.operator.common.model.Labels.STRIMZI_KIND_LABEL + "=topic");
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

    protected void stopTopicOperator() throws InterruptedException, ExecutionException, TimeoutException {
        LOGGER.info("Stopping Topic Operator");
        CompletableFuture<Void> async = new CompletableFuture<>();
        if (deploymentId != null) {
            vertx.undeploy(deploymentId, ar -> {
                deploymentId = null;
                if (ar.failed()) {
                    LOGGER.error("Error undeploying session", ar.cause());
                    async.completeExceptionally(ar.cause());
                } else {
                    async.complete(null);
                }
            });
        }
        async.get(60, TimeUnit.SECONDS);
        LOGGER.info("Stopped Topic Operator");
    }

    protected KafkaTopic createKafkaTopicResource(KafkaTopic topicResource) throws InterruptedException, ExecutionException, TimeoutException {
        String topicName = new TopicName(topicResource).toString();
        // Create a Topic Resource
        operation().inNamespace(NAMESPACE).create(topicResource);

        // Wait for the topic to be created
        waitForTopicInKafka(topicName);
        assertStatusReady(topicResource.getMetadata().getName());
        return topicResource;
    }

    protected void assertStatusReady(String topicName) throws InterruptedException, ExecutionException, TimeoutException {
        waitFor(() -> {
            KafkaTopic kafkaTopic = operation().inNamespace(NAMESPACE).withName(topicName).get();
            if (kafkaTopic != null) {
                KafkaTopicStatus status = kafkaTopic.getStatus();
                if (status != null
                        && Objects.equals(status.getObservedGeneration(), kafkaTopic.getMetadata().getGeneration())
                        && status.getConditions() != null) {
                    List<Condition> conditions = status.getConditions();
                    assertThat(conditions.size() > 0, is(true));
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
        }, "status ready for topic " + topicName);
    }

    protected void assertStatusNotReady(String topicName, String message) throws InterruptedException, ExecutionException, TimeoutException {
        assertStatusNotReady(topicName, null, message);
    }

    protected void assertStatusNotReady(String topicName, Class<? extends Exception> reason, String message) throws InterruptedException, ExecutionException, TimeoutException {
        waitFor(() -> {
            KafkaTopic kafkaTopic = operation().inNamespace(NAMESPACE).withName(topicName).get();
            if (kafkaTopic != null) {
                KafkaTopicStatus status = kafkaTopic.getStatus();
                if (status != null
                        && Objects.equals(status.getObservedGeneration(), kafkaTopic.getMetadata().getGeneration())
                        && status.getConditions() != null) {
                    List<Condition> conditions = status.getConditions();
                    assertThat(conditions.size() > 0, is(true));
                    Optional<Condition> unreadyCondition = conditions.stream().filter(condition ->
                            "NotReady".equals(condition.getType()) &&
                                    "True".equals(condition.getStatus())).findFirst();
                    if (unreadyCondition.isPresent()) {
                        if (reason != null) {
                            assertThat(unreadyCondition.get().getReason() + ": " + unreadyCondition.get().getMessage(), is(reason.getSimpleName() + ": " + message));
                        } else {
                            assertThat(unreadyCondition.get().getMessage(), is(message));
                        }
                        return true;
                    } else {
                        LOGGER.info(conditions);
                    }
                }
            } else {
                LOGGER.info("{} does not exist", topicName);
            }
            return false;
        }, "status ready");
    }

    protected KafkaTopic createKafkaTopicResource(String topicName) throws InterruptedException, ExecutionException, TimeoutException {
        Topic topic = new Topic.Builder(topicName, 1, (short) 1, emptyMap()).build();
        KafkaTopic topicResource = TopicSerialization.toTopicResource(topic, labels);
        return createKafkaTopicResource(topicResource);
    }

    /**
     * Create a topic in Kafka with a single partition and RF=1.
     * @param topicName The name of the topic.
     * @return The name of the KafkaTopic resource that was created in Kube.
     * @throws InterruptedException
     * @throws ExecutionException
     */
    protected String createTopic(String topicName) throws InterruptedException, ExecutionException, TimeoutException {
        return createTopic(topicName, new NewTopic(topicName, 1, (short) 1));
    }

    /**
     * Create a topic in Kafka with a single partition and the given replica assignments
     * @param topicName The name of the topic.
     * @param replicaAssignments The replica assignments.
     * @return The name of the KafkaTopic resource that was created in Kube.
     * @throws InterruptedException
     * @throws ExecutionException
     */
    protected String createTopic(String topicName, List<Integer> replicaAssignments) throws InterruptedException, ExecutionException, TimeoutException {
        return createTopic(topicName, new NewTopic(topicName, singletonMap(0, replicaAssignments)));
    }

    protected String createTopic(String topicName, NewTopic o) throws InterruptedException, ExecutionException, TimeoutException {
        LOGGER.info("Creating topic {}", topicName);
        // Create a topic
        String resourceName = new TopicName(topicName).asKubeName().toString();
        CreateTopicsResult crt = adminClient.createTopics(singletonList(o));
        crt.all().get();

        // Wait for the resource to be created
        waitForTopicInKube(resourceName);

        LOGGER.info("topic {} has been created", resourceName);
        return resourceName;
    }

    protected void waitForTopicInKube(String resourceName) throws InterruptedException, ExecutionException, TimeoutException {
        waitForTopicInKube(resourceName, true);
    }

    protected void waitForTopicInKube(String resourceName, boolean exist) throws TimeoutException, InterruptedException {
        waitFor(() -> {
            KafkaTopic topic = operation().inNamespace(NAMESPACE).withName(resourceName).get();
            LOGGER.info("Polled topic {} waiting for " + (exist ? "existence" : "non-existence"), resourceName);
            return topic != null == exist;
        }, "Expected the KafkaTopic '" + resourceName + "' to " + (exist ? "exist" : "not exist") + " in Kubernetes by now");
    }

    protected void alterTopicConfigInKafkaAndAwaitReconciliation(String topicName, String resourceName) throws InterruptedException, ExecutionException, TimeoutException {
        String key = "compression.type";
        final String changedValue = alterTopicConfigInKafka(topicName, key, value -> "snappy".equals(value) ? "lz4" : "snappy");
        awaitTopicConfigInKube(resourceName, key, changedValue);
    }

    protected void awaitTopicConfigInKube(String resourceName, String key, String expectedValue) throws TimeoutException, InterruptedException {

        // Wait for the resource to be modified
        waitFor(() -> {
            KafkaTopic topic = operation().inNamespace(NAMESPACE).withName(resourceName).get();
            LOGGER.info("Polled topic {}, waiting for config change", resourceName);
            String gotValue = TopicSerialization.fromTopicResource(topic).getConfig().get(key);
            LOGGER.info("Expecting value {}, got value {}", expectedValue, gotValue);
            return expectedValue.equals(gotValue);
        }, "Expected the config of topic " + resourceName + " to have " + key + "=" + expectedValue + " in Kube by now");
    }

    protected String alterTopicConfigInKafka(String topicName, String key, Function<String, String> mutator) throws InterruptedException, ExecutionException {
        // Get the topic config
        ConfigResource configResource = topicConfigResource(topicName);
        org.apache.kafka.clients.admin.Config config = getTopicConfig(configResource);

        Map<String, ConfigEntry> m = new HashMap<>();
        for (ConfigEntry entry: config.entries()) {
            if (entry.name().equals(key)
                || entry.source() != ConfigEntry.ConfigSource.DEFAULT_CONFIG
                    && entry.source() != ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG) {
                m.put(entry.name(), entry);
            }
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

    protected void alterTopicNumPartitions(String topicName, String resourceName) throws InterruptedException, ExecutionException, TimeoutException {
        int changedValue = 2;

        NewPartitions newPartitions = NewPartitions.increaseTo(changedValue);
        Map<String, NewPartitions> map = new HashMap<>(1);
        map.put(topicName, newPartitions);

        CreatePartitionsResult createPartitionsResult = adminClient.createPartitions(map);
        createPartitionsResult.all().get();

        // Wait for the resource to be modified
        waitFor(() -> {
            KafkaTopic topic = operation().inNamespace(NAMESPACE).withName(resourceName).get();
            LOGGER.info("Polled topic {}, waiting for partitions change", resourceName);
            int gotValue = TopicSerialization.fromTopicResource(topic).getNumPartitions();
            LOGGER.info("Expected value {}, got value {}", changedValue, gotValue);
            return changedValue == gotValue;
        }, "Expected the topic " + topicName + "to have " + changedValue + " partitions by now");
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

    protected void createAndAlterTopicConfig(String topicName) throws InterruptedException, ExecutionException, TimeoutException {
        String resourceName = createTopic(topicName);
        alterTopicConfigInKafkaAndAwaitReconciliation(topicName, resourceName);
    }

    protected void deleteTopicInKafkaAndAwaitReconciliation(String topicName, String resourceName) throws InterruptedException, ExecutionException, TimeoutException {
        deleteTopicInKafka(topicName, resourceName);

        // Wait for the resource to be deleted
        waitFor(() -> {
            KafkaTopic topic = operation().inNamespace(NAMESPACE).withName(resourceName).get();
            LOGGER.info("Polled topic {}, got {}, waiting for deletion", resourceName, topic);
            return topic == null;
        }, "Expected the topic " + topicName + " to have been deleted by now");
    }

    protected void deleteTopicInKafka(String topicName, String resourceName) throws InterruptedException, ExecutionException {
        LOGGER.info("Deleting topic {} (KafkaTopic {})", topicName, resourceName);
        // Now we can delete the topic
        DeleteTopicsResult dlt = adminClient.deleteTopics(singletonList(topicName));
        dlt.all().get();
        LOGGER.info("Deleted topic {}", topicName);
    }

    protected void createAndDeleteTopic(String topicName) throws InterruptedException, ExecutionException, TimeoutException {
        String resourceName = createTopic(topicName);
        deleteTopicInKafkaAndAwaitReconciliation(topicName, resourceName);
    }

    protected void createAndAlterNumPartitions(String topicName) throws InterruptedException, ExecutionException, TimeoutException {
        String resourceName = createTopic(topicName);
        alterTopicNumPartitions(topicName, resourceName);
    }

    protected void waitFor(BooleanSupplier ready, String message) throws TimeoutException, InterruptedException {
        // Note that this timeout for an individual wait must be less than
        // the Vertx @Timeout for the test as a whole
        long timeout = 120_000;
        long deadline = System.currentTimeMillis() + timeout;

        while (System.currentTimeMillis() < deadline) {
            try {
                if (ready.getAsBoolean()) {
                    return;
                }
            } catch (Throwable t) {
                throw new AssertionError("Exception from condition while waiting for " + message, t);
            }
            Thread.sleep(3_000);
        }
        throw new TimeoutException("Timeout waiting for " + message);
    }

    protected void waitForEvent(KafkaTopic kafkaTopic, String expectedMessage, TopicOperator.EventType expectedType) throws InterruptedException, ExecutionException, TimeoutException {
        waitFor(() -> {
            List<Event> items = kubeClient.events().inNamespace(NAMESPACE).withLabels(labels.labels()).list().getItems();
            List<Event> filtered = items.stream().
                    filter(evt -> !preExistingEvents.contains(evt.getMetadata().getUid())
                            && "KafkaTopic".equals(evt.getInvolvedObject().getKind())
                            && kafkaTopic.getMetadata().getName().equals(evt.getInvolvedObject().getName())).
                    collect(Collectors.toList());
            LOGGER.debug("Waiting for events: {}", filtered.stream().map(evt -> evt.getMessage()).collect(Collectors.toList()));
            return filtered.stream().anyMatch(event ->
                    Pattern.matches(expectedMessage, event.getMessage()) &&
                        Objects.equals(expectedType.name, event.getType()) &&
                        event.getInvolvedObject() != null &&
                        event.getLastTimestamp() != null &&
                        Objects.equals("KafkaTopic", event.getInvolvedObject().getKind()) &&
                        Objects.equals(kafkaTopic.getMetadata().getName(), event.getInvolvedObject().getName()));
        }, "Expected an error event");
    }

    protected MixedOperation<KafkaTopic, KafkaTopicList, DoneableKafkaTopic, Resource<KafkaTopic, DoneableKafkaTopic>> operation() {
        return kubeClient.customResources(Crds.kafkaTopic(), KafkaTopic.class, KafkaTopicList.class, DoneableKafkaTopic.class);
    }

    protected void waitForTopicInKafka(String topicName) throws InterruptedException, ExecutionException, TimeoutException {
        waitForTopicInKafka(topicName, true);
    }

    protected void waitForTopicInKafka(String topicName, boolean exist) throws TimeoutException, InterruptedException {
        waitFor(() -> {
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
        }, "Expected topic '" + topicName + "' to " + (exist ? "exist" : "not exist") + " in Kafka by now");
    }

    protected void deleteInKubeAndAwaitReconciliation(String topicName, KafkaTopic topicResource) throws InterruptedException, ExecutionException, TimeoutException {
        deleteInKube(topicResource.getMetadata().getName());

        // Wait for the topic to be deleted
        waitFor(() -> {
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
        }, "Expected topic to be deleted by now");
    }

    protected void deleteInKube(String resourceName) throws InterruptedException, ExecutionException, TimeoutException {
        // can now delete the topicResource
        operation().inNamespace(NAMESPACE).withName(resourceName).cascading(true).delete();
        waitFor(() -> {
            return operation().inNamespace(NAMESPACE).withName(resourceName).get() == null;
        }, "verified deletion of KafkaTopic " + resourceName);
    }

    protected void awaitTopicConfigInKafka(String topicName, String key, String expectedValue) throws InterruptedException, ExecutionException, TimeoutException {
        // Wait for that to be reflected in the kafka topic
        waitFor(() -> {
            ConfigResource configResource = topicConfigResource(topicName);
            org.apache.kafka.clients.admin.Config config = getTopicConfig(configResource);
            String retention = config.get("retention.ms").value();
            LOGGER.debug("retention of {}, waiting for 12341234", retention);
            return expectedValue.equals(retention);
        },  "Expected the topic " + topicName + " to have retention.ms=" + expectedValue + " in Kafka");
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

