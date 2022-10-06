/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Status;
import io.fabric8.kubernetes.api.model.StatusBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.WatcherException;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaTopicBuilder;
import io.strimzi.test.container.StrimziKafkaCluster;
import kafka.server.KafkaConfig$;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

public class TopicOperatorIT extends TopicOperatorBaseIT {
    private static final Logger LOGGER = LogManager.getLogger(TopicOperatorIT.class);
    protected static StrimziKafkaCluster kafkaCluster;

    @BeforeAll
    public void beforeAll() throws Exception {
        kafkaCluster = new StrimziKafkaCluster(numKafkaBrokers(), numKafkaBrokers(), kafkaClusterConfig());
        kafkaCluster.start();

        setupKubeCluster();
        setup(kafkaCluster);

        LOGGER.info("Using namespace {}", NAMESPACE);
        startTopicOperator(kafkaCluster);
    }

    @AfterAll
    public void afterAll() throws InterruptedException, ExecutionException, TimeoutException {
        try {
            teardown(true);
        } finally {
            teardownKubeCluster();
            adminClient.close();
            kafkaCluster.stop();
        }
    }

    @AfterEach
    void afterEach() throws InterruptedException, TimeoutException {
        // clean-up KafkaTopic resources in Kubernetes
        clearKafkaTopics(true);
    }

    protected static int numKafkaBrokers() {
        return 1;
    }

    protected static Map<String, String> kafkaClusterConfig() {
        Map<String, String> p = new HashMap<>();
        p.put(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), "false");
        p.put("zookeeper.connect", "zookeeper:2181");
        return p;
    }


    @Test
    public void testTopicAdded() throws Exception {
        createTopic("test-topic-added");
    }

    @Test
    public void testTopicAddedWithReplicasAndPartitions() throws Exception {
        createTopic("test-topic-added-rp", 1, (short) 1);
    }

    @Test
    public void testTopicAddedWithEncodableName() throws Exception {
        createTopic("test-TOPIC_ADDED_ENCODABLE");
    }

    @Test
    public void testTopicDeleted() throws Exception {
        createAndDeleteTopic("test-topic-deleted");
    }

    @Test
    public void testTopicDeletedWithEncodableName() throws Exception {
        createAndDeleteTopic("test-TOPIC_DELETED_ENCODABLE");
    }

    @Test
    public void testTopicConfigChanged() throws Exception {
        createAndAlterTopicConfig("test-topic-config-changed");
    }

    @Test
    public void testTopicConfigChangedWithEncodableName() throws Exception {
        createAndAlterTopicConfig("test-TOPIC_CONFIG_CHANGED_ENCODABLE");
    }

    @Test
    public void testTopicNumPartitionsChanged() throws Exception {
        createAndAlterNumPartitions("test-topic-partitions-changed");
    }

    @Test
    public void testTopicNumPartitionsDecreased() throws Exception {
        String topicName = "topic-partitions-decreased-in-kube";
        String resourceName = createTopic(topicName, new NewTopic(topicName, 2, (short) 1));
        KafkaTopic changedTopic = new KafkaTopicBuilder(operation().inNamespace(NAMESPACE).withName(resourceName).get())
                .editOrNewSpec().withPartitions(1).endSpec().build();
        KafkaTopic replaced = operation().inNamespace(NAMESPACE).withName(resourceName).replace(changedTopic);
        assertStatusNotReady(topicName, PartitionDecreaseException.class,
                "Number of partitions cannot be decreased");
        Long generation = operation().inNamespace(NAMESPACE).withName(resourceName).get().getMetadata().getGeneration();
        // Now modify Kafka-side to cause another reconciliation: We want the same status.
        alterTopicConfigInKafka(topicName, "compression.type", value -> "snappy".equals(value) ? "lz4" : "snappy");
        // Wait for a periodic reconciliation
        Thread.sleep(RECONCILIATION_INTERVAL + 10_000);
        assertStatusNotReady(topicName, PartitionDecreaseException.class,
                "Number of partitions cannot be decreased");
    }

    @Test
    public void testInvalidConfig() throws Exception {
        String topicName = "topic-invalid-config";
        String expectedMessage = "Invalid config value for resource ConfigResource(type=TOPIC, name='" + topicName + "'): Invalid value x for configuration min.insync.replicas: Not a number of type INT";

        String resourceName = createTopic(topicName, new NewTopic(topicName, 2, (short) 1));
        KafkaTopic changedTopic = new KafkaTopicBuilder(operation().inNamespace(NAMESPACE).withName(resourceName).get())
                .editOrNewSpec().addToConfig("min.insync.replicas", "x").endSpec().build();
        KafkaTopic replaced = operation().inNamespace(NAMESPACE).withName(resourceName).replace(changedTopic);
        assertStatusNotReady(topicName, InvalidRequestException.class,
                expectedMessage);
        // Now modify Kafka-side to cause another reconciliation: We want the same status.
        alterTopicConfigInKafka(topicName, "compression.type", value -> "snappy".equals(value) ? "lz4" : "snappy");
        // Wait for a periodic reconciliation
        Thread.sleep(RECONCILIATION_INTERVAL + 10_000);
        assertStatusNotReady(topicName, InvalidRequestException.class,
                expectedMessage);
    }

    @Test
    public void testKafkaTopicAdded() throws InterruptedException, ExecutionException, TimeoutException {
        String topicName = "test-kafkatopic-created";
        createKafkaTopicResource(topicName);
    }

    void createKafkaTopicResourceError(String topicName, Map<String, String> objectObjectMap, int rf,
                                       String expectedMessage) throws InterruptedException, ExecutionException, TimeoutException {
        Topic topic = new Topic.Builder(topicName, 1, (short) 1, objectObjectMap).build();
        KafkaTopic kafkaTopic = TopicSerialization.toTopicResource(topic, labels);
        kafkaTopic.getSpec().setReplicas(rf);

        // Create a Topic Resource
        operation().inNamespace(NAMESPACE).create(kafkaTopic);
        assertStatusNotReady(topicName, expectedMessage);
    }

    @Test
    public void testKafkaTopicAddedWithMoreReplicasThanBrokers() throws InterruptedException, ExecutionException, TimeoutException {
        createKafkaTopicResourceError("test-resource-created-with-more-replicas-than-brokers", emptyMap(), 42, "org.apache.kafka.common.errors.InvalidReplicationFactorException: Replication factor: 42 larger than available brokers: 1.");
    }

    @Test
    public void testKafkaTopicAddedWithHigherMinIsrThanBrokers() throws InterruptedException, ExecutionException, TimeoutException {
        createKafkaTopicResourceError("test-resource-created-with-higher-min-isr-than-brokers",
                singletonMap("min.insync.replicas", "42"), 42,
               "org.apache.kafka.common.errors.InvalidReplicationFactorException: Replication factor: 42 larger than available brokers: 1.");
    }

    @Test
    public void testKafkaTopicAddedWithUnknownConfig() throws InterruptedException, ExecutionException, TimeoutException {
        createKafkaTopicResourceError("test-resource-created-with-unknown-config",
                singletonMap("aardvark", "zebra"), 1,
               "org.apache.kafka.common.errors.InvalidConfigurationException: Unknown topic config name: aardvark");
    }

    @Test
    public void testKafkaTopicAddedWithInvalidConfig() throws InterruptedException, ExecutionException, TimeoutException {
        createKafkaTopicResourceError("test-resource-created-with-invalid-config",
                singletonMap("message.format.version", "zebra"), 1,
               "org.apache.kafka.common.errors.InvalidConfigurationException: Invalid value zebra for configuration message.format.version: Version `zebra` is not a valid version");
    }

    @Test
    public void testKafkaTopicAddedWithBadData() {
        String topicName = "test-resource-created-with-bad-data";
        Topic topic = new Topic.Builder(topicName, 1, (short) 1, emptyMap()).build();
        KafkaTopic kafkaTopic = TopicSerialization.toTopicResource(topic, labels);
        kafkaTopic.getSpec().setPartitions(-1);

        // Create a Topic Resource
        try {
            operation().inNamespace(NAMESPACE).create(kafkaTopic);
            fail();
        } catch (KubernetesClientException e) {
            assertThat(e.getMessage().contains("spec.partitions in body should be greater than or equal to 1"), is(true));
        }
    }

    @Test
    public void testKafkaTopicDeleted() throws InterruptedException, ExecutionException, TimeoutException {
        // create the Topic Resource
        String topicName = "test-kafkatopic-deleted";
        KafkaTopic topicResource = createKafkaTopicResource(topicName);
        deleteInKubeAndAwaitReconciliation(topicName, topicResource);
    }

    @Test
    public void testKafkaTopicModifiedRetentionChanged() throws Exception {
        // create the topic
        String topicName = "test-kafkatopic-modified-retention-changed";
        KafkaTopic topicResource = createKafkaTopicResource(topicName);
        String expectedValue = alterTopicConfigInKube(topicResource.getMetadata().getName(),
                "retention.ms", currentValue -> Integer.toString(Integer.parseInt(currentValue) + 1));
        awaitTopicConfigInKafka(topicName, "retention.ms", expectedValue);
    }

    @Test
    public void testKafkaTopicModifiedWithBadData() throws Exception {
        // create the topicResource
        String topicName = "test-kafkatopic-modified-with-bad-data";
        KafkaTopic topicResource = createKafkaTopicResource(topicName);

        // now change the topicResource
        KafkaTopic changedTopic = new KafkaTopicBuilder(operation().inNamespace(NAMESPACE).withName(topicResource.getMetadata().getName()).get())
                .editOrNewSpec().withPartitions(-1).endSpec().build();
        try {
            operation().inNamespace(NAMESPACE).withName(topicResource.getMetadata().getName()).replace(changedTopic);
        } catch (KubernetesClientException e) {
            assertThat(e.getMessage().contains("spec.partitions in body should be greater than or equal to 1"), is(true));
        }
    }

    @Test
    public void testKafkaTopicModifiedNameChanged() throws Exception {
        // create the topicResource
        String topicName = "test-kafkatopic-modified-name-changed";
        KafkaTopic topicResource = createKafkaTopicResource(topicName);

        // now change the topicResource
        String changedName = topicName.toUpperCase(Locale.ENGLISH);
        LOGGER.info("Changing Topic Resource spec.topicName from {} to {}", topicName, changedName);
        KafkaTopic changedTopic = new KafkaTopicBuilder(operation().inNamespace(NAMESPACE).withName(topicResource.getMetadata().getName()).get())
                .editOrNewSpec().withTopicName(changedName).endSpec().build();
        operation().inNamespace(NAMESPACE).withName(topicResource.getMetadata().getName()).replace(changedTopic);

        // We expect this to cause a warning event
        waitForEvent(topicResource,
                "Kafka topics cannot be renamed, but KafkaTopic's spec.topicName has changed.",
                TopicOperator.EventType.WARNING);

    }

    @Test
    public void testCreateTwoResourcesManagingOneTopic() throws InterruptedException, ExecutionException, TimeoutException {
        String topicName = "two-resources-one-topic";
        Topic topic = new Topic.Builder(topicName, 1, (short) 1, emptyMap()).build();
        KafkaTopic topicResource = TopicSerialization.toTopicResource(topic, labels);
        KafkaTopic topicResource2 = new KafkaTopicBuilder(topicResource).withMetadata(new ObjectMetaBuilder(topicResource.getMetadata()).withName(topicName + "-1").build()).build();
        // create one
        createKafkaTopicResource(topicResource2);
        // create another
        operation().inNamespace(NAMESPACE).create(topicResource);

        waitForEvent(topicResource,
                "Failure processing KafkaTopic watch event ADDED on resource two-resources-one-topic with labels \\{.*\\}: " +
                        "Topic 'two-resources-one-topic' is already managed via KafkaTopic 'two-resources-one-topic-1' it cannot also be managed via the KafkaTopic 'two-resources-one-topic'",
                TopicOperator.EventType.WARNING);
    }


    @Test
    public void testReconcile() throws InterruptedException, ExecutionException, TimeoutException {
        String topicName = "test-reconcile";

        Topic topic = new Topic.Builder(topicName, 1, (short) 1, emptyMap()).build();
        KafkaTopic topicResource = TopicSerialization.toTopicResource(topic, labels);
        String resourceName = topicResource.getMetadata().getName();

        operation().inNamespace(NAMESPACE).create(topicResource);

        // Wait for the resource to be created
        waitFor(() -> {
            KafkaTopic createdResource = operation().inNamespace(NAMESPACE).withName(resourceName).get();
            LOGGER.info("Polled kafkatopic {} waiting for creation", resourceName);

            // modify resource
            if (createdResource != null) {
                createdResource.getSpec().setPartitions(2);
                operation().inNamespace(NAMESPACE).withName(resourceName).patch(createdResource);
            }

            return createdResource != null;
        }, "Expected the kafkatopic to have been created by now");

        // trigger an immediate reconcile, while topic operator is dealing with resource modification
        session.topicOperator.reconcileAllTopics("periodic");

        // Wait for the topic to be created
        waitForTopicInKafka(topicName);
        assertStatusReady(topicName);
    }

    @Test
    public void testRecreateTopicWatcher() throws InterruptedException, ExecutionException, TimeoutException {
        String topicName = "test-reconcile";
        String topicName2 = "test-reconcile-2";

        Topic topic = new Topic.Builder(topicName, 1, (short) 1, emptyMap()).build();
        KafkaTopic topicResource = TopicSerialization.toTopicResource(topic, labels);
        String resourceName = topicResource.getMetadata().getName();

        operation().inNamespace(NAMESPACE).create(topicResource);

        // Wait for the resource to be created
        waitFor(() -> {
            KafkaTopic createdResource = operation().inNamespace(NAMESPACE).withName(resourceName).get();
            LOGGER.info("Polled kafkatopic {} waiting for creation", resourceName);

            // modify resource
            if (createdResource != null) {
                createdResource.getSpec().setPartitions(2);
                operation().inNamespace(NAMESPACE).withName(resourceName).patch(createdResource);
            }

            return createdResource != null;
        }, "Expected the kafkatopic to have been created by now");

        Status status = new StatusBuilder()
                .withStatus("pokazene")
                .withCode(HttpURLConnection.HTTP_GONE)
                .withMessage("pokazene")
                .build();
        WatcherException e = new WatcherException(status.toString());
        LOGGER.info("stopping TW");
        session.topicWatch.close();
        session.topicsWatcher.stop();
        session.watcher.onClose(e);

        // trigger an immediate reconcile, while topic operator is dealing with resource modification
        session.topicOperator.reconcileAllTopics("periodic");

        // Wait for the topic to be created
        waitForTopicInKafka(topicName);
        assertStatusReady(topicName);

        Topic topic2 = new Topic.Builder(topicName2, 1, (short) 1, emptyMap()).build();
        KafkaTopic topicResource2 = TopicSerialization.toTopicResource(topic2, labels);
        String resourceName2 = topicResource2.getMetadata().getName();

        operation().inNamespace(NAMESPACE).create(topicResource2);

        // Wait for the resource to be created
        waitFor(() -> {
            KafkaTopic createdResource = operation().inNamespace(NAMESPACE).withName(resourceName2).get();
            LOGGER.info("Polled kafkatopic {} waiting for creation", resourceName2);

            // modify resource
            if (createdResource != null) {
                createdResource.getSpec().setPartitions(2);
                operation().inNamespace(NAMESPACE).withName(resourceName2).patch(createdResource);
            }
            return createdResource != null;
        }, "Expected the kafkatopic to have been created by now");

        // I assume the test should fail because of topic `test-reconcile-2` should not exist i
        waitForTopicInKafka(topicName2);
        assertStatusReady(topicName2);
    }

    // TODO: What happens if we create and then change labels to the resource predicate isn't matched any more
    //       What then happens if we change labels back?

    @Test
    public void testKafkaTopicWithOwnerRef() throws InterruptedException, ExecutionException, TimeoutException {
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
        createKafkaTopicResource(topicResource);
        assertThat(operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getOwnerReferences().size(), is(1));
        assertThat(operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getOwnerReferences().get(0).getUid(), is(uid));
        assertThat(operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getAnnotations().size(), is(1));
        assertThat(operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getAnnotations().get("iam"), is("groot"));
        assertThat(operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getLabels().size(), is(2));
        assertThat(operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getLabels().get("iam"), is("root"));

        // edit kafka topic
        topicName = "test-kafka-topic-with-owner-ref-2";
        Topic topic2 = new Topic.Builder(topicName, 1, (short) 1, emptyMap(), metadata).build();
        KafkaTopic topicResource2 = TopicSerialization.toTopicResource(topic2, labels);
        topicResource = TopicSerialization.toTopicResource(topic2, labels);
        topicResource.getMetadata().getAnnotations().put("han", "solo");
        createKafkaTopicResource(topicResource2);
        assertThat(operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getOwnerReferences().get(0).getUid(), is(uid));
        assertThat(operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getAnnotations().size(), is(2));
        assertThat(operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getAnnotations().get("iam"), is("groot"));
        assertThat(operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getAnnotations().get("han"), is("solo"));

        // edit k8s topic
        topicName = "test-kafka-topic-with-owner-ref-3";
        Topic topic3 = new Topic.Builder(topicName, 1, (short) 1, emptyMap(), metadata).build();
        topic3.getMetadata().getLabels().put("stan", "lee");
        topicResource = TopicSerialization.toTopicResource(topic3, labels);
        createKafkaTopicResource(topicResource);
        assertThat(operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getOwnerReferences().get(0).getUid(), is(uid));
        assertThat(operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getLabels().size(), is(3));
        assertThat(operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getLabels().get("stan"), is("lee"));
        assertThat(operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getLabels().get("iam"), is("root"));
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
    public void testReconciliationOnStartup() throws ExecutionException, InterruptedException, TimeoutException {
        // 1. Create topic A in Kube and reconcile
        String topicNameZ = "topic-z";
        {
            Topic topicZ = new Topic.Builder(topicNameZ, 1, (short) 1, emptyMap()).build();
            KafkaTopic topicResourceZ = TopicSerialization.toTopicResource(topicZ, labels);
            String resourceNameZ = topicResourceZ.getMetadata().getName();
            operation().inNamespace(NAMESPACE).create(topicResourceZ);
            waitForTopicInKafka(topicNameZ);
            assertStatusReady(topicNameZ);
        }

        String topicNameA = "topic-a";
        {
            Topic topicA = new Topic.Builder(topicNameA, 1, (short) 1, emptyMap()).build();
            KafkaTopic topicResourceA = TopicSerialization.toTopicResource(topicA, labels);
            String resourceNameA = topicResourceA.getMetadata().getName();
            operation().inNamespace(NAMESPACE).create(topicResourceA);
            waitForTopicInKafka(topicNameA);
            assertStatusReady(topicNameA);
        }
        String topicNameB = "topic-b";
        String resourceNameB;
        {
            Topic topicB = new Topic.Builder(topicNameB, 1, (short) 1, emptyMap()).build();
            KafkaTopic topicResourceB = TopicSerialization.toTopicResource(topicB, labels);
            resourceNameB = topicResourceB.getMetadata().getName();
            operation().inNamespace(NAMESPACE).create(topicResourceB);
            waitForTopicInKafka(topicNameB);
            assertStatusReady(topicNameB);
        }
        String topicNameC = "topic-c";
        {
            Topic topicC = new Topic.Builder(topicNameC, 1, (short) 1, emptyMap()).build();
            KafkaTopic topicResourceC = TopicSerialization.toTopicResource(topicC, labels);
            String resourceNameC = topicResourceC.getMetadata().getName();
            operation().inNamespace(NAMESPACE).create(topicResourceC);
            waitForTopicInKafka(topicNameC);
            assertStatusReady(topicNameC);
        }

        // 2. Stop TO
        stopTopicOperator();

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
        startTopicOperator(kafkaCluster);

        // 5. Verify topics A, X and Y exist on both sides
        waitForTopicInKafka(topicNameA);
        waitForTopicInKafka(topicNameX);
        waitForTopicInKafka(topicNameY);
        assertStatusReady(topicNameA);
        assertStatusReady(topicNameX);
        assertStatusReady(topicNameY);
        waitForTopicInKube(topicNameA);
        waitForTopicInKube(topicNameX);
        waitForTopicInKube(topicNameY);

        // 5. Verify topics B and C deleted on both sides
        waitForTopicInKube(topicNameB, false);
        waitForTopicInKube(topicNameC, false);
        waitForTopicInKafka(topicNameB, false);
        waitForTopicInKafka(topicNameC, false);

        // 5. Verify topics A and Z were changed.
        awaitTopicConfigInKube(topicNameA, "retention.ms", alteredConfigA);
        awaitTopicConfigInKube(topicNameZ, "retention.ms", alteredConfigZ);
        awaitTopicConfigInKafka(topicNameA, "retention.ms", alteredConfigA);
        awaitTopicConfigInKafka(topicNameZ, "retention.ms", alteredConfigZ);
    }

}
