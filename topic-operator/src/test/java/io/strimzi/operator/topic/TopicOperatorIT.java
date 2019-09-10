/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.debezium.kafka.KafkaCluster;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaTopicBuilder;
import io.strimzi.test.BaseITST;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import kafka.server.KafkaConfig$;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(VertxUnitRunner.class)
public class TopicOperatorIT extends TopicOperatorBaseIT {

    private static final Logger LOGGER = LogManager.getLogger(TopicOperatorIT.class);

    @Before
    public void setup(TestContext context) throws Exception {
        LOGGER.info("Setting up test");
        kubeCluster().before();
        Runtime.getRuntime().addShutdownHook(kafkaHook);
        int counts = 3;
        do {
            try {
                kafkaCluster = new KafkaCluster();
                kafkaCluster.addBrokers(1);
                kafkaCluster.deleteDataPriorToStartup(true);
                kafkaCluster.deleteDataUponShutdown(true);
                kafkaCluster.usingDirectory(Files.createTempDirectory("operator-integration-test").toFile());
                Properties p = new Properties();
                p.setProperty(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), "false");
                kafkaCluster.withKafkaConfiguration(p);
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

    @After
    public void teardown(TestContext context) throws InterruptedException {
        LOGGER.info("Tearing down test");

        if (kubeClient != null) {
            List<KafkaTopic> items = operation().inNamespace(NAMESPACE).list().getItems();

            // Wait for the operator to delete all the existing topics in Kafka
            for (KafkaTopic item : items) {
                LOGGER.info("Deleting {} from Kube", item.getMetadata().getName());
                operation().inNamespace(NAMESPACE).withName(item.getMetadata().getName()).delete();
                LOGGER.info("Awaiting deletion of {} in Kafka", item.getMetadata().getName());
                waitForTopicInKafka(context, new TopicName(item).toString(), false);
                waitForTopicInKube(context, item.getMetadata().getName(), false);
            }
        }

        Thread.sleep(5_000);

        stopTopicOperator(context);

        adminClient.close();
        if (kafkaCluster != null) {
            kafkaCluster.shutdown();
        }
        Runtime.getRuntime().removeShutdownHook(kafkaHook);
        LOGGER.info("Finished tearing down test");
    }

    @Test
    public void testTopicAdded(TestContext context) throws Exception {
        createTopic(context, "test-topic-added");
    }

    @Test
    public void testTopicAddedWithEncodableName(TestContext context) throws Exception {
        createTopic(context, "test-TOPIC_ADDED_ENCODABLE");
    }

    @Test
    public void testTopicDeleted(TestContext context) throws Exception {
        createAndDeleteTopic(context, "test-topic-deleted");
    }

    @Test
    public void testTopicDeletedWithEncodableName(TestContext context) throws Exception {
        createAndDeleteTopic(context, "test-TOPIC_DELETED_ENCODABLE");
    }

    @Test
    public void testTopicConfigChanged(TestContext context) throws Exception {
        createAndAlterTopicConfig(context, "test-topic-config-changed");
    }

    @Test
    public void testTopicConfigChangedWithEncodableName(TestContext context) throws Exception {
        createAndAlterTopicConfig(context, "test-TOPIC_CONFIG_CHANGED_ENCODABLE");
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

    void createKafkaTopicResourceError(TestContext context, String topicName, Map<String, String> objectObjectMap, int rf,
                                       String expectedMessage) {
        Topic topic = new Topic.Builder(topicName, 1, (short) 1, objectObjectMap).build();
        KafkaTopic kafkaTopic = TopicSerialization.toTopicResource(topic, labels);
        kafkaTopic.getSpec().setReplicas(rf);

        // Create a Topic Resource
        operation().inNamespace(NAMESPACE).create(kafkaTopic);
        assertStatusNotReady(context, topicName, expectedMessage);
    }

    public void testKafkaTopicAddedWithMoreReplicasThanBrokers(TestContext context) {
        createKafkaTopicResourceError(context, "test-resource-created-with-more-replicas-than-brokers", emptyMap(), 42, "Replication factor: 42 larger than available brokers: 1.");
    }

    @Test
    public void testKafkaTopicAddedWithHigherMinIsrThanBrokers(TestContext context) {
        createKafkaTopicResourceError(context, "test-resource-created-with-higher-min-isr-than-brokers",
                singletonMap("min.insync.replicas", "42"), 42,
               "Replication factor: 42 larger than available brokers: 1.");
    }

    @Test
    public void testKafkaTopicAddedWithUnknownConfig(TestContext context) {
        createKafkaTopicResourceError(context, "test-resource-created-with-unknown-config",
                singletonMap("aardvark", "zebra"), 1,
               "Unknown topic config name: aardvark");
    }

    @Test
    public void testKafkaTopicAddedWithInvalidConfig(TestContext context) {
        createKafkaTopicResourceError(context, "test-resource-created-with-invalid-config",
                singletonMap("message.format.version", "zebra"), 1,
               "Invalid value zebra for configuration message.format.version: Version `zebra` is not a valid version");
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

    @Test
    public void testKafkaTopicModifiedRetentionChanged(TestContext context) throws Exception {
        // create the topic
        String topicName = "test-kafkatopic-modified-retention-changed";
        KafkaTopic topicResource = createKafkaTopicResource(context, topicName);
        String expectedValue = alterTopicConfigInKube(topicResource.getMetadata().getName(),
                "retention.ms", currentValue -> Integer.toString(Integer.parseInt(currentValue) + 1));
        awaitTopicConfigInKafka(context, topicName, "retention.ms", expectedValue);
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
        assertStatusReady(context, topicName);
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
            assertStatusReady(testContext, topicNameZ);
        }

        String topicNameA = "topic-a";
        {
            Topic topicA = new Topic.Builder(topicNameA, 1, (short) 1, emptyMap()).build();
            KafkaTopic topicResourceA = TopicSerialization.toTopicResource(topicA, labels);
            String resourceNameA = topicResourceA.getMetadata().getName();
            operation().inNamespace(NAMESPACE).create(topicResourceA);
            waitForTopicInKafka(testContext, topicNameA);
            assertStatusReady(testContext, topicNameA);
        }
        String topicNameB = "topic-b";
        String resourceNameB;
        {
            Topic topicB = new Topic.Builder(topicNameB, 1, (short) 1, emptyMap()).build();
            KafkaTopic topicResourceB = TopicSerialization.toTopicResource(topicB, labels);
            resourceNameB = topicResourceB.getMetadata().getName();
            operation().inNamespace(NAMESPACE).create(topicResourceB);
            waitForTopicInKafka(testContext, topicNameB);
            assertStatusReady(testContext, topicNameB);
        }
        String topicNameC = "topic-c";
        {
            Topic topicC = new Topic.Builder(topicNameC, 1, (short) 1, emptyMap()).build();
            KafkaTopic topicResourceC = TopicSerialization.toTopicResource(topicC, labels);
            String resourceNameC = topicResourceC.getMetadata().getName();
            operation().inNamespace(NAMESPACE).create(topicResourceC);
            waitForTopicInKafka(testContext, topicNameC);
            assertStatusReady(testContext, topicNameC);
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
        deleteInKube(testContext, topicNameC);

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
        assertStatusReady(testContext, topicNameA);
        assertStatusReady(testContext, topicNameX);
        assertStatusReady(testContext, topicNameY);
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
