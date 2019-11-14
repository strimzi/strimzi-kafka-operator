/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaTopicBuilder;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import kafka.server.KafkaConfig$;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Timeout(value = 10, timeUnit = TimeUnit.MINUTES)
@ExtendWith(VertxExtension.class)
public class TopicOperatorIT extends TopicOperatorBaseIT {

    private static final Logger LOGGER = LogManager.getLogger(TopicOperatorIT.class);

    @Override
    protected int numKafkaBrokers() {
        return 1;
    }

    @Override
    protected Properties kafkaClusterConfig() {
        Properties p = new Properties();
        p.setProperty(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), "false");
        return p;
    }


    @Test
    public void testTopicAdded(VertxTestContext context) throws Exception {
        createTopic(context, "test-topic-added");
        context.completeNow();
    }

    @Test
    public void testTopicAddedWithEncodableName(VertxTestContext context) throws Exception {
        createTopic(context, "test-TOPIC_ADDED_ENCODABLE");
        context.completeNow();
    }

    @Test
    public void testTopicDeleted(VertxTestContext context) throws Exception {
        createAndDeleteTopic(context, "test-topic-deleted");
        context.completeNow();
    }

    @Test
    public void testTopicDeletedWithEncodableName(VertxTestContext context) throws Exception {
        createAndDeleteTopic(context, "test-TOPIC_DELETED_ENCODABLE");
        context.completeNow();
    }

    @Test
    public void testTopicConfigChanged(VertxTestContext context) throws Exception {
        createAndAlterTopicConfig(context, "test-topic-config-changed");
        context.completeNow();
    }

    @Test
    public void testTopicConfigChangedWithEncodableName(VertxTestContext context) throws Exception {
        createAndAlterTopicConfig(context, "test-TOPIC_CONFIG_CHANGED_ENCODABLE");
        context.completeNow();
    }

    @Test
    public void testTopicNumPartitionsChanged(VertxTestContext context) throws Exception {
        createAndAlterNumPartitions(context, "test-topic-partitions-changed");
        context.completeNow();
    }

    @Test
    public void testKafkaTopicAdded(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topicName = "test-kafkatopic-created";
        createKafkaTopicResource(context, topicName);
        context.completeNow();
    }

    void createKafkaTopicResourceError(VertxTestContext context, String topicName, Map<String, String> objectObjectMap, int rf,
                                       String expectedMessage) throws InterruptedException, ExecutionException, TimeoutException {
        Topic topic = new Topic.Builder(topicName, 1, (short) 1, objectObjectMap).build();
        KafkaTopic kafkaTopic = TopicSerialization.toTopicResource(topic, labels);
        kafkaTopic.getSpec().setReplicas(rf);

        // Create a Topic Resource
        operation().inNamespace(NAMESPACE).create(kafkaTopic);
        assertStatusNotReady(context, topicName, expectedMessage);
    }

    public void testKafkaTopicAddedWithMoreReplicasThanBrokers(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        createKafkaTopicResourceError(context, "test-resource-created-with-more-replicas-than-brokers", emptyMap(), 42, "Replication factor: 42 larger than available brokers: 1.");
    }

    @Test
    public void testKafkaTopicAddedWithHigherMinIsrThanBrokers(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        createKafkaTopicResourceError(context, "test-resource-created-with-higher-min-isr-than-brokers",
                singletonMap("min.insync.replicas", "42"), 42,
               "Replication factor: 42 larger than available brokers: 1.");
        context.completeNow();
    }

    @Test
    public void testKafkaTopicAddedWithUnknownConfig(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        createKafkaTopicResourceError(context, "test-resource-created-with-unknown-config",
                singletonMap("aardvark", "zebra"), 1,
               "Unknown topic config name: aardvark");
        context.completeNow();
    }

    @Test
    public void testKafkaTopicAddedWithInvalidConfig(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        createKafkaTopicResourceError(context, "test-resource-created-with-invalid-config",
                singletonMap("message.format.version", "zebra"), 1,
               "Invalid value zebra for configuration message.format.version: Version `zebra` is not a valid version");
        context.completeNow();
    }

    @Test
    public void testKafkaTopicAddedWithBadData(VertxTestContext context) {
        String topicName = "test-resource-created-with-bad-data";
        Topic topic = new Topic.Builder(topicName, 1, (short) 1, emptyMap()).build();
        KafkaTopic kafkaTopic = TopicSerialization.toTopicResource(topic, labels);
        kafkaTopic.getSpec().setPartitions(-1);

        // Create a Topic Resource
        try {
            operation().inNamespace(NAMESPACE).create(kafkaTopic);
        } catch (KubernetesClientException e) {
            assertThat(e.getMessage().contains("spec.partitions in body should be greater than or equal to 1"), is(true));
            context.completeNow();
        }
    }

    @Test
    public void testKafkaTopicDeleted(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        // create the Topic Resource
        String topicName = "test-kafkatopic-deleted";
        KafkaTopic topicResource = createKafkaTopicResource(context, topicName);
        deleteInKubeAndAwaitReconciliation(context, topicName, topicResource);
        context.completeNow();
    }

    @Test
    public void testKafkaTopicModifiedRetentionChanged(VertxTestContext context) throws Exception {
        // create the topic
        String topicName = "test-kafkatopic-modified-retention-changed";
        KafkaTopic topicResource = createKafkaTopicResource(context, topicName);
        String expectedValue = alterTopicConfigInKube(topicResource.getMetadata().getName(),
                "retention.ms", currentValue -> Integer.toString(Integer.parseInt(currentValue) + 1));
        awaitTopicConfigInKafka(context, topicName, "retention.ms", expectedValue);
        context.completeNow();
    }

    @Test
    public void testKafkaTopicModifiedWithBadData(VertxTestContext context) throws Exception {
        // create the topicResource
        String topicName = "test-kafkatopic-modified-with-bad-data";
        KafkaTopic topicResource = createKafkaTopicResource(context, topicName);

        // now change the topicResource
        KafkaTopic changedTopic = new KafkaTopicBuilder(operation().inNamespace(NAMESPACE).withName(topicResource.getMetadata().getName()).get())
                .editOrNewSpec().withPartitions(-1).endSpec().build();
        try {
            operation().inNamespace(NAMESPACE).withName(topicResource.getMetadata().getName()).replace(changedTopic);
        } catch (KubernetesClientException e) {
            assertThat(e.getMessage().contains("spec.partitions in body should be greater than or equal to 1"), is(true));
        }
        context.completeNow();
    }

    @Test
    public void testKafkaTopicModifiedNameChanged(VertxTestContext context) throws Exception {
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
        context.completeNow();

    }

    @Test
    public void testCreateTwoResourcesManagingOneTopic(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
        String topicName = "two-resources-one-topic";
        Topic topic = new Topic.Builder(topicName, 1, (short) 1, emptyMap()).build();
        KafkaTopic topicResource = TopicSerialization.toTopicResource(topic, labels);
        KafkaTopic topicResource2 = new KafkaTopicBuilder(topicResource).withMetadata(new ObjectMetaBuilder(topicResource.getMetadata()).withName(topicName + "-1").build()).build();
        // create one
        createKafkaTopicResource(context, topicResource2);
        // create another
        operation().inNamespace(NAMESPACE).create(topicResource);

        waitForEvent(context, topicResource,
                "Failure processing KafkaTopic watch event ADDED on resource two-resources-one-topic with labels \\{.*\\}: " +
                        "Topic 'two-resources-one-topic' is already managed via KafkaTopic 'two-resources-one-topic-1' it cannot also be managed via the KafkaTopic 'two-resources-one-topic'",
                TopicOperator.EventType.WARNING);
        context.completeNow();
    }


    @Test
    public void testReconcile(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
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
        context.completeNow();
    }

    // TODO: What happens if we create and then change labels to the resource predicate isn't matched any more
    //       What then happens if we change labels back?

    @Test
    public void testKafkaTopicWithOwnerRef(VertxTestContext context) throws InterruptedException, ExecutionException, TimeoutException {
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
        assertThat(operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getOwnerReferences().size(), is(1));
        assertThat(operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getOwnerReferences().get(0).getUid(), is(uid));
        assertThat(operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getAnnotations().size(), is(1));
        assertThat(operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getAnnotations().get("iam"), is("groot"));
        assertThat(operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getLabels().size(), is(5));
        assertThat(operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getLabels().get("iam"), is("root"));

        // edit kafka topic
        topicName = "test-kafka-topic-with-owner-ref-2";
        Topic topic2 = new Topic.Builder(topicName, 1, (short) 1, emptyMap(), metadata).build();
        KafkaTopic topicResource2 = TopicSerialization.toTopicResource(topic2, labels);
        topicResource = TopicSerialization.toTopicResource(topic2, labels);
        topicResource.getMetadata().getAnnotations().put("han", "solo");
        createKafkaTopicResource(context, topicResource2);
        assertThat(operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getOwnerReferences().get(0).getUid(), is(uid));
        assertThat(operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getAnnotations().size(), is(2));
        assertThat(operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getAnnotations().get("iam"), is("groot"));
        assertThat(operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getAnnotations().get("han"), is("solo"));

        // edit k8s topic
        topicName = "test-kafka-topic-with-owner-ref-3";
        Topic topic3 = new Topic.Builder(topicName, 1, (short) 1, emptyMap(), metadata).build();
        topic3.getMetadata().getLabels().put("stan", "lee");
        topicResource = TopicSerialization.toTopicResource(topic3, labels);
        createKafkaTopicResource(context, topicResource);
        assertThat(operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getOwnerReferences().get(0).getUid(), is(uid));
        assertThat(operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getLabels().size(), is(6));
        assertThat(operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getLabels().get("stan"), is("lee"));
        assertThat(operation().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getLabels().get("iam"), is("root"));
        context.completeNow();
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
    public void testReconciliationOnStartup(VertxTestContext context) throws ExecutionException, InterruptedException, TimeoutException {
        // 1. Create topic A in Kube and reconcile
        String topicNameZ = "topic-z";
        {
            Topic topicZ = new Topic.Builder(topicNameZ, 1, (short) 1, emptyMap()).build();
            KafkaTopic topicResourceZ = TopicSerialization.toTopicResource(topicZ, labels);
            String resourceNameZ = topicResourceZ.getMetadata().getName();
            operation().inNamespace(NAMESPACE).create(topicResourceZ);
            waitForTopicInKafka(context, topicNameZ);
            assertStatusReady(context, topicNameZ);
        }

        String topicNameA = "topic-a";
        {
            Topic topicA = new Topic.Builder(topicNameA, 1, (short) 1, emptyMap()).build();
            KafkaTopic topicResourceA = TopicSerialization.toTopicResource(topicA, labels);
            String resourceNameA = topicResourceA.getMetadata().getName();
            operation().inNamespace(NAMESPACE).create(topicResourceA);
            waitForTopicInKafka(context, topicNameA);
            assertStatusReady(context, topicNameA);
        }
        String topicNameB = "topic-b";
        String resourceNameB;
        {
            Topic topicB = new Topic.Builder(topicNameB, 1, (short) 1, emptyMap()).build();
            KafkaTopic topicResourceB = TopicSerialization.toTopicResource(topicB, labels);
            resourceNameB = topicResourceB.getMetadata().getName();
            operation().inNamespace(NAMESPACE).create(topicResourceB);
            waitForTopicInKafka(context, topicNameB);
            assertStatusReady(context, topicNameB);
        }
        String topicNameC = "topic-c";
        {
            Topic topicC = new Topic.Builder(topicNameC, 1, (short) 1, emptyMap()).build();
            KafkaTopic topicResourceC = TopicSerialization.toTopicResource(topicC, labels);
            String resourceNameC = topicResourceC.getMetadata().getName();
            operation().inNamespace(NAMESPACE).create(topicResourceC);
            waitForTopicInKafka(context, topicNameC);
            assertStatusReady(context, topicNameC);
        }

        // 2. Stop TO
        stopTopicOperator(context);

        // 3. Modify topic A in kubernetes and topic Z in Kafka
        String alteredConfigA = alterTopicConfigInKafka(topicNameA,
                "retention.ms", currentValue -> Integer.toString(Integer.parseInt(currentValue) + 1));
        String alteredConfigZ = alterTopicConfigInKube(topicNameZ,
                "retention.ms", currentValue -> Integer.toString(Integer.parseInt(currentValue) + 1));

        // 3. Delete topic B in Kafka, delete topic C in Kubernetes
        deleteTopicInKafka(topicNameB, resourceNameB);
        deleteInKube(context, topicNameC);

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
        startTopicOperator(context);

        // 5. Verify topics A, X and Y exist on both sides
        waitForTopicInKafka(context, topicNameA);
        waitForTopicInKafka(context, topicNameX);
        waitForTopicInKafka(context, topicNameY);
        assertStatusReady(context, topicNameA);
        assertStatusReady(context, topicNameX);
        assertStatusReady(context, topicNameY);
        waitForTopicInKube(context, topicNameA);
        waitForTopicInKube(context, topicNameX);
        waitForTopicInKube(context, topicNameY);

        // 5. Verify topics B and C deleted on both sides
        waitForTopicInKube(context, topicNameB, false);
        waitForTopicInKube(context, topicNameC, false);
        waitForTopicInKafka(context, topicNameB, false);
        waitForTopicInKafka(context, topicNameC, false);

        // 5. Verify topics A and Z were changed.
        awaitTopicConfigInKube(context, topicNameA, "retention.ms", alteredConfigA);
        awaitTopicConfigInKube(context, topicNameZ, "retention.ms", alteredConfigZ);
        awaitTopicConfigInKafka(context, topicNameA, "retention.ms", alteredConfigA);
        awaitTopicConfigInKafka(context, topicNameZ, "retention.ms", alteredConfigZ);
        context.completeNow();
    }
}
