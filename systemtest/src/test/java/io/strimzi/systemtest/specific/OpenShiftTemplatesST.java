/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.specific;

import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.OpenShiftOnly;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectS2IResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.cmdClient.Oc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CONNECT_S2I;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.TestUtils.map;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Basic tests for the OpenShift templates.
 * This only tests that the template create the appropriate resource,
 * not that the created resource is processed by operator(s) in the appropriate way.
 */
@OpenShiftOnly
@Tag(REGRESSION)
public class OpenShiftTemplatesST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(OpenShiftTemplatesST.class);

    public static final String NAMESPACE = "template-test";
    private Oc oc = (Oc) cmdKubeClient(NAMESPACE);

    public Kafka getKafka(String clusterName) {
        return KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(clusterName).get();
    }

    public KafkaConnect getKafkaConnect(String clusterName) {
        return KafkaConnectResource.kafkaConnectClient().inNamespace(NAMESPACE).withName(clusterName).get();
    }

    public KafkaConnectS2I getKafkaConnectS2I(String clusterName) {
        return KafkaConnectS2IResource.kafkaConnectS2IClient().inNamespace(NAMESPACE).withName(clusterName).get();
    }

    @ParallelTest
    void testStrimziEphemeral(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        oc.createResourceAndApply("strimzi-ephemeral", map("CLUSTER_NAME", clusterName,
                "ZOOKEEPER_NODE_COUNT", "1",
                "KAFKA_NODE_COUNT", "1"));

        Kafka kafka = getKafka(clusterName);
        assertThat(kafka, is(notNullValue()));

        assertThat(kafka.getSpec().getKafka().getReplicas(), is(1));
        assertThat(kafka.getSpec().getZookeeper().getReplicas(), is(1));
        assertThat(kafka.getSpec().getKafka().getStorage().getType(), is("ephemeral"));
        assertThat(kafka.getSpec().getZookeeper().getStorage().getType(), is("ephemeral"));
    }

    @ParallelTest
    void testStrimziPersistent(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        oc.createResourceAndApply("strimzi-persistent", map("CLUSTER_NAME", clusterName,
                "ZOOKEEPER_NODE_COUNT", "1",
                "KAFKA_NODE_COUNT", "1"));

        Kafka kafka = getKafka(clusterName);
        assertThat(kafka, is(notNullValue()));
        assertThat(kafka.getSpec().getKafka().getReplicas(), is(1));
        assertThat(kafka.getSpec().getZookeeper().getReplicas(), is(1));
        assertThat(kafka.getSpec().getKafka().getStorage().getType(), is("jbod"));
        assertThat(kafka.getSpec().getZookeeper().getStorage().getType(), is("persistent-claim"));
    }

    @ParallelTest
    void testStrimziEphemeralWithCustomParameters(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        oc.createResourceAndApply("strimzi-ephemeral", map("CLUSTER_NAME", clusterName,
                "ZOOKEEPER_HEALTHCHECK_DELAY", "30",
                "ZOOKEEPER_HEALTHCHECK_TIMEOUT", "10",
                "KAFKA_HEALTHCHECK_DELAY", "30",
                "KAFKA_HEALTHCHECK_TIMEOUT", "10",
                "KAFKA_DEFAULT_REPLICATION_FACTOR", "2",
                "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "5",
                "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "5"));

        Kafka kafka = getKafka(clusterName);
        assertThat(kafka, is(notNullValue()));

        assertThat(kafka.getSpec().getZookeeper().getLivenessProbe().getInitialDelaySeconds(), is(30));
        assertThat(kafka.getSpec().getZookeeper().getReadinessProbe().getInitialDelaySeconds(), is(30));
        assertThat(kafka.getSpec().getZookeeper().getLivenessProbe().getTimeoutSeconds(), is(10));
        assertThat(kafka.getSpec().getZookeeper().getReadinessProbe().getTimeoutSeconds(), is(10));
        assertThat(kafka.getSpec().getKafka().getLivenessProbe().getInitialDelaySeconds(), is(30));
        assertThat(kafka.getSpec().getKafka().getReadinessProbe().getInitialDelaySeconds(), is(30));
        assertThat(kafka.getSpec().getKafka().getLivenessProbe().getTimeoutSeconds(), is(10));
        assertThat(kafka.getSpec().getKafka().getReadinessProbe().getTimeoutSeconds(), is(10));
        assertThat(kafka.getSpec().getKafka().getConfig().get("default.replication.factor"), is("2"));
        assertThat(kafka.getSpec().getKafka().getConfig().get("offsets.topic.replication.factor"), is("5"));
        assertThat(kafka.getSpec().getKafka().getConfig().get("transaction.state.log.replication.factor"), is("5"));
    }

    @ParallelTest
    void testStrimziPersistentWithCustomParameters(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        oc.createResourceAndApply("strimzi-persistent", map("CLUSTER_NAME", clusterName,
                "ZOOKEEPER_HEALTHCHECK_DELAY", "30",
                "ZOOKEEPER_HEALTHCHECK_TIMEOUT", "10",
                "KAFKA_HEALTHCHECK_DELAY", "30",
                "KAFKA_HEALTHCHECK_TIMEOUT", "10",
                "KAFKA_DEFAULT_REPLICATION_FACTOR", "2",
                "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "5",
                "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "5",
                "ZOOKEEPER_VOLUME_CAPACITY", "2Gi",
                "KAFKA_VOLUME_CAPACITY", "2Gi"));

        //TODO Add assertions to check that Kafka brokers have a custom configuration
        Kafka kafka = getKafka(clusterName);
        assertThat(kafka, is(notNullValue()));

        assertThat(kafka.getSpec().getZookeeper().getLivenessProbe().getInitialDelaySeconds(), is(30));
        assertThat(kafka.getSpec().getZookeeper().getReadinessProbe().getInitialDelaySeconds(), is(30));
        assertThat(kafka.getSpec().getZookeeper().getLivenessProbe().getTimeoutSeconds(), is(10));
        assertThat(kafka.getSpec().getZookeeper().getReadinessProbe().getTimeoutSeconds(), is(10));
        assertThat(kafka.getSpec().getKafka().getLivenessProbe().getInitialDelaySeconds(), is(30));
        assertThat(kafka.getSpec().getKafka().getReadinessProbe().getInitialDelaySeconds(), is(30));
        assertThat(kafka.getSpec().getKafka().getLivenessProbe().getTimeoutSeconds(), is(10));
        assertThat(kafka.getSpec().getKafka().getReadinessProbe().getTimeoutSeconds(), is(10));
        assertThat(kafka.getSpec().getKafka().getConfig().get("default.replication.factor"), is("2"));
        assertThat(kafka.getSpec().getKafka().getConfig().get("offsets.topic.replication.factor"), is("5"));
        assertThat(kafka.getSpec().getKafka().getConfig().get("transaction.state.log.replication.factor"), is("5"));
        assertThat(((PersistentClaimStorage) ((JbodStorage) kafka.getSpec().getKafka().getStorage()).getVolumes().get(0)).getSize(), is("2Gi"));
        assertThat(((PersistentClaimStorage) kafka.getSpec().getZookeeper().getStorage()).getSize(), is("2Gi"));
    }

    @ParallelTest
    @Tag(CONNECT)
    void testConnect(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        oc.createResourceAndApply("strimzi-connect", map("CLUSTER_NAME", clusterName,
                "INSTANCES", "1"));

        KafkaConnect connect = getKafkaConnect(clusterName);
        assertThat(connect, is(notNullValue()));
        assertThat(connect.getSpec().getReplicas(), is(1));
    }

    @ParallelTest
    @Tag(CONNECT_S2I)
    void testConnectS2I(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        oc.createResourceAndApply("strimzi-connect-s2i", map("CLUSTER_NAME", clusterName,
                "INSTANCES", "1"));

        KafkaConnectS2I cm = getKafkaConnectS2I(clusterName);
        assertThat(cm, is(notNullValue()));
        assertThat(cm.getSpec().getReplicas(), is(1));
    }

    @ParallelTest
    void testTopicOperator(ExtensionContext extensionContext) {
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        oc.createResourceAndApply("strimzi-topic", map(
                "TOPIC_NAME", topicName,
                "TOPIC_PARTITIONS", "10",
                "TOPIC_REPLICAS", "2"));

        KafkaTopic topic = KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).withName(topicName).get();
        assertThat(topic, is(notNullValue()));
        assertThat(topic.getSpec(), is(notNullValue()));
        assertThat(topic.getSpec().getTopicName(), is(nullValue()));
        assertThat(topic.getSpec().getPartitions(), is(10));
        assertThat(topic.getSpec().getReplicas(), is(2));
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        LOGGER.info("Creating resources before the test class");
        cluster.createNamespace(NAMESPACE);
        cluster.createCustomResources(Constants.PATH_TO_PACKAGING_EXAMPLES + "/templates/cluster-operator",
            Constants.PATH_TO_PACKAGING_EXAMPLES + "/templates/topic-operator",
            TestUtils.CRD_KAFKA,
            TestUtils.CRD_KAFKA_CONNECT,
            TestUtils.CRD_KAFKA_CONNECT_S2I,
            TestUtils.CRD_TOPIC,
            TestUtils.USER_PATH + "/src/rbac/role-edit-kafka.yaml");
    }

    @AfterAll
    protected void tearDownEnvironmentAfterAll() {
        cluster.deleteCustomResources();
        cluster.deleteNamespaces();
    }
}
