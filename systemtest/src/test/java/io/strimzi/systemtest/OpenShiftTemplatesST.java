/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaAssemblyList;
import io.strimzi.api.kafka.KafkaConnectAssemblyList;
import io.strimzi.api.kafka.KafkaConnectS2IAssemblyList;
import io.strimzi.api.kafka.KafkaTopicList;
import io.strimzi.api.kafka.model.DoneableKafka;
import io.strimzi.api.kafka.model.DoneableKafkaConnect;
import io.strimzi.api.kafka.model.DoneableKafkaConnectS2I;
import io.strimzi.api.kafka.model.DoneableKafkaTopic;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.PersistentClaimStorage;
import io.strimzi.test.annotations.OpenShiftOnly;
import io.strimzi.test.extensions.StrimziExtension;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.k8s.Oc;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;

import static io.strimzi.test.TestUtils.map;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static io.strimzi.test.extensions.StrimziExtension.REGRESSION;

/**
 * Basic tests for the OpenShift templates.
 * This only tests that the template create the appropriate resource,
 * not that the created resource is processed by operator(s) in the appropriate way.
 */
@ExtendWith(StrimziExtension.class)
@OpenShiftOnly
public class OpenShiftTemplatesST extends AbstractST {

    public static final String NAMESPACE = "template-test";

    public static KubeClusterResource cluster = new KubeClusterResource();

    private ObjectMapper mapper = new ObjectMapper();
    private Oc oc = (Oc) KUBE_CLIENT;
    private KubernetesClient client = new DefaultKubernetesClient();

    private Kafka getKafkaWithName(String clusterName) {
        return client.customResources(Crds.kafka(), Kafka.class, KafkaAssemblyList.class, DoneableKafka.class).inNamespace(NAMESPACE).withName(clusterName).get();
    }

    private KafkaConnect getKafkaConnectWithName(String clusterName) {
        return client.customResources(Crds.kafkaConnect(), KafkaConnect.class, KafkaConnectAssemblyList.class, DoneableKafkaConnect.class).inNamespace(NAMESPACE).withName(clusterName).get();
    }

    private KafkaConnectS2I getKafkaConnectS2IWithName(String clusterName) {
        return client.customResources(Crds.kafkaConnectS2I(), KafkaConnectS2I.class, KafkaConnectS2IAssemblyList.class, DoneableKafkaConnectS2I.class).inNamespace(NAMESPACE).withName(clusterName).get();
    }

    @Test
    @Tag(REGRESSION)
    void testStrimziEphemeral() {
        String clusterName = "foo";
        oc.newApp("strimzi-ephemeral", map("CLUSTER_NAME", clusterName,
                "ZOOKEEPER_NODE_COUNT", "1",
                "KAFKA_NODE_COUNT", "1"));

        Kafka kafka = getKafkaWithName(clusterName);
        assertNotNull(kafka);

        assertEquals(1, kafka.getSpec().getKafka().getReplicas());
        assertEquals(1, kafka.getSpec().getZookeeper().getReplicas());
        assertEquals("ephemeral", kafka.getSpec().getKafka().getStorage().getType());
        assertEquals("ephemeral", kafka.getSpec().getZookeeper().getStorage().getType());
    }

    @Test
    @Tag(REGRESSION)
    void testStrimziPersistent() {
        String clusterName = "bar";
        oc.newApp("strimzi-persistent", map("CLUSTER_NAME", clusterName,
                "ZOOKEEPER_NODE_COUNT", "1",
                "KAFKA_NODE_COUNT", "1"));

        Kafka kafka = getKafkaWithName(clusterName);
        assertNotNull(kafka);
        assertEquals(1, kafka.getSpec().getKafka().getReplicas());
        assertEquals(1, kafka.getSpec().getZookeeper().getReplicas());
        assertEquals("persistent-claim", kafka.getSpec().getKafka().getStorage().getType());
        assertEquals("persistent-claim", kafka.getSpec().getZookeeper().getStorage().getType());
    }

    @Test
    @Tag(REGRESSION)
    void testStrimziEphemeralWithCustomParameters() {
        String clusterName = "test-ephemeral-with-custom-parameters";
        oc.newApp("strimzi-ephemeral", map("CLUSTER_NAME", clusterName,
                "ZOOKEEPER_HEALTHCHECK_DELAY", "30",
                "ZOOKEEPER_HEALTHCHECK_TIMEOUT", "10",
                "KAFKA_HEALTHCHECK_DELAY", "30",
                "KAFKA_HEALTHCHECK_TIMEOUT", "10",
                "KAFKA_DEFAULT_REPLICATION_FACTOR", "2",
                "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "5",
                "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "5"));

        //TODO Add assertions to check that Kafka brokers have a custom configuration
        Kafka kafka = getKafkaWithName(clusterName);
        assertNotNull(kafka);

        assertEquals(30, kafka.getSpec().getZookeeper().getLivenessProbe().getInitialDelaySeconds());
        assertEquals(30, kafka.getSpec().getZookeeper().getReadinessProbe().getInitialDelaySeconds());
        assertEquals(10, kafka.getSpec().getZookeeper().getLivenessProbe().getTimeoutSeconds());
        assertEquals(10, kafka.getSpec().getZookeeper().getReadinessProbe().getTimeoutSeconds());
        assertEquals(30, kafka.getSpec().getKafka().getLivenessProbe().getInitialDelaySeconds());
        assertEquals(30, kafka.getSpec().getKafka().getReadinessProbe().getInitialDelaySeconds());
        assertEquals(10, kafka.getSpec().getKafka().getLivenessProbe().getTimeoutSeconds());
        assertEquals(10, kafka.getSpec().getKafka().getReadinessProbe().getTimeoutSeconds());
        assertEquals("2", kafka.getSpec().getKafka().getConfig().get("default.replication.factor"));
        assertEquals("5", kafka.getSpec().getKafka().getConfig().get("offsets.topic.replication.factor"));
        assertEquals("5", kafka.getSpec().getKafka().getConfig().get("transaction.state.log.replication.factor"));
    }

    @Test
    @Tag(REGRESSION)
    void testStrimziPersistentWithCustomParameters() {
        String clusterName = "test-persistent-with-custom-parameters";
        oc.newApp("strimzi-persistent", map("CLUSTER_NAME", clusterName,
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
        Kafka kafka = getKafkaWithName(clusterName);
        assertNotNull(kafka);

        assertEquals(30, kafka.getSpec().getZookeeper().getLivenessProbe().getInitialDelaySeconds());
        assertEquals(30, kafka.getSpec().getZookeeper().getReadinessProbe().getInitialDelaySeconds());
        assertEquals(10, kafka.getSpec().getZookeeper().getLivenessProbe().getTimeoutSeconds());
        assertEquals(10, kafka.getSpec().getZookeeper().getReadinessProbe().getTimeoutSeconds());
        assertEquals(30, kafka.getSpec().getKafka().getLivenessProbe().getInitialDelaySeconds());
        assertEquals(30, kafka.getSpec().getKafka().getReadinessProbe().getInitialDelaySeconds());
        assertEquals(10, kafka.getSpec().getKafka().getLivenessProbe().getTimeoutSeconds());
        assertEquals(10, kafka.getSpec().getKafka().getReadinessProbe().getTimeoutSeconds());
        assertEquals("2", kafka.getSpec().getKafka().getConfig().get("default.replication.factor"));
        assertEquals("5", kafka.getSpec().getKafka().getConfig().get("offsets.topic.replication.factor"));
        assertEquals("5", kafka.getSpec().getKafka().getConfig().get("transaction.state.log.replication.factor"));
        assertEquals("2Gi", ((PersistentClaimStorage) kafka.getSpec().getKafka().getStorage()).getSize());
        assertEquals("2Gi", ((PersistentClaimStorage) kafka.getSpec().getZookeeper().getStorage()).getSize());
    }

    @Test
    @Tag(REGRESSION)
    void testConnect() {
        String clusterName = "test-connect";
        oc.newApp("strimzi-connect", map("CLUSTER_NAME", clusterName,
                "INSTANCES", "1"));

        KafkaConnect connect = getKafkaConnectWithName(clusterName);
        assertNotNull(connect);
        assertEquals(1, connect.getSpec().getReplicas());
    }

    @Test
    @Tag(REGRESSION)
    void testS2i() {
        String clusterName = "test-s2i";
        oc.newApp("strimzi-connect-s2i", map("CLUSTER_NAME", clusterName,
                "INSTANCES", "1"));

        KafkaConnectS2I cm = getKafkaConnectS2IWithName(clusterName);
        assertNotNull(cm);
        assertEquals(1, cm.getSpec().getReplicas());
    }

    @Test
    @Tag(REGRESSION)
    void testTopicOperator() {
        String topicName = "test-topic-topic";
        oc.newApp("strimzi-topic", map(
                "TOPIC_NAME", topicName,
                "TOPIC_PARTITIONS", "10",
                "TOPIC_REPLICAS", "2"));

        KafkaTopic topic = client.customResources(Crds.topic(), KafkaTopic.class, KafkaTopicList.class, DoneableKafkaTopic.class).inNamespace(NAMESPACE).withName(topicName).get();
        assertNotNull(topic);
        assertNotNull(topic.getSpec());
        assertNull(topic.getSpec().getTopicName());
        assertEquals(Integer.valueOf(10), topic.getSpec().getPartitions());
        assertEquals(Integer.valueOf(2), topic.getSpec().getReplicas());
    }

    @BeforeAll
    void setupEnvironment() {
        createNamespace(NAMESPACE);
        createCustomResources(Arrays.asList(
                "../examples/templates/cluster-operator",
                "../examples/templates/topic-operator",
                TestUtils.CRD_KAFKA,
                TestUtils.CRD_KAFKA_CONNECT,
                TestUtils.CRD_KAFKA_CONNECT_S2I,
                TestUtils.CRD_TOPIC,
                "src/rbac/role-edit-kafka.yaml"));
    }

    @AfterAll
    void teardownEnvironment() {
        deleteCustomResources();
        deleteNamespaces();
    }
}
