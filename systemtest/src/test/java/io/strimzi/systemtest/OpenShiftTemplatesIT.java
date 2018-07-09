/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.DoneableKafkaAssembly;
import io.strimzi.api.kafka.KafkaAssemblyList;
import io.strimzi.api.kafka.model.KafkaAssembly;
import io.strimzi.api.kafka.model.PersistentClaimStorage;
import io.strimzi.test.JUnitGroup;
import io.strimzi.test.Namespace;
import io.strimzi.test.OpenShiftOnly;
import io.strimzi.test.Resources;
import io.strimzi.test.StrimziRunner;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.k8s.Oc;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Map;

import static io.strimzi.test.TestUtils.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Basic tests for the OpenShift templates.
 * This only tests that the template create the appropriate resource,
 * not that the created resource is processed by operator(s) in the appropriate way.
 */
@RunWith(StrimziRunner.class)
@OpenShiftOnly
@Namespace(OpenShiftTemplatesIT.NAMESPACE)
@Resources(value = "../examples/templates/cluster-operator", asAdmin = true)
@Resources(value = "../examples/templates/topic-operator", asAdmin = true)
public class OpenShiftTemplatesIT {

    public static final String NAMESPACE = "template-test";

    @ClassRule
    public static KubeClusterResource cluster = new KubeClusterResource();

    private ObjectMapper mapper = new ObjectMapper();
    private Oc oc = (Oc) cluster.client();
    private KubernetesClient client = new DefaultKubernetesClient();

    private KafkaAssembly getKafkaWithName(String clusterName) {
        CustomResourceDefinition crd = client.customResourceDefinitions().withName(KafkaAssembly.CRD_NAME).get();
        KafkaAssembly kafka = client.customResources(crd, KafkaAssembly.class, KafkaAssemblyList.class, DoneableKafkaAssembly.class).inNamespace(NAMESPACE).withName(clusterName).get();
        return kafka;
    }

    @Test
    @JUnitGroup(name = "regression")
    public void testStrimziEphemeral() throws IOException {
        String clusterName = "foo";
        oc.newApp("strimzi-ephemeral", map("CLUSTER_NAME", clusterName,
                "ZOOKEEPER_NODE_COUNT", "1",
                "KAFKA_NODE_COUNT", "1"));

        KafkaAssembly kafka = getKafkaWithName(clusterName);
        assertNotNull(kafka);

        assertEquals(1, kafka.getSpec().getKafka().getReplicas());
        assertEquals(1, kafka.getSpec().getZookeeper().getReplicas());
        assertEquals("ephemeral", kafka.getSpec().getKafka().getStorage().getType());
        assertEquals("ephemeral", kafka.getSpec().getZookeeper().getStorage().getType());
    }

    @Test
    @JUnitGroup(name = "regression")
    public void testStrimziPersistent() throws IOException {
        String clusterName = "bar";
        oc.newApp("strimzi-persistent", map("CLUSTER_NAME", clusterName,
                "ZOOKEEPER_NODE_COUNT", "1",
                "KAFKA_NODE_COUNT", "1"));

        KafkaAssembly kafka = getKafkaWithName(clusterName);
        assertNotNull(kafka);
        assertEquals(1, kafka.getSpec().getKafka().getReplicas());
        assertEquals(1, kafka.getSpec().getZookeeper().getReplicas());
        assertEquals("persistent-claim", kafka.getSpec().getKafka().getStorage().getType());
        assertEquals("persistent-claim", kafka.getSpec().getZookeeper().getStorage().getType());
    }

    @Test
    @JUnitGroup(name = "regression")
    public void testStrimziEphemeralWithCustomParameters() throws IOException {
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
        KafkaAssembly kafka = getKafkaWithName(clusterName);
        assertNotNull(kafka);

        assertEquals("30", kafka.getSpec().getZookeeper().getLivenessProbe().getInitialDelaySeconds());
        assertEquals("30", kafka.getSpec().getZookeeper().getReadinessProbe().getInitialDelaySeconds());
        assertEquals("10", kafka.getSpec().getZookeeper().getLivenessProbe().getTimeoutSeconds());
        assertEquals("10", kafka.getSpec().getZookeeper().getReadinessProbe().getTimeoutSeconds());
        assertEquals("30", kafka.getSpec().getKafka().getLivenessProbe().getInitialDelaySeconds());
        assertEquals("30", kafka.getSpec().getKafka().getReadinessProbe().getInitialDelaySeconds());
        assertEquals("10", kafka.getSpec().getKafka().getLivenessProbe().getTimeoutSeconds());
        assertEquals("10", kafka.getSpec().getKafka().getReadinessProbe().getTimeoutSeconds());
        assertEquals("2", kafka.getSpec().getKafka().getConfig().get("default.replication.factor"));
        assertEquals("5", kafka.getSpec().getKafka().getConfig().get("offsets.topic.replication.factor"));
        assertEquals("5", kafka.getSpec().getKafka().getConfig().get("transaction.state.log.replication.factor"));
    }

    @Test
    @JUnitGroup(name = "regression")
    public void testStrimziPersistentWithCustomParameters() throws IOException {
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
        KafkaAssembly kafka = getKafkaWithName(clusterName);
        assertNotNull(kafka);

        assertEquals("30", kafka.getSpec().getZookeeper().getLivenessProbe().getInitialDelaySeconds());
        assertEquals("30", kafka.getSpec().getZookeeper().getReadinessProbe().getInitialDelaySeconds());
        assertEquals("10", kafka.getSpec().getZookeeper().getLivenessProbe().getTimeoutSeconds());
        assertEquals("10", kafka.getSpec().getZookeeper().getReadinessProbe().getTimeoutSeconds());
        assertEquals("30", kafka.getSpec().getKafka().getLivenessProbe().getInitialDelaySeconds());
        assertEquals("30", kafka.getSpec().getKafka().getReadinessProbe().getInitialDelaySeconds());
        assertEquals("10", kafka.getSpec().getKafka().getLivenessProbe().getTimeoutSeconds());
        assertEquals("10", kafka.getSpec().getKafka().getReadinessProbe().getTimeoutSeconds());
        assertEquals("2", kafka.getSpec().getKafka().getConfig().get("default.replication.factor"));
        assertEquals("5", kafka.getSpec().getKafka().getConfig().get("offsets.topic.replication.factor"));
        assertEquals("5", kafka.getSpec().getKafka().getConfig().get("transaction.state.log.replication.factor"));
        assertEquals("2Gi", ((PersistentClaimStorage) kafka.getSpec().getKafka().getStorage()).getSize());
        assertEquals("2Gi", ((PersistentClaimStorage) kafka.getSpec().getZookeeper().getStorage()).getSize());
    }

    @Test
    @JUnitGroup(name = "regression")
    public void testConnect() {
        String clusterName = "test-connect";
        oc.newApp("strimzi-connect", map("CLUSTER_NAME", clusterName,
                "INSTANCES", "1"));

        ConfigMap cm = client.configMaps().inNamespace(NAMESPACE).withName(clusterName).get();
        assertNotNull(cm);
        Map<String, String> cmData = cm.getData();
        assertEquals("1", cmData.get("nodes"));
    }

    @Test
    @JUnitGroup(name = "regression")
    public void testS2i() {
        String clusterName = "test-s2i";
        oc.newApp("strimzi-connect-s2i", map("CLUSTER_NAME", clusterName,
                "INSTANCES", "1"));

        ConfigMap cm = client.configMaps().inNamespace(NAMESPACE).withName(clusterName).get();
        assertNotNull(cm);
        Map<String, String> cmData = cm.getData();
        assertEquals("1", cmData.get("nodes"));
    }

    @Test
    @JUnitGroup(name = "regression")
    public void testTopicOperator() {
        String topicName = "test-topic-cm";
        String mapName = "test-topic-cm-foo";
        oc.newApp("strimzi-topic", map(
                "MAP_NAME", mapName,
                "TOPIC_NAME", topicName,
                "TOPIC_PARTITIONS", "10",
                "TOPIC_REPLICAS", "2"));

        ConfigMap cm = client.configMaps().inNamespace(NAMESPACE).withName(mapName).get();
        assertNotNull(cm);
        Map<String, String> cmData = cm.getData();
        assertEquals(topicName, cmData.get("name"));
        assertEquals("10", cmData.get("partitions"));
        assertEquals("2", cmData.get("replicas"));
    }
}
