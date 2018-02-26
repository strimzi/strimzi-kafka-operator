/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
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
 * not that the created resource is processed by controller(s) in the appropriate way.
 */
@RunWith(StrimziRunner.class)
@OpenShiftOnly
@Namespace(OpenShiftTemplatesTest.NAMESPACE)
@Resources(value = "../examples/openshift/cluster-controller", asAdmin = true)
@Resources(value = "../examples/openshift/topic-controller", asAdmin = true)
public class OpenShiftTemplatesTest {

    public static final String NAMESPACE = "template-test";

    @ClassRule
    public static KubeClusterResource cluster = new KubeClusterResource();

    ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testStrimziEphemeral() throws IOException {
        Oc oc = (Oc) cluster.client();
        String clusterName = "foo";
        oc.newApp("strimzi-ephemeral", map("CLUSTER_NAME", clusterName,
                "ZOOKEEPER_NODE_COUNT", "1",
                "KAFKA_NODE_COUNT", "1"));

        KubernetesClient client = new DefaultKubernetesClient();
        ConfigMap cm = client.configMaps().inNamespace(NAMESPACE).withName(clusterName).get();
        assertNotNull(cm);
        Map<String, String> cmData = cm.getData();
        assertEquals("1", cmData.get("kafka-nodes"));
        assertEquals("1", cmData.get("zookeeper-nodes"));
        assertEquals("ephemeral", mapper.readTree(cmData.get("kafka-storage")).get("type").asText());
        assertEquals("ephemeral", mapper.readTree(cmData.get("zookeeper-storage")).get("type").asText());
    }

    @Test
    public void testStrimziPersistent() throws IOException {
        Oc oc = (Oc) cluster.client();
        String clusterName = "bar";
        oc.newApp("strimzi-persistent", map("CLUSTER_NAME", clusterName,
                "ZOOKEEPER_NODE_COUNT", "1",
                "KAFKA_NODE_COUNT", "1"));

        KubernetesClient client = new DefaultKubernetesClient();
        ConfigMap cm = client.configMaps().inNamespace("template-test").withName(clusterName).get();
        assertNotNull(cm);
        Map<String, String> cmData = cm.getData();
        assertEquals("1", cmData.get("kafka-nodes"));
        assertEquals("1", cmData.get("zookeeper-nodes"));
        assertEquals("persistent-claim", mapper.readTree(cmData.get("kafka-storage")).get("type").asText());
        assertEquals("persistent-claim", mapper.readTree(cmData.get("zookeeper-storage")).get("type").asText());
    }

    @Test
    public void testConnect() {
        Oc oc = (Oc) cluster.client();
        String clusterName = "test-connect";
        oc.newApp("strimzi-connect", map("CLUSTER_NAME", clusterName,
                "INSTANCES", "1"));

        KubernetesClient client = new DefaultKubernetesClient();
        ConfigMap cm = client.configMaps().inNamespace("template-test").withName(clusterName).get();
        assertNotNull(cm);
        Map<String, String> cmData = cm.getData();
        assertEquals("1", cmData.get("nodes"));
    }

    @Test
    public void testS2i() {
        Oc oc = (Oc) cluster.client();
        String clusterName = "test-s2i";
        oc.newApp("strimzi-connect-s2i", map("CLUSTER_NAME", clusterName,
                "INSTANCES", "1"));

        KubernetesClient client = new DefaultKubernetesClient();
        ConfigMap cm = client.configMaps().inNamespace("template-test").withName(clusterName).get();
        assertNotNull(cm);
        Map<String, String> cmData = cm.getData();
        assertEquals("1", cmData.get("nodes"));
    }

    @Test
    public void testTopicController() {
        Oc oc = (Oc) cluster.client();
        String topicName = "test-topic-cm";
        String mapName = "test-topic-cm-foo";
        oc.newApp("strimzi-topic", map(
                "MAP_NAME", mapName,
                "TOPIC_NAME", topicName,
                "TOPIC_PARTITIONS", "10",
                "TOPIC_REPLICAS", "2"));

        KubernetesClient client = new DefaultKubernetesClient();
        ConfigMap cm = client.configMaps().inNamespace("template-test").withName(mapName).get();
        assertNotNull(cm);
        Map<String, String> cmData = cm.getData();
        assertEquals(topicName, cmData.get("name"));
        assertEquals("10", cmData.get("partitions"));
        assertEquals("2", cmData.get("replicas"));
    }
}
