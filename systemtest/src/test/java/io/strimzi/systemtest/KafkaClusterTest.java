/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.test.ClusterController;
import io.strimzi.test.KafkaCluster;
import io.strimzi.test.Namespace;
import io.strimzi.test.OpenShiftOnly;
import io.strimzi.test.Resources;
import io.strimzi.test.CmData;
import io.strimzi.test.StrimziRunner;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClient;
import io.strimzi.test.k8s.KubeClusterException;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.k8s.Oc;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import static io.strimzi.test.TestUtils.indent;
import static io.strimzi.test.TestUtils.map;
import static junit.framework.TestCase.assertTrue;
import static matchers.Matchers.valueOfCmEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(StrimziRunner.class)
@Namespace(KafkaClusterTest.NAMESPACE)
@ClusterController
public class KafkaClusterTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaClusterTest.class);

    public static final String NAMESPACE = "kafka-cluster-test";

    @ClassRule
    public static KubeClusterResource cluster = new KubeClusterResource();

    private KubeClient<?> kubeClient = cluster.client();

    private static String kafkaStatefulSetName(String clusterName) {
        return clusterName + "-kafka";
    }

    private static String kafkaPodName(String clusterName, int podId) {
        return kafkaStatefulSetName(clusterName) + "-" + podId;
    }

    private static String zookeeperStatefulSetName(String clusterName) {
        return clusterName + "-zookeeper";
    }

    private static String zookeeperPodName(String clusterName, int podId) {
        return zookeeperStatefulSetName(clusterName) + "-" + podId;
    }

    private static String zookeeperPVCName(String clusterName, int podId) {
        return "data-" + zookeeperStatefulSetName(clusterName) + "-" + podId;
    }

    private static String kafkaPVCName(String clusterName, int podId) {
        return "data-" + kafkaStatefulSetName(clusterName) + "-" + podId;
    }

    @BeforeClass
    public static void waitForCc() {
        // TODO Build this into the annos, or get rid of the annos
        //cluster.client().waitForDeployment("strimzi-cluster-controller");
    }

    @Test
    @Resources(value = "../examples/templates/cluster-controller", asAdmin = true)
    @OpenShiftOnly
    public void testDeployKafkaClusterViaTemplate() {
        Oc oc = (Oc) this.kubeClient;
        String clusterName = "openshift-my-cluster";
        oc.newApp("strimzi-ephemeral", map("CLUSTER_NAME", clusterName));
        oc.waitForStatefulSet(zookeeperStatefulSetName(clusterName), 1);
        oc.waitForStatefulSet(kafkaStatefulSetName(clusterName), 3);
        oc.deleteByName("cm", clusterName);
        oc.waitForResourceDeletion("statefulset", kafkaStatefulSetName(clusterName));
        oc.waitForResourceDeletion("statefulset", zookeeperStatefulSetName(clusterName));
    }

    private void replaceCm(String cmName, String fieldName, String fieldValue) {
        try {
            String jsonString = kubeClient.get("cm", cmName);
            YAMLMapper mapper = new YAMLMapper();
            JsonNode node = mapper.readTree(jsonString);
            ((ObjectNode) node.get("data")).put(fieldName, fieldValue);
            String content = mapper.writeValueAsString(node);
            kubeClient.replaceContent(content);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    @KafkaCluster(name = "my-cluster", kafkaNodes = 3)
    public void testKafkaScaleUpScaleDown() {
        // kafka cluster already deployed via annotation
        String clusterName = "my-cluster";
        LOGGER.info("Running kafkaScaleUpScaleDown {}", clusterName);

        //kubeClient.waitForStatefulSet(kafkaStatefulSetName(clusterName), 3);
        KubernetesClient client = new DefaultKubernetesClient();

        final int initialReplicas = client.apps().statefulSets().inNamespace(NAMESPACE).withName(kafkaStatefulSetName(clusterName)).get().getStatus().getReplicas();
        assertEquals(3, initialReplicas);

        // scale up
        final int scaleTo = initialReplicas + 1;
        final int newPodId = initialReplicas;
        final int newBrokerId = newPodId;
        final String newPodName = kafkaPodName(clusterName,  newPodId);
        final String firstPodName = kafkaPodName(clusterName,  0);
        LOGGER.info("Scaling up to {}", scaleTo);
        replaceCm(clusterName, "kafka-nodes", String.valueOf(initialReplicas + 1));
        kubeClient.waitForStatefulSet(kafkaStatefulSetName(clusterName), initialReplicas + 1);

        // Test that the new broker has joined the kafka cluster by checking it knows about all the other broker's API versions
        // (execute bash because we want the env vars expanded in the pod)
        String versions = getBrokerApiVersions(newPodName);
        for (int brokerId = 0; brokerId < scaleTo; brokerId++) {
            assertTrue(versions.indexOf("(id: " + brokerId + " rack: ") >= 0);
        }
        // TODO Check for k8s events, logs for errors

        // scale down
        LOGGER.info("Scaling down");
        //client.apps().statefulSets().inNamespace(NAMESPACE).withName(kafkaStatefulSetName(clusterName)).scale(initialReplicas, true);
        replaceCm(clusterName, "kafka-nodes", String.valueOf(initialReplicas));
        kubeClient.waitForStatefulSet(kafkaStatefulSetName(clusterName), initialReplicas);

        final int finalReplicas = client.apps().statefulSets().inNamespace(NAMESPACE).withName(kafkaStatefulSetName(clusterName)).get().getStatus().getReplicas();
        assertEquals(initialReplicas, finalReplicas);
        versions = getBrokerApiVersions(firstPodName);

        assertTrue("Expect the added broker, " + newBrokerId + ",  to no longer be present in output of kafka-broker-api-versions.sh",
                versions.indexOf("(id: " + newBrokerId + " rack: ") == -1);
        // TODO Check for k8s events, logs for errors
    }

    private String getBrokerApiVersions(String podName) {
        AtomicReference<String> versions = new AtomicReference<>();
        TestUtils.waitFor("kafka-broker-api-versions.sh success", 1_000L, 30_000L, () -> {
            try {
                String output = kubeClient.exec(podName,
                        "/opt/kafka/bin/kafka-broker-api-versions.sh", "--bootstrap-server", "localhost:9092").out();
                versions.set(output);
                return true;
            } catch (KubeClusterException e) {
                LOGGER.trace("/opt/kafka/bin/kafka-broker-api-versions.sh: {}", e.getMessage());
                return false;
            }
        });
        return versions.get();
    }

    @Test
    @KafkaCluster(name = "my-cluster", kafkaNodes = 1, zkNodes = 1)
    public void testZookeeperScaleUpScaleDown() {
        // kafka cluster already deployed via annotation
        String clusterName = "my-cluster";
        LOGGER.info("Running zookeeperScaleUpScaleDown with cluster {}", clusterName);
        //kubeClient.waitForStatefulSet(zookeeperStatefulSetName(clusterName), 1);
        KubernetesClient client = new DefaultKubernetesClient();
        final int initialReplicas = client.apps().statefulSets().inNamespace(NAMESPACE).withName(zookeeperStatefulSetName(clusterName)).get().getStatus().getReplicas();
        assertEquals(1, initialReplicas);

        // scale up
        final int scaleTo = initialReplicas + 2;
        final int[] newPodIds = {initialReplicas, initialReplicas + 1};
        final String[] newPodName = {
                zookeeperPodName(clusterName,  newPodIds[0]),
                zookeeperPodName(clusterName,  newPodIds[1])
        };
        final String firstPodName = zookeeperPodName(clusterName,  0);
        LOGGER.info("Scaling up to {}", scaleTo);
        replaceCm(clusterName, "zookeeper-nodes", String.valueOf(scaleTo));
        kubeClient.waitForPod(newPodName[0]);
        kubeClient.waitForPod(newPodName[1]);

        // check the new node is either in leader or follower state
        waitForZkMntr(firstPodName, Pattern.compile("zk_server_state\\s+(leader|follower)"));
        waitForZkMntr(newPodName[0], Pattern.compile("zk_server_state\\s+(leader|follower)"));
        waitForZkMntr(newPodName[1], Pattern.compile("zk_server_state\\s+(leader|follower)"));

        // TODO Check for k8s events, logs for errors

        // scale down
        LOGGER.info("Scaling down");
        replaceCm(clusterName, "zookeeper-nodes", String.valueOf(1));
        kubeClient.waitForResourceDeletion("po", zookeeperPodName(clusterName,  1));
        // Wait for the one remaining node to enter standalone mode
        waitForZkMntr(firstPodName, Pattern.compile("zk_server_state\\s+standalone"));

        // TODO Check for k8s events, logs for errors
    }

    private void waitForZkMntr(String pod, Pattern pattern) {
        long timeoutMs = 120_000L;
        long pollMs = 1_000L;
        TestUtils.waitFor("mntr", pollMs, timeoutMs, () -> {
            try {
                String output = kubeClient.exec(pod,
                        "/bin/bash", "-c", "echo mntr | nc localhost 2181").out();

                if (pattern.matcher(output).find()) {
                    return true;
                }
            } catch (KubeClusterException e) {
                LOGGER.trace("Exception while waiting for ZK to become leader/follower, ignoring", e);
            }
            return false;
        },
            () -> LOGGER.info("zookeeper `mntr` output at the point of timeout does not match {}:{}{}",
                pattern.pattern(),
                System.lineSeparator(),
                indent(kubeClient.exec(pod, "/bin/bash", "-c", "echo mntr | nc localhost 2181").out()))
        );
    }

    @Test
    @KafkaCluster(name = "my-cluster", kafkaNodes = 1, config = {
        @CmData(key = "zookeeper-healthcheck-delay", value = "30"),
        @CmData(key = "zookeeper-healthcheck-timeout", value = "10"),
        @CmData(key = "kafka-healthcheck-delay", value = "30"),
        @CmData(key = "kafka-healthcheck-timeout", value = "10"),
        @CmData(key = "KAFKA_DEFAULT_REPLICATION_FACTOR", value = "2"),
        @CmData(key = "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", value = "5"),
        @CmData(key = "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", value = "5")
    })
    public void testClusterWithCustomParameters() {
        // kafka cluster already deployed via annotation
        String clusterName = "my-cluster";
        LOGGER.info("Running clusterWithCustomParameters with cluster {}", clusterName);

        //TODO Add assertions to check that Kafka brokers have a custom configuration
        String jsonString = kubeClient.get("cm", clusterName);
        assertThat(jsonString, valueOfCmEquals("zookeeper-healthcheck-delay", "30"));
        assertThat(jsonString, valueOfCmEquals("zookeeper-healthcheck-timeout", "10"));
        assertThat(jsonString, valueOfCmEquals("kafka-healthcheck-delay", "30"));
        assertThat(jsonString, valueOfCmEquals("kafka-healthcheck-timeout", "10"));
        assertThat(jsonString, valueOfCmEquals("KAFKA_DEFAULT_REPLICATION_FACTOR", "2"));
        assertThat(jsonString, valueOfCmEquals("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "5"));
        assertThat(jsonString, valueOfCmEquals("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "5"));
    }

    @Test
    @KafkaCluster(name = "my-cluster-persistent", kafkaNodes = 2, zkNodes = 2, config = {
        @CmData(key = "kafka-storage", value = "{ \"type\": \"persistent-claim\", \"size\": \"1Gi\", \"delete-claim\": false }"),
        @CmData(key = "zookeeper-storage", value = "{ \"type\": \"persistent-claim\", \"size\": \"1Gi\", \"delete-claim\": false }"),
        @CmData(key = "zookeeper-healthcheck-delay", value = "30"),
        @CmData(key = "zookeeper-healthcheck-timeout", value = "15"),
        @CmData(key = "kafka-healthcheck-delay", value = "30"),
        @CmData(key = "kafka-healthcheck-timeout", value = "15"),
        @CmData(key = "KAFKA_DEFAULT_REPLICATION_FACTOR", value = "2"),
        @CmData(key = "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", value = "5"),
        @CmData(key = "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", value = "5")
    })
    @OpenShiftOnly
    public void testDeployKafkaOnPersistentStorage() {
        String clusterName = "my-cluster-persistent";
        int expectedZKPods = 2;
        int expectedKafkaPods = 2;
        Oc oc = (Oc) this.kubeClient;

        List<String> persistentVolumeClaimNames = oc.list("pvc");
        assertTrue(persistentVolumeClaimNames.size() == (expectedZKPods + expectedKafkaPods));

        //Checking Persistent volume claims for Zookeeper nodes
        for (int i = 0; i < expectedZKPods; i++) {
            assertTrue(persistentVolumeClaimNames.contains(zookeeperPVCName(clusterName, i)));
        }

        //Checking Persistent volume claims for Kafka nodes
        for (int i = 0; i < expectedZKPods; i++) {
            assertTrue(persistentVolumeClaimNames.contains(kafkaPVCName(clusterName, i)));
        }

        String configMap = kubeClient.get("cm", clusterName);
        assertThat(configMap, valueOfCmEquals("zookeeper-healthcheck-delay", "30"));
        assertThat(configMap, valueOfCmEquals("zookeeper-healthcheck-timeout", "15"));
        assertThat(configMap, valueOfCmEquals("kafka-healthcheck-delay", "30"));
        assertThat(configMap, valueOfCmEquals("kafka-healthcheck-timeout", "15"));
        assertThat(configMap, valueOfCmEquals("KAFKA_DEFAULT_REPLICATION_FACTOR", "2"));
        assertThat(configMap, valueOfCmEquals("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "5"));
        assertThat(configMap, valueOfCmEquals("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "5"));
    }
}
