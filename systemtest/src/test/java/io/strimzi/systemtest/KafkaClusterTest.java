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
import io.strimzi.test.Namespace;
import io.strimzi.test.OpenShiftOnly;
import io.strimzi.test.Resources;
import io.strimzi.test.StrimziRunner;
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
import java.util.regex.Pattern;

import static io.strimzi.test.TestUtils.map;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(StrimziRunner.class)
@Namespace(KafkaClusterTest.NAMESPACE)
@Resources(value = "../resources/openshift/cluster-controller", asAdmin = true)
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

    @BeforeClass
    public static void waitForCc() {
        // TODO Build this into the annos, or get rid of the annos
        cluster.client().waitForDeployment("strimzi-cluster-controller");
    }

    @Test
    @OpenShiftOnly
    public void testDeployKafkaClusterViaTemplate() {
        Oc oc = (Oc) this.kubeClient;
        String clusterName = "openshift-my-cluster";
        oc.newApp("strimzi-ephemeral", map("CLUSTER_NAME", clusterName));
        oc.waitForStatefulSet(zookeeperStatefulSetName(clusterName), true);
        oc.waitForStatefulSet(kafkaStatefulSetName(clusterName), true);
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
    @Resources("../resources/kubernetes/kafka-ephemeral.yaml")
    public void testKafkaScaleUpScaleDown() {
        // kafka cluster already deployed via annotation
        String clusterName = "my-cluster";
        LOGGER.info("Running kafkaScaleUpScaleDown {}", clusterName);

        kubeClient.waitForStatefulSet(kafkaStatefulSetName(clusterName), true);
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
        kubeClient.waitForStatefulSet(kafkaStatefulSetName(clusterName), true);

        // Test that the new broker has joined the kafka cluster by checking it knows about all the other broker's API versions
        // (execute bash because we want the env vars expanded in the pod)
        String versions = kubeClient.exec(newPodName,
                "/opt/kafka/bin/kafka-broker-api-versions.sh", "--bootstrap-server", "localhost:9092").out();
        for (int brokerId = 0; brokerId < scaleTo; brokerId++) {
            assertTrue(versions.indexOf("(id: " + brokerId + " rack: ") >= 0);
        }
        // TODO Check for k8s events, logs for errors

        // scale down
        LOGGER.info("Scaling down");
        //client.apps().statefulSets().inNamespace(NAMESPACE).withName(kafkaStatefulSetName(clusterName)).scale(initialReplicas, true);
        replaceCm(clusterName, "kafka-nodes", String.valueOf(initialReplicas));
        kubeClient.waitForStatefulSet(kafkaStatefulSetName(clusterName), true);

        final int finalReplicas = client.apps().statefulSets().inNamespace(NAMESPACE).withName(kafkaStatefulSetName(clusterName)).get().getStatus().getReplicas();
        assertEquals(initialReplicas, finalReplicas);
        versions = kubeClient.exec(firstPodName,
                "/opt/kafka/bin/kafka-broker-api-versions.sh", "--bootstrap-server", "localhost:9092").out();

        assertTrue("Expect the added broker, " + newBrokerId + ",  to no longer be present in output of kafka-broker-api-versions.sh",
                versions.indexOf("(id: " + newBrokerId + " rack: ") == -1);
        // TODO Check for k8s events, logs for errors
    }

    @Test
    @Resources("../resources/kubernetes/kafka-ephemeral.yaml")
    public void testZookeeperScaleUpScaleDown() {
        // kafka cluster already deployed via annotation
        String clusterName = "my-cluster";
        LOGGER.info("Running zookeeperScaleUpScaleDown with cluster {}", clusterName);
        kubeClient.waitForStatefulSet(zookeeperStatefulSetName(clusterName), true);
        KubernetesClient client = new DefaultKubernetesClient();
        final int initialReplicas = client.apps().statefulSets().inNamespace(NAMESPACE).withName(zookeeperStatefulSetName(clusterName)).get().getStatus().getReplicas();
        assertEquals(1, initialReplicas);

        // scale up
        final int scaleTo = initialReplicas + 1;
        final int newPodId = initialReplicas;
        final String newPodName = zookeeperPodName(clusterName,  newPodId);
        final String firstPodName = zookeeperPodName(clusterName,  0);
        LOGGER.info("Scaling up to {}", scaleTo);
        replaceCm(clusterName, "zookeeper-nodes", String.valueOf(initialReplicas + 1));
        kubeClient.waitForPod(newPodName);

        // check the new node is either in leader or follower state
        waitForZkMntr(newPodName, Pattern.compile("zk_server_state\\s+(leader|follower)"));

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
        long timeoutMs = 30_000;
        long pollMs = 1_000L;
        long t0 = System.currentTimeMillis();
        String output;
        while (true) {
            try {
                output = kubeClient.exec(pod,
                        "/bin/bash", "-c", "echo mntr | nc localhost 2181").out();

                if (pattern.matcher(output).find()) {
                    break;
                }
                LOGGER.debug("Output, but it's not what's expected: {}", output);
            } catch (KubeClusterException e) {
                LOGGER.trace("Exception while waiting for ZK to become leader/follower, ignoring", e);
            }
            if (System.currentTimeMillis() - t0 > timeoutMs) {
                fail("Timeout after " + timeoutMs + "ms waiting for ZK in pod " + pod + " to become leader/follower");
            }
            try {
                Thread.sleep(pollMs);
            } catch (InterruptedException e2) {
                break;
            }
        }
    }

}
