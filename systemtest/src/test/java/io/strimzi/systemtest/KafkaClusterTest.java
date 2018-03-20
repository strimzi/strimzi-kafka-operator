/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.jayway.jsonpath.JsonPath;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.test.ClusterController;
import io.strimzi.test.CmData;
import io.strimzi.test.KafkaCluster;
import io.strimzi.test.Namespace;
import io.strimzi.test.OpenShiftOnly;
import io.strimzi.test.Resources;
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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.k8s.Events.Created;
import static io.strimzi.systemtest.k8s.Events.Failed;
import static io.strimzi.systemtest.k8s.Events.FailedSync;
import static io.strimzi.systemtest.k8s.Events.FailedValidation;
import static io.strimzi.systemtest.k8s.Events.Killing;
import static io.strimzi.systemtest.k8s.Events.Pulled;
import static io.strimzi.systemtest.k8s.Events.Pulling;
import static io.strimzi.systemtest.k8s.Events.Scheduled;
import static io.strimzi.systemtest.k8s.Events.Started;
import static io.strimzi.systemtest.k8s.Events.SuccessfulDelete;
import static io.strimzi.systemtest.k8s.Events.Unhealthy;
import static io.strimzi.systemtest.matchers.Matchers.hasAllOfReasons;
import static io.strimzi.systemtest.matchers.Matchers.hasNoneOfReasons;
import static io.strimzi.systemtest.matchers.Matchers.valueOfCmEquals;
import static io.strimzi.test.TestUtils.indent;
import static io.strimzi.test.TestUtils.map;
import static io.strimzi.test.k8s.BaseKubeClient.CM;
import static io.strimzi.test.k8s.BaseKubeClient.DEPLOYMENT;
import static io.strimzi.test.k8s.BaseKubeClient.SERVICE;
import static io.strimzi.test.k8s.BaseKubeClient.STATEFUL_SET;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;


@RunWith(StrimziRunner.class)
@Namespace(KafkaClusterTest.NAMESPACE)
@ClusterController
public class KafkaClusterTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaClusterTest.class);

    public static final String NAMESPACE = "kafka-cluster-test";
    private static final String CLUSTER_NAME = "my-cluster";

    @ClassRule
    public static KubeClusterResource cluster = new KubeClusterResource();

    private KubeClient<?> kubeClient = cluster.client();
    private KubernetesClient client = new DefaultKubernetesClient();

    // can be used as kafka stateful set or service names
    private static String kafkaClusterName(String clusterName) {
        return clusterName + "-kafka";
    }

    private static String kafkaPodName(String clusterName, int podId) {
        return kafkaClusterName(clusterName) + "-" + podId;
    }

    private static String kafkaHeadlessServiceName(String clusterName) {
        return kafkaClusterName(clusterName) + "-headless";
    }

    private static String kafkaMetricsConfigName(String clusterName) {
        return kafkaClusterName(clusterName) + "-metrics-config";
    }

    private static String kafkaPVCName(String clusterName, int podId) {
        return "data-" + kafkaClusterName(clusterName) + "-" + podId;
    }

    // can be used as zookeeper stateful set or service names
    private static String zookeeperClusterName(String clusterName) {
        return clusterName + "-zookeeper";
    }

    private static String zookeeperPodName(String clusterName, int podId) {
        return zookeeperClusterName(clusterName) + "-" + podId;
    }

    private static String zookeeperHeadlessServiceName(String clusterName) {
        return zookeeperClusterName(clusterName) + "-headless";
    }

    private static String zookeeperMetricsConfigName(String clusterName) {
        return zookeeperClusterName(clusterName) + "-metrics-config";
    }

    private static String zookeeperPVCName(String clusterName, int podId) {
        return "data-" + zookeeperClusterName(clusterName) + "-" + podId;
    }

    private static String topicControllerDeploymentName(String clusterName) {
        return clusterName + "-topic-controller";
    }

    private static String topicControllerDeploymentName(String clusterName) {
        return clusterName + "-topic-controller";
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
        oc.waitForStatefulSet(zookeeperClusterName(clusterName), 1);
        oc.waitForStatefulSet(kafkaClusterName(clusterName), 3);
        oc.deleteByName("cm", clusterName);
        oc.waitForResourceDeletion("statefulset", kafkaClusterName(clusterName));
        oc.waitForResourceDeletion("statefulset", zookeeperClusterName(clusterName));
    }

    private void replaceCm(String cmName, String fieldName, String fieldValue) {
        try {
            String jsonString = kubeClient.get("cm", cmName);
            YAMLMapper mapper = new YAMLMapper();
            JsonNode node = mapper.readTree(jsonString);
            ((ObjectNode) node.get("data")).put(fieldName, fieldValue);
            String content = mapper.writeValueAsString(node);
            kubeClient.replaceContent(content);
            LOGGER.info("Value in Config Map replaced");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    @KafkaCluster(name = CLUSTER_NAME, kafkaNodes = 3)
    public void testKafkaScaleUpScaleDown() {
        // kafka cluster already deployed via annotation
        LOGGER.info("Running kafkaScaleUpScaleDown {}", CLUSTER_NAME);

        //kubeClient.waitForStatefulSet(kafkaStatefulSetName(clusterName), 3);

        final int initialReplicas = client.apps().statefulSets().inNamespace(NAMESPACE).withName(kafkaClusterName(CLUSTER_NAME)).get().getStatus().getReplicas();
        assertEquals(3, initialReplicas);

        // scale up
        final int scaleTo = initialReplicas + 1;
        final int newPodId = initialReplicas;
        final int newBrokerId = newPodId;
        final String newPodName = kafkaPodName(CLUSTER_NAME,  newPodId);
        final String firstPodName = kafkaPodName(CLUSTER_NAME,  0);
        LOGGER.info("Scaling up to {}", scaleTo);
        replaceCm(CLUSTER_NAME, "kafka-nodes", String.valueOf(initialReplicas + 1));
        kubeClient.waitForStatefulSet(kafkaClusterName(CLUSTER_NAME), initialReplicas + 1);

        // Test that the new broker has joined the kafka cluster by checking it knows about all the other broker's API versions
        // (execute bash because we want the env vars expanded in the pod)
        String versions = getBrokerApiVersions(newPodName);
        for (int brokerId = 0; brokerId < scaleTo; brokerId++) {
            assertTrue(versions, versions.indexOf("(id: " + brokerId + " rack: ") >= 0);
        }

        //Test that the new pod does not have errors or failures in events
        List<Event> events = getEvents("Pod", newPodName);
        assertThat(events, hasAllOfReasons(Scheduled, Pulling, Pulled, Created, Started));
        assertThat(events, hasNoneOfReasons(Failed, Unhealthy, FailedSync, FailedValidation));

        // TODO Check logs for errors

        // scale down
        LOGGER.info("Scaling down");
        //client.apps().statefulSets().inNamespace(NAMESPACE).withName(kafkaStatefulSetName(CLUSTER_NAME)).scale(initialReplicas, true);
        replaceCm(CLUSTER_NAME, "kafka-nodes", String.valueOf(initialReplicas));
        kubeClient.waitForStatefulSet(kafkaClusterName(CLUSTER_NAME), initialReplicas);

        final int finalReplicas = client.apps().statefulSets().inNamespace(NAMESPACE).withName(kafkaClusterName(CLUSTER_NAME)).get().getStatus().getReplicas();
        assertEquals(initialReplicas, finalReplicas);
        versions = getBrokerApiVersions(firstPodName);

        assertTrue("Expect the added broker, " + newBrokerId + ",  to no longer be present in output of kafka-broker-api-versions.sh",
                versions.indexOf("(id: " + newBrokerId + " rack: ") == -1);

        //Test that the new broker has event 'Killing'
        assertThat(getEvents("Pod", newPodName), hasAllOfReasons(Killing));
        //Test that stateful set has event 'SuccessfulDelete'
        assertThat(getEvents("StatefulSet", kafkaStatefulSetName(clusterName)), hasAllOfReasons(SuccessfulDelete));

        // TODO Check logs for errors
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
    @KafkaCluster(name = CLUSTER_NAME, kafkaNodes = 1, zkNodes = 1)
    public void testZookeeperScaleUpScaleDown() {
        // kafka cluster already deployed via annotation
        LOGGER.info("Running zookeeperScaleUpScaleDown with cluster {}", CLUSTER_NAME);
        //kubeClient.waitForStatefulSet(zookeeperStatefulSetName(CLUSTER_NAME), 1);
        KubernetesClient client = new DefaultKubernetesClient();
        final int initialReplicas = client.apps().statefulSets().inNamespace(NAMESPACE).withName(zookeeperClusterName(CLUSTER_NAME)).get().getStatus().getReplicas();
        assertEquals(1, initialReplicas);

        // scale up
        final int scaleTo = initialReplicas + 2;
        final int[] newPodIds = {initialReplicas, initialReplicas + 1};
        final String[] newPodName = {
                zookeeperPodName(CLUSTER_NAME,  newPodIds[0]),
                zookeeperPodName(CLUSTER_NAME,  newPodIds[1])
        };
        final String firstPodName = zookeeperPodName(CLUSTER_NAME,  0);
        LOGGER.info("Scaling up to {}", scaleTo);
        replaceCm(CLUSTER_NAME, "zookeeper-nodes", String.valueOf(scaleTo));
        kubeClient.waitForPod(newPodName[0]);
        kubeClient.waitForPod(newPodName[1]);

        // check the new node is either in leader or follower state
        waitForZkMntr(firstPodName, Pattern.compile("zk_server_state\\s+(leader|follower)"));
        waitForZkMntr(newPodName[0], Pattern.compile("zk_server_state\\s+(leader|follower)"));
        waitForZkMntr(newPodName[1], Pattern.compile("zk_server_state\\s+(leader|follower)"));

        // TODO Check logs for errors
        //Test that first pod does not have errors or failures in events
        List<Event> eventsForFirstPod = getEvents("Pod", newPodName[0]);
        assertThat(eventsForFirstPod, hasAllOfReasons(Scheduled, Pulling, Pulled, Created, Started));
        assertThat(eventsForFirstPod, hasNoneOfReasons(Failed, Unhealthy, FailedSync, FailedValidation));

        //Test that second pod does not have errors or failures in events
        List<Event> eventsForSecondPod = getEvents("Pod", newPodName[1]);
        assertThat(eventsForSecondPod, hasAllOfReasons(Scheduled, Pulling, Pulled, Created, Started));
        assertThat(eventsForSecondPod, hasNoneOfReasons(Failed, Unhealthy, FailedSync, FailedValidation));

        // scale down
        LOGGER.info("Scaling down");
        replaceCm(CLUSTER_NAME, "zookeeper-nodes", String.valueOf(1));
        kubeClient.waitForResourceDeletion("po", zookeeperPodName(CLUSTER_NAME,  1));
        // Wait for the one remaining node to enter standalone mode
        waitForZkMntr(firstPodName, Pattern.compile("zk_server_state\\s+standalone"));

        //Test that the second pod has event 'Killing'
        assertThat(getEvents("Pod", newPodName[1]), hasAllOfReasons(Killing));
        //Test that stateful set has event 'SuccessfulDelete'
        assertThat(getEvents("StatefulSet", zookeeperStatefulSetName(clusterName)), hasAllOfReasons(SuccessfulDelete));
        // TODO Check logs for errors
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
    @KafkaCluster(name = CLUSTER_NAME, kafkaNodes = 1, config = {
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
        LOGGER.info("Running clusterWithCustomParameters with cluster {}", CLUSTER_NAME);

        //TODO Add assertions to check that Kafka brokers have a custom configuration
        String jsonString = kubeClient.get("cm", CLUSTER_NAME);
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

    @Test
    @OpenShiftOnly
    @KafkaCluster(name = "my-cluster", kafkaNodes = 2, zkNodes = 2, config = {
            @CmData(key = "zookeeper-healthcheck-delay", value = "30"),
            @CmData(key = "zookeeper-healthcheck-timeout", value = "10"),
            @CmData(key = "kafka-healthcheck-delay", value = "30"),
            @CmData(key = "kafka-healthcheck-timeout", value = "10"),
            @CmData(key = "KAFKA_DEFAULT_REPLICATION_FACTOR", value = "2"),
            @CmData(key = "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", value = "5"),
            @CmData(key = "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", value = "5")
    })
    public void testForUpdateValuesInConfigMap() {
        String clusterName = "my-cluster";
        int expectedZKPods = 2;
        int expectedKafkaPods = 2;
        List<Date> zkPodStartTime = new ArrayList<>();
        for (int i = 0; i < expectedZKPods; i++) {
            zkPodStartTime.add(kubeClient.getResourceCreateTimestamp("pod", zookeeperPodName(clusterName, i)));
        }
        List<Date> kafkaPodStartTime = new ArrayList<>();
        for (int i = 0; i < expectedKafkaPods; i++) {
            kafkaPodStartTime.add(kubeClient.getResourceCreateTimestamp("pod", kafkaPodName(clusterName, i)));
        }
        Oc oc = (Oc) this.kubeClient;
        replaceCm(clusterName, "zookeeper-healthcheck-delay", "23");
        replaceCm(clusterName, "kafka-healthcheck-delay", "23");
        replaceCm(clusterName, "KAFKA_DEFAULT_REPLICATION_FACTOR", "1");
        for (int i = 0; i < expectedZKPods; i++) {
            kubeClient.waitForResourceUpdate("pod", zookeeperPodName(clusterName, i), zkPodStartTime.get(i));
            kubeClient.waitForPod(zookeeperPodName(clusterName,  i));
        }
        for (int i = 0; i < expectedKafkaPods; i++) {
            kubeClient.waitForResourceUpdate("pod", kafkaPodName(clusterName, i), kafkaPodStartTime.get(i));
            kubeClient.waitForPod(kafkaPodName(clusterName,  i));
        }
        String configMap = kubeClient.get("cm", clusterName);
        assertThat(configMap, valueOfCmEquals("zookeeper-healthcheck-delay", "23"));
        assertThat(configMap, valueOfCmEquals("kafka-healthcheck-delay", "23"));
        assertThat(configMap, valueOfCmEquals("KAFKA_DEFAULT_REPLICATION_FACTOR", "1"));
        LOGGER.info("Verified CM and Testing kafka pods");
        for (int i = 0; i < expectedKafkaPods; i++) {
            String kafkaPodJson = oc.getResourceAsJson("pod", kafkaPodName(clusterName, i));
            assertEquals("1", getValueFromJson(kafkaPodJson,
                    globalVariableJsonPathBuilder("KAFKA_DEFAULT_REPLICATION_FACTOR")));
            String initialDelaySecondsPath = "$.spec.containers[*].livenessProbe.initialDelaySeconds";
            assertEquals("23", getValueFromJson(kafkaPodJson, initialDelaySecondsPath));
        }
        LOGGER.info("Testing Zookeepers");
        for (int i = 0; i < expectedZKPods; i++) {
            String zkPodJson = kubeClient.getResourceAsJson("pod", zookeeperPodName(clusterName, i));
            String initialDelaySecondsPath = "$.spec.containers[*].livenessProbe.initialDelaySeconds";
            assertEquals("23", getValueFromJson(zkPodJson, initialDelaySecondsPath));
        }
    }

    @Test
    @KafkaCluster(name = CLUSTER_NAME, kafkaNodes = 1)
    public void testDeleteTopicControllerDeployment() {
        // kafka cluster already deployed via annotation
        String topicControllerDeploymentName = topicControllerDeploymentName(CLUSTER_NAME);
        LOGGER.info("Running deleteTopicControllerDeployment with cluster {}", CLUSTER_NAME);

        kubeClient.deleteByName(DEPLOYMENT, topicControllerDeploymentName);
        kubeClient.waitForResourceDeletion(DEPLOYMENT, topicControllerDeploymentName);

        LOGGER.info("Waiting for recovery {}", topicControllerDeploymentName);
        kubeClient.waitForDeployment(topicControllerDeploymentName);
    }

    @Test
    @KafkaCluster(name = CLUSTER_NAME, kafkaNodes = 1)
    public void testDeleteKafkaStatefulSet() {
        // kafka cluster already deployed via annotation
        String kafkaStatefulSetName = kafkaClusterName(CLUSTER_NAME);

        LOGGER.info("Running deleteKafkaStatefulSet with cluster {}", CLUSTER_NAME);

        kubeClient.deleteByName(STATEFUL_SET, kafkaStatefulSetName);
        kubeClient.waitForResourceDeletion(STATEFUL_SET, kafkaStatefulSetName);

        LOGGER.info("Waiting for recovery {}", kafkaStatefulSetName);
        kubeClient.waitForStatefulSet(kafkaStatefulSetName, 1);
    }

    @Test
    @KafkaCluster(name = CLUSTER_NAME, kafkaNodes = 1, zkNodes = 1)
    public void testDeleteZookeeperStatefulSet() {
        // kafka cluster already deployed via annotation
        String zookeeperStatefulSetName = zookeeperClusterName(CLUSTER_NAME);

        LOGGER.info("Running deleteZookeeperStatefulSet with cluster {}", CLUSTER_NAME);

        kubeClient.deleteByName(STATEFUL_SET, zookeeperStatefulSetName);
        kubeClient.waitForResourceDeletion(STATEFUL_SET, zookeeperStatefulSetName);

        LOGGER.info("Waiting for recovery {}", zookeeperStatefulSetName);
        kubeClient.waitForStatefulSet(zookeeperStatefulSetName, 1);
    }

    private String getValueFromJson(String json, String jsonPath) {
        String value = JsonPath.parse(json).read(jsonPath).toString().replaceAll("\\p{P}", "");
        return value;
    }

    private String globalVariableJsonPathBuilder(String variable) {
        String path = "$.spec.containers[*].env[?(@.name=='" + variable + "')].value";
        return path;
    }

    @Test
    @KafkaCluster(name = CLUSTER_NAME, kafkaNodes = 1)
    public void testDeleteKafkaService() {
        // kafka cluster already deployed via annotation
        String kafkaServiceName = kafkaClusterName(CLUSTER_NAME);

        LOGGER.info("Running deleteKafkaService with cluster {}", CLUSTER_NAME);

        kubeClient.deleteByName(SERVICE, kafkaServiceName);
        kubeClient.waitForResourceDeletion(SERVICE, kafkaServiceName);

        LOGGER.info("Waiting for creation {}", kafkaServiceName);
        kubeClient.waitForResourceCreation(SERVICE, kafkaServiceName);
    }

    @Test
    @KafkaCluster(name = CLUSTER_NAME, kafkaNodes = 1)
    public void testDeleteZookeeperService() {
        // kafka cluster already deployed via annotation
        String zookeeperServiceName = zookeeperClusterName(CLUSTER_NAME);

        LOGGER.info("Running deleteKafkaService with cluster {}", CLUSTER_NAME);

        kubeClient.deleteByName(SERVICE, zookeeperServiceName);
        kubeClient.waitForResourceDeletion(SERVICE, zookeeperServiceName);

        LOGGER.info("Waiting for creation {}", zookeeperServiceName);
        kubeClient.waitForResourceCreation(SERVICE, zookeeperServiceName);
    }

    @Test
    @KafkaCluster(name = CLUSTER_NAME, kafkaNodes = 1)
    public void testDeleteKafkaHeadlessService() {
        // kafka cluster already deployed via annotation
        String kafkaHeadlessServiceName = kafkaHeadlessServiceName(CLUSTER_NAME);

        LOGGER.info("Running deleteKafkaHeadlessService with cluster {}", CLUSTER_NAME);

        kubeClient.deleteByName(SERVICE, kafkaHeadlessServiceName);
        kubeClient.waitForResourceDeletion(SERVICE, kafkaHeadlessServiceName);

        LOGGER.info("Waiting for creation {}", kafkaHeadlessServiceName);
        kubeClient.waitForResourceCreation(SERVICE, kafkaHeadlessServiceName);
    }

    @Test
    @KafkaCluster(name = CLUSTER_NAME, kafkaNodes = 1)
    public void testDeleteZookeeperHeadlessService() {
        // kafka cluster already deployed via annotation
        String zookeeperHeadlessServiceName = zookeeperHeadlessServiceName(CLUSTER_NAME);

        LOGGER.info("Running deleteKafkaHeadlessService with cluster {}", CLUSTER_NAME);

        kubeClient.deleteByName(SERVICE, zookeeperHeadlessServiceName);
        kubeClient.waitForResourceDeletion(SERVICE, zookeeperHeadlessServiceName);

        LOGGER.info("Waiting for creation {}", zookeeperHeadlessServiceName);
        kubeClient.waitForResourceCreation(SERVICE, zookeeperHeadlessServiceName);
    }

    @Test
    @KafkaCluster(name = CLUSTER_NAME, kafkaNodes = 1)
    public void testDeleteKafkaMetricsConfig() {
        // kafka cluster already deployed via annotation
        String kafkaMetricsConfigName = kafkaMetricsConfigName(CLUSTER_NAME);

        LOGGER.info("Running deleteKafkaMetricsConfig with cluster {}", CLUSTER_NAME);

        kubeClient.deleteByName(CM, kafkaMetricsConfigName);
        kubeClient.waitForResourceDeletion(CM, kafkaMetricsConfigName);

        LOGGER.info("Waiting for creation {}", kafkaMetricsConfigName);
        kubeClient.waitForResourceCreation(CM, kafkaMetricsConfigName);
    }

    @Test
    @KafkaCluster(name = CLUSTER_NAME, kafkaNodes = 1)
    public void testDeleteZookeeperMetricsConfig() {
        // kafka cluster already deployed via annotation
        String zookeeperMetricsConfigName = zookeeperMetricsConfigName(CLUSTER_NAME);

        LOGGER.info("Running deleteZookeeperMetricsConfig with cluster {}", CLUSTER_NAME);

        kubeClient.deleteByName(CM, zookeeperMetricsConfigName);
        kubeClient.waitForResourceDeletion(CM, zookeeperMetricsConfigName);

        LOGGER.info("Waiting for creation {}", zookeeperMetricsConfigName);
        kubeClient.waitForResourceCreation(CM, zookeeperMetricsConfigName);
    }

    private List<Event> getEvents(String resourceType, String resourceName) {
        return client.events().inNamespace(NAMESPACE).list().getItems().stream()
                .filter(event -> event.getInvolvedObject().getKind().equals(resourceType))
                .filter(event -> event.getInvolvedObject().getName().equals(resourceName))
                .collect(Collectors.toList());
    }
}