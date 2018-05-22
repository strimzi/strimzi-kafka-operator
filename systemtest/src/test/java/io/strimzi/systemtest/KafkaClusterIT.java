/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.test.ClusterOperator;
import io.strimzi.test.CmData;
import io.strimzi.test.JUnitGroup;
import io.strimzi.test.KafkaCluster;
import io.strimzi.test.Namespace;
import io.strimzi.test.OpenShiftOnly;
import io.strimzi.test.Resources;
import io.strimzi.test.StrimziRunner;
import io.strimzi.test.Topic;
import io.strimzi.test.k8s.Oc;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static io.strimzi.systemtest.k8s.Events.Created;
import static io.strimzi.systemtest.k8s.Events.Failed;
import static io.strimzi.systemtest.k8s.Events.FailedSync;
import static io.strimzi.systemtest.k8s.Events.FailedValidation;
import static io.strimzi.systemtest.k8s.Events.Killing;
import static io.strimzi.systemtest.k8s.Events.Pulled;
import static io.strimzi.systemtest.k8s.Events.Scheduled;
import static io.strimzi.systemtest.k8s.Events.Started;
import static io.strimzi.systemtest.k8s.Events.SuccessfulDelete;
import static io.strimzi.systemtest.k8s.Events.Unhealthy;
import static io.strimzi.systemtest.matchers.Matchers.hasAllOfReasons;
import static io.strimzi.systemtest.matchers.Matchers.hasNoneOfReasons;
import static io.strimzi.systemtest.matchers.Matchers.valueOfCmEquals;
import static io.strimzi.test.StrimziRunner.TOPIC_CM;
import static io.strimzi.test.TestUtils.map;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.valid4j.matchers.jsonpath.JsonPathMatchers.hasJsonPath;

@RunWith(StrimziRunner.class)
@Namespace(KafkaClusterIT.NAMESPACE)
@ClusterOperator
public class KafkaClusterIT extends AbstractClusterIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaClusterIT.class);

    public static final String NAMESPACE = "kafka-cluster-test";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final String TOPIC_NAME = "test-topic";

    @BeforeClass
    public static void waitForCc() {
        // TODO Build this into the annos, or get rid of the annos
        //cluster.client().waitForDeployment("strimzi-cluster-operator");
    }

    @Test
    @JUnitGroup(name = "regression")
    @OpenShiftOnly
    @Resources(value = "../examples/templates/cluster-operator", asAdmin = true)
    public void testDeployKafkaClusterViaTemplate() {
        Oc oc = (Oc) this.kubeClient;
        String clusterName = "openshift-my-cluster";
        oc.newApp("strimzi-ephemeral", map("CLUSTER_NAME", clusterName));
        oc.waitForStatefulSet(zookeeperClusterName(clusterName), 3);
        oc.waitForStatefulSet(kafkaClusterName(clusterName), 3);
        oc.deleteByName("cm", clusterName);
        oc.waitForResourceDeletion("statefulset", kafkaClusterName(clusterName));
        oc.waitForResourceDeletion("statefulset", zookeeperClusterName(clusterName));
    }

    @Test
    @JUnitGroup(name = "acceptance")
    @KafkaCluster(name = CLUSTER_NAME, kafkaNodes = 3, zkNodes = 1)
    public void testKafkaAndZookeeperScaleUpScaleDown() {
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
        assertThat(events, hasAllOfReasons(Scheduled, Pulled, Created, Started));
        assertThat(events, hasNoneOfReasons(Failed, Unhealthy, FailedSync, FailedValidation));
        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged();

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
        assertThat(getEvents("StatefulSet", kafkaClusterName(CLUSTER_NAME)), hasAllOfReasons(SuccessfulDelete));
        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged();
    }

    @Test
    @JUnitGroup(name = "regression")
    @KafkaCluster(name = CLUSTER_NAME, kafkaNodes = 1, zkNodes = 1)
    public void testZookeeperScaleUpScaleDown() {
        // kafka cluster already deployed via annotation
        LOGGER.info("Running zookeeperScaleUpScaleDown with cluster {}", CLUSTER_NAME);
        //kubeClient.waitForStatefulSet(zookeeperStatefulSetName(CLUSTER_NAME), 1);
        KubernetesClient client = new DefaultKubernetesClient();
        final int initialZkReplicas = client.apps().statefulSets().inNamespace(NAMESPACE).withName(zookeeperClusterName(CLUSTER_NAME)).get().getStatus().getReplicas();
        assertEquals(1, initialZkReplicas);

        // scale up
        final int scaleZkTo = initialZkReplicas + 2;
        final int[] newPodIds = {initialZkReplicas, initialZkReplicas + 1};
        final String[] newZkPodName = {
                zookeeperPodName(CLUSTER_NAME,  newPodIds[0]),
                zookeeperPodName(CLUSTER_NAME,  newPodIds[1])
        };
        final String firstZkPodName = zookeeperPodName(CLUSTER_NAME,  0);
        LOGGER.info("Scaling up to {}", scaleZkTo);
        replaceCm(CLUSTER_NAME, "zookeeper-nodes", String.valueOf(scaleZkTo));
        kubeClient.waitForPod(newZkPodName[0]);
        kubeClient.waitForPod(newZkPodName[1]);

        // check the new node is either in leader or follower state
        waitForZkMntr(firstZkPodName, Pattern.compile("zk_server_state\\s+(leader|follower)"));
        waitForZkMntr(newZkPodName[0], Pattern.compile("zk_server_state\\s+(leader|follower)"));
        waitForZkMntr(newZkPodName[1], Pattern.compile("zk_server_state\\s+(leader|follower)"));

        //Test that first pod does not have errors or failures in events
        List<Event> eventsForFirstPod = getEvents("Pod", newZkPodName[0]);
        assertThat(eventsForFirstPod, hasAllOfReasons(Scheduled, Pulled, Created, Started));
        assertThat(eventsForFirstPod, hasNoneOfReasons(Failed, Unhealthy, FailedSync, FailedValidation));

        //Test that second pod does not have errors or failures in events
        List<Event> eventsForSecondPod = getEvents("Pod", newZkPodName[1]);
        assertThat(eventsForSecondPod, hasAllOfReasons(Scheduled, Pulled, Created, Started));
        assertThat(eventsForSecondPod, hasNoneOfReasons(Failed, Unhealthy, FailedSync, FailedValidation));

        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged();

        // scale down
        LOGGER.info("Scaling down");
        replaceCm(CLUSTER_NAME, "zookeeper-nodes", String.valueOf(1));
        kubeClient.waitForResourceDeletion("po", zookeeperPodName(CLUSTER_NAME,  1));
        // Wait for the one remaining node to enter standalone mode
        waitForZkMntr(firstZkPodName, Pattern.compile("zk_server_state\\s+standalone"));

        //Test that the second pod has event 'Killing'
        assertThat(getEvents("Pod", newZkPodName[1]), hasAllOfReasons(Killing));
        //Test that stateful set has event 'SuccessfulDelete'
        assertThat(getEvents("StatefulSet", zookeeperClusterName(CLUSTER_NAME)), hasAllOfReasons(SuccessfulDelete));
        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged();
    }

    @Test
    @JUnitGroup(name = "regression")
    @KafkaCluster(name = "my-cluster", kafkaNodes = 2, zkNodes = 2, config = {
            @CmData(key = "zookeeper-healthcheck-delay", value = "30"),
            @CmData(key = "zookeeper-healthcheck-timeout", value = "10"),
            @CmData(key = "kafka-healthcheck-delay", value = "30"),
            @CmData(key = "kafka-healthcheck-timeout", value = "10"),
            @CmData(key = "kafka-config", value = "{\"default.replication.factor\": 1,\"offsets.topic.replication.factor\": 1,\"transaction.state.log.replication.factor\": 1}"),
            @CmData(key = "zookeeper-config", value = "{\"timeTick\": 2000, \"initLimit\": 5, \"syncLimit\": 2}")
    })
    public void testCustomAndUpdatedValues() {
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

        LOGGER.info("Verify values before update");
        String configMapBefore = kubeClient.get("cm", clusterName);
        assertThat(configMapBefore, valueOfCmEquals("zookeeper-healthcheck-delay", "30"));
        assertThat(configMapBefore, valueOfCmEquals("zookeeper-healthcheck-timeout", "10"));
        assertThat(configMapBefore, valueOfCmEquals("kafka-healthcheck-delay", "30"));
        assertThat(configMapBefore, valueOfCmEquals("kafka-healthcheck-timeout", "10"));
        assertThat(configMapBefore, valueOfCmEquals("kafka-config", "{\"default.replication.factor\": 1,\"offsets.topic.replication.factor\": 1,\"transaction.state.log.replication.factor\": 1}"));
        assertThat(configMapBefore, valueOfCmEquals("zookeeper-config", "{\"timeTick\": 2000, \"initLimit\": 5, \"syncLimit\": 2}"));

        for (int i = 0; i < expectedKafkaPods; i++) {
            String kafkaPodJson = kubeClient.getResourceAsJson("pod", kafkaPodName(clusterName, i));
            assertEquals("transaction.state.log.replication.factor=1\\ndefault.replication.factor=1\\noffsets.topic.replication.factor=1\\n".replaceAll("\\p{P}", ""), getValueFromJson(kafkaPodJson,
                    globalVariableJsonPathBuilder("KAFKA_CONFIGURATION")));
            assertThat(kafkaPodJson, hasJsonPath("$.spec.containers[*].livenessProbe.initialDelaySeconds", hasItem(30)));
            assertThat(kafkaPodJson, hasJsonPath("$.spec.containers[*].livenessProbe.timeoutSeconds", hasItem(10)));
        }
        LOGGER.info("Testing Zookeepers");
        for (int i = 0; i < expectedZKPods; i++) {
            String zkPodJson = kubeClient.getResourceAsJson("pod", zookeeperPodName(clusterName, i));
            assertEquals("timeTick=2000\\nautopurge.purgeInterval=1\\nsyncLimit=2\\ninitLimit=5\\n".replaceAll("\\p{P}", ""), getValueFromJson(zkPodJson,
                    globalVariableJsonPathBuilder("ZOOKEEPER_CONFIGURATION")));
            assertThat(zkPodJson, hasJsonPath("$.spec.containers[*].livenessProbe.initialDelaySeconds", hasItem(30)));
            assertThat(zkPodJson, hasJsonPath("$.spec.containers[*].livenessProbe.timeoutSeconds", hasItem(10)));
        }

        Map<String, String> changes = new HashMap<>();
        changes.put("zookeeper-healthcheck-delay", "31");
        changes.put("zookeeper-healthcheck-timeout", "11");
        changes.put("kafka-healthcheck-delay", "31");
        changes.put("kafka-healthcheck-timeout", "11");
        changes.put("kafka-config", "{\"default.replication.factor\": 2,\"offsets.topic.replication.factor\": 2,\"transaction.state.log.replication.factor\": 2}");
        changes.put("zookeeper-config", "{\"timeTick\": 2100, \"initLimit\": 6, \"syncLimit\": 3}");
        replaceCm(clusterName, changes);

        for (int i = 0; i < expectedZKPods; i++) {
            kubeClient.waitForResourceUpdate("pod", zookeeperPodName(clusterName, i), zkPodStartTime.get(i));
            kubeClient.waitForPod(zookeeperPodName(clusterName,  i));
        }
        for (int i = 0; i < expectedKafkaPods; i++) {
            kubeClient.waitForResourceUpdate("pod", kafkaPodName(clusterName, i), kafkaPodStartTime.get(i));
            kubeClient.waitForPod(kafkaPodName(clusterName,  i));
        }

        LOGGER.info("Verify values after update");
        String configMapAfter = kubeClient.get("cm", clusterName);
        assertThat(configMapAfter, valueOfCmEquals("zookeeper-healthcheck-delay", "31"));
        assertThat(configMapAfter, valueOfCmEquals("zookeeper-healthcheck-timeout", "11"));
        assertThat(configMapAfter, valueOfCmEquals("kafka-healthcheck-delay", "31"));
        assertThat(configMapAfter, valueOfCmEquals("kafka-healthcheck-timeout", "11"));
        assertThat(configMapAfter, valueOfCmEquals("kafka-config", "{\"default.replication.factor\": 2,\"offsets.topic.replication.factor\": 2,\"transaction.state.log.replication.factor\": 2}"));
        assertThat(configMapAfter, valueOfCmEquals("zookeeper-config", "{\"timeTick\": 2100, \"initLimit\": 6, \"syncLimit\": 3}"));

        for (int i = 0; i < expectedKafkaPods; i++) {
            String kafkaPodJson = kubeClient.getResourceAsJson("pod", kafkaPodName(clusterName, i));
            assertEquals("transaction.state.log.replication.factor=2\\ndefault.replication.factor=2\\noffsets.topic.replication.factor=2\\n".replaceAll("\\p{P}", ""), getValueFromJson(kafkaPodJson,
                    globalVariableJsonPathBuilder("KAFKA_CONFIGURATION")));

            assertThat(kafkaPodJson, hasJsonPath("$.spec.containers[*].livenessProbe.initialDelaySeconds", hasItem(31)));
            assertThat(kafkaPodJson, hasJsonPath("$.spec.containers[*].livenessProbe.timeoutSeconds", hasItem(11)));
        }
        LOGGER.info("Testing Zookeepers");
        for (int i = 0; i < expectedZKPods; i++) {
            String zkPodJson = kubeClient.getResourceAsJson("pod", zookeeperPodName(clusterName, i));
            assertEquals("timeTick=2100\\nautopurge.purgeInterval=1\\nsyncLimit=3\\ninitLimit=6\\n".replaceAll("\\p{P}", ""), getValueFromJson(zkPodJson,
                    globalVariableJsonPathBuilder("ZOOKEEPER_CONFIGURATION")));
            assertThat(zkPodJson, hasJsonPath("$.spec.containers[*].livenessProbe.initialDelaySeconds", hasItem(31)));
            assertThat(zkPodJson, hasJsonPath("$.spec.containers[*].livenessProbe.timeoutSeconds", hasItem(11)));
        }
    }

    @Test
    @JUnitGroup(name = "regression")
    @KafkaCluster(name = CLUSTER_NAME, kafkaNodes = 3, config = {
            @CmData(key = "kafka-config", value = "{\"default.replication.factor\": 3,\"offsets.topic.replication.factor\": 3,\"transaction.state.log.replication.factor\": 3}")
            })
    @Topic(name = TOPIC_NAME, clusterName = "my-cluster")
    public void testSendMessages() {
        int messagesCount = 20;
        sendMessages(CLUSTER_NAME, TOPIC_NAME, messagesCount, 1);
        String consumedMessages = consumeMessages(CLUSTER_NAME, TOPIC_NAME, 1, 30, 2);

        assertThat(consumedMessages, hasJsonPath("$[*].count", hasItem(messagesCount)));
        assertThat(consumedMessages, hasJsonPath("$[*].partitions[*].topic", hasItem(TOPIC_NAME)));

    }

    @KafkaCluster(name = "jvm-resource-cluster",
        kafkaNodes = 1,
        zkNodes = 1,
        config = {
            @CmData(key = "kafka-resources",
                    value = "{ \"limits\": {\"memory\": \"2Gi\", \"cpu\": \"400m\"}, " +
                            "\"requests\": {\"memory\": \"2Gi\", \"cpu\": \"400m\"}}"),
            @CmData(key = "kafka-jvmOptions",
                    value = "{\"-Xmx\": \"1g\", \"-Xms\": \"1G\"}"),
            @CmData(key = "zookeeper-resources",
                    value = "{ \"limits\": {\"memory\": \"1G\", \"cpu\": \"300m\"}, " +
                            "\"requests\": {\"memory\": \"1G\", \"cpu\": \"300m\"} }"),
            @CmData(key = "zookeeper-jvmOptions",
                    value = "{\"-Xmx\": \"600m\", \"-Xms\": \"300m\"}"),
            @CmData(key = "topic-operator-config",
                    value = "{\"resources\": { \"limits\": {\"memory\": \"500M\", \"cpu\": \"300m\"}, " +
                            "\"requests\": {\"memory\": \"500M\", \"cpu\": \"300m\"} } }")
    })

    @Test
    @JUnitGroup(name = "acceptance")
    public void testJvmAndResources() {
        assertResources(NAMESPACE, "jvm-resource-cluster-kafka-0",
                "2Gi", "400m", "2Gi", "400m");
        assertExpectedJavaOpts("jvm-resource-cluster-kafka-0",
                "-Xmx1g", "-Xms1G");

        assertResources(NAMESPACE, "jvm-resource-cluster-zookeeper-0",
                "1G", "300m", "1G", "300m");
        assertExpectedJavaOpts("jvm-resource-cluster-zookeeper-0",
                "-Xmx600m", "-Xms300m");

        String podName = client.pods().inNamespace(NAMESPACE).list().getItems().stream().filter(p -> p.getMetadata().getName().startsWith("jvm-resource-cluster-topic-operator-")).findFirst().get().getMetadata().getName();

        assertResources(NAMESPACE, podName,
                "500M", "300m", "500M", "300m");
    }

    @Test
    @JUnitGroup(name = "regression")
    @KafkaCluster(name = CLUSTER_NAME)
    public void testForTopicOperator() {
        //Createing topics for testing
        kubeClient.create(TOPIC_CM);
        assertThat(listTopicsUsingPodCLI(CLUSTER_NAME, kafkaPodName(CLUSTER_NAME, 1)), hasItem("my-topic"));

        createTopicUsingPodCLI(CLUSTER_NAME, kafkaPodName(CLUSTER_NAME, 1), "topic-from-cli", 1, 1);
        assertThat(listTopicsUsingPodCLI(CLUSTER_NAME, kafkaPodName(CLUSTER_NAME, 1)), hasItems("my-topic", "topic-from-cli"));
        assertThat(kubeClient.list("cm"), hasItems("my-topic", "topic-from-cli", "my-topic"));

        //Updating first topic using pod CLI
        updateTopicPartitionsCountUsingPodCLI(CLUSTER_NAME, kafkaPodName(CLUSTER_NAME, 1), "my-topic", 2);
        assertThat(describeTopicUsingPodCLI(CLUSTER_NAME, kafkaPodName(CLUSTER_NAME, 1), "my-topic"),
                hasItems("PartitionCount:2"));
        String testTopicCM = kubeClient.get("cm", "my-topic");
        assertThat(testTopicCM, valueOfCmEquals("partitions", "2"));

        //Updating second topic via CM update
        replaceCm("topic-from-cli", "partitions", "2");
        assertThat(describeTopicUsingPodCLI(CLUSTER_NAME, kafkaPodName(CLUSTER_NAME, 1), "topic-from-cli"),
                hasItems("PartitionCount:2"));
        testTopicCM = kubeClient.get("cm", "topic-from-cli");
        assertThat(testTopicCM, valueOfCmEquals("partitions", "2"));

        //Deleting first topic by deletion of CM
        kubeClient.deleteByName("cm", "topic-from-cli");
        assertThat(listTopicsUsingPodCLI(CLUSTER_NAME, kafkaPodName(CLUSTER_NAME, 1)), not(hasItems("topic-from-cli")));

        //Deleting another topic using pod CLI
        deleteTopicUsingPodCLI(CLUSTER_NAME, kafkaPodName(CLUSTER_NAME, 1), "my-topic");
        List<String> topics = listTopicsUsingPodCLI(CLUSTER_NAME, kafkaPodName(CLUSTER_NAME, 1));
        assertThat(topics, not(hasItems("topic-from-cli", "my-topic")));
    }
}