/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.Zookeeper;
import io.strimzi.test.ClusterOperator;
import io.strimzi.test.JUnitGroup;
import io.strimzi.test.KafkaFromClasspathYaml;
import io.strimzi.test.Namespace;
import io.strimzi.test.OpenShiftOnly;
import io.strimzi.test.Resources;
import io.strimzi.test.StrimziRunner;
import io.strimzi.test.TestUtils;
import io.strimzi.test.Topic;
import io.strimzi.test.k8s.Oc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Date;
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
import static java.util.concurrent.TimeUnit.SECONDS;
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

    private static final Logger LOGGER = LogManager.getLogger(KafkaClusterIT.class);

    public static final String NAMESPACE = "kafka-cluster-test";
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

        //Testing docker images
        testDockerImagesForKafkaCluster(clusterName, 3, 3, false);

        oc.deleteByName("Kafka", clusterName);
        oc.waitForResourceDeletion("statefulset", kafkaClusterName(clusterName));
        oc.waitForResourceDeletion("statefulset", zookeeperClusterName(clusterName));
    }

    @Test
    @JUnitGroup(name = "acceptance")
    @KafkaFromClasspathYaml()
    public void testKafkaAndZookeeperScaleUpScaleDown() {
        testDockerImagesForKafkaCluster(CLUSTER_NAME, 3, 1, false);
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
        replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec().getKafka().setReplicas(initialReplicas + 1);
        });
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
        assertNoCoErrorsLogged(stopwatch.runtime(SECONDS));

        // scale down
        LOGGER.info("Scaling down");
        //client.apps().statefulSets().inNamespace(NAMESPACE).withName(kafkaStatefulSetName(CLUSTER_NAME)).scale(initialReplicas, true);
        replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec().getKafka().setReplicas(initialReplicas);
        });
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
        assertNoCoErrorsLogged(stopwatch.runtime(SECONDS));
    }

    @Test
    @JUnitGroup(name = "regression")
    @KafkaFromClasspathYaml()
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
        replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec().getZookeeper().setReplicas(scaleZkTo);
        });
        kubeClient.waitForPod(newZkPodName[0]);
        kubeClient.waitForPod(newZkPodName[1]);

        // check the new node is either in leader or follower state
        waitForZkMntr(Pattern.compile("zk_server_state\\s+(leader|follower)"), 0, 1, 2);

        //Test that first pod does not have errors or failures in events
        List<Event> eventsForFirstPod = getEvents("Pod", newZkPodName[0]);
        assertThat(eventsForFirstPod, hasAllOfReasons(Scheduled, Pulled, Created, Started));
        assertThat(eventsForFirstPod, hasNoneOfReasons(Failed, Unhealthy, FailedSync, FailedValidation));

        //Test that second pod does not have errors or failures in events
        List<Event> eventsForSecondPod = getEvents("Pod", newZkPodName[1]);
        assertThat(eventsForSecondPod, hasAllOfReasons(Scheduled, Pulled, Created, Started));
        assertThat(eventsForSecondPod, hasNoneOfReasons(Failed, Unhealthy, FailedSync, FailedValidation));

        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(stopwatch.runtime(SECONDS));

        // scale down
        LOGGER.info("Scaling down");
        replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec().getZookeeper().setReplicas(1);
        });
        kubeClient.waitForResourceDeletion("po", zookeeperPodName(CLUSTER_NAME,  1));
        // Wait for the one remaining node to enter standalone mode
        waitForZkMntr(Pattern.compile("zk_server_state\\s+standalone"), 0);

        //Test that the second pod has event 'Killing'
        assertThat(getEvents("Pod", newZkPodName[1]), hasAllOfReasons(Killing));
        //Test that stateful set has event 'SuccessfulDelete'
        assertThat(getEvents("StatefulSet", zookeeperClusterName(CLUSTER_NAME)), hasAllOfReasons(SuccessfulDelete));
        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(stopwatch.runtime(SECONDS));
    }

    @Test
    @JUnitGroup(name = "regression")
    @KafkaFromClasspathYaml()
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

        replaceKafkaResource(clusterName, k -> {
            Kafka kafka = k.getSpec().getKafka();
            kafka.getLivenessProbe().setInitialDelaySeconds(31);
            kafka.getReadinessProbe().setInitialDelaySeconds(31);
            kafka.getLivenessProbe().setTimeoutSeconds(11);
            kafka.getReadinessProbe().setTimeoutSeconds(11);
            kafka.setConfig(TestUtils.fromJson("{\"default.replication.factor\": 2,\"offsets.topic.replication.factor\": 2,\"transaction.state.log.replication.factor\": 2}", Map.class));
            Zookeeper zookeeper = k.getSpec().getZookeeper();
            zookeeper.getLivenessProbe().setInitialDelaySeconds(31);
            zookeeper.getReadinessProbe().setInitialDelaySeconds(31);
            zookeeper.getLivenessProbe().setTimeoutSeconds(11);
            zookeeper.getReadinessProbe().setTimeoutSeconds(11);
            zookeeper.setConfig(TestUtils.fromJson("{\"timeTick\": 2100, \"initLimit\": 6, \"syncLimit\": 3}", Map.class));
        });

        for (int i = 0; i < expectedZKPods; i++) {
            kubeClient.waitForResourceUpdate("pod", zookeeperPodName(clusterName, i), zkPodStartTime.get(i));
            kubeClient.waitForPod(zookeeperPodName(clusterName,  i));
        }
        for (int i = 0; i < expectedKafkaPods; i++) {
            kubeClient.waitForResourceUpdate("pod", kafkaPodName(clusterName, i), kafkaPodStartTime.get(i));
            kubeClient.waitForPod(kafkaPodName(clusterName,  i));
        }

        LOGGER.info("Verify values after update");
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
    @KafkaFromClasspathYaml()
    @Topic(name = TOPIC_NAME, clusterName = "my-cluster")
    public void testSendMessages() {
        int messagesCount = 20;
        sendMessages(CLUSTER_NAME, TOPIC_NAME, messagesCount, 1);
        String consumedMessages = consumeMessages(CLUSTER_NAME, TOPIC_NAME, 1, 30, 2);

        assertThat(consumedMessages, hasJsonPath("$[*].count", hasItem(messagesCount)));
        assertThat(consumedMessages, hasJsonPath("$[*].partitions[*].topic", hasItem(TOPIC_NAME)));

    }

    @KafkaFromClasspathYaml
    @Test
    @JUnitGroup(name = "regression")
    public void testJvmAndResources() {
        assertResources(NAMESPACE, "jvm-resource-cluster-kafka-0",
                "2Gi", "400m", "2Gi", "400m");
        assertExpectedJavaOpts("jvm-resource-cluster-kafka-0",
                "-Xmx1g", "-Xms1G", "-server", "-XX:+UseG1GC");

        assertResources(NAMESPACE, "jvm-resource-cluster-zookeeper-0",
                "1Gi", "300m", "1Gi", "300m");
        assertExpectedJavaOpts("jvm-resource-cluster-zookeeper-0",
                "-Xmx600m", "-Xms300m", "-server", "-XX:+UseG1GC");

        String podName = client.pods().inNamespace(NAMESPACE).list().getItems().stream().filter(p -> p.getMetadata().getName().startsWith("jvm-resource-cluster-topic-operator-")).findFirst().get().getMetadata().getName();

        assertResources(NAMESPACE, podName,
                "500M", "300m", "500M", "300m");
    }

    @Test
    @JUnitGroup(name = "regression")
    @KafkaFromClasspathYaml
    public void testForTopicOperator() {
        //Createing topics for testing
        kubeClient.create(TOPIC_CM);
        assertThat(listTopicsUsingPodCLI(CLUSTER_NAME, 0), hasItem("my-topic"));

        createTopicUsingPodCLI(CLUSTER_NAME, 0, "topic-from-cli", 1, 1);
        assertThat(listTopicsUsingPodCLI(CLUSTER_NAME, 0), hasItems("my-topic", "topic-from-cli"));
        assertThat(kubeClient.list("cm"), hasItems("my-topic", "topic-from-cli", "my-topic"));

        //Updating first topic using pod CLI
        updateTopicPartitionsCountUsingPodCLI(CLUSTER_NAME, 0, "my-topic", 2);
        assertThat(describeTopicUsingPodCLI(CLUSTER_NAME, 0, "my-topic"),
                hasItems("PartitionCount:2"));
        String testTopicCM = kubeClient.get("cm", "my-topic");
        assertThat(testTopicCM, valueOfCmEquals("partitions", "2"));

        //Updating second topic via CM update
        replaceCm("topic-from-cli", "partitions", "2");
        assertThat(describeTopicUsingPodCLI(CLUSTER_NAME, 0, "topic-from-cli"),
                hasItems("PartitionCount:2"));
        testTopicCM = kubeClient.get("cm", "topic-from-cli");
        assertThat(testTopicCM, valueOfCmEquals("partitions", "2"));

        //Deleting first topic by deletion of CM
        kubeClient.deleteByName("cm", "topic-from-cli");

        //Deleting another topic using pod CLI
        deleteTopicUsingPodCLI(CLUSTER_NAME, 0, "my-topic");
        kubeClient.waitForResourceDeletion("cm", "my-topic");
        List<String> topics = listTopicsUsingPodCLI(CLUSTER_NAME, 0);
        assertThat(topics, not(hasItems("topic-from-cli", "my-topic")));
    }

    private void testDockerImagesForKafkaCluster(String clusterName, int kafkaPods, int zkPods, boolean rackAwareEnabled) {
        LOGGER.info("Verifying docker image names");
        //Verifying docker image for cluster-operator

        Map<String, String> imgFromDeplConf = getImagesFromConfig(kubeClient.getResourceAsJson(
                "deployment", "strimzi-cluster-operator"));

        //Verifying docker image for zookeeper pods
        for (int i = 0; i < zkPods; i++) {
            String imgFromPod = getContainerImageNameFromPod(zookeeperPodName(clusterName, i), "zookeeper");
            assertEquals(imgFromDeplConf.get(ZK_IMAGE), imgFromPod);
            imgFromPod = getContainerImageNameFromPod(zookeeperPodName(clusterName, i), "tls-sidecar");
            assertEquals(imgFromDeplConf.get(TLS_SIDECAR_ZOOKEEPER_IMAGE), imgFromPod);
        }

        //Verifying docker image for kafka pods
        for (int i = 0; i < kafkaPods; i++) {
            String imgFromPod = getContainerImageNameFromPod(kafkaPodName(clusterName, i), "kafka");
            assertEquals(imgFromDeplConf.get(KAFKA_IMAGE), imgFromPod);
            imgFromPod = getContainerImageNameFromPod(kafkaPodName(clusterName, i), "tls-sidecar");
            assertEquals(imgFromDeplConf.get(TLS_SIDECAR_KAFKA_IMAGE), imgFromPod);
            if (rackAwareEnabled) {
                String initContainerImage = getInitContainerImageName(kafkaPodName(clusterName, i));
                assertEquals(imgFromDeplConf.get(KAFKA_INIT_IMAGE), initContainerImage);
            }
        }

        //Verifying docker image for topic-operator
        String topicOperatorPodName = kubeClient.listResourcesByLabel("pod",
                "strimzi.io/name=" + clusterName + "-topic-operator").get(0);
        String imgFromPod = getContainerImageNameFromPod(topicOperatorPodName, "topic-operator");
        assertEquals(imgFromDeplConf.get(TO_IMAGE), imgFromPod);
        imgFromPod = getContainerImageNameFromPod(topicOperatorPodName, "tls-sidecar");
        assertEquals(imgFromDeplConf.get(TLS_SIDECAR_TO_IMAGE), imgFromPod);

        LOGGER.info("Docker images verified");
    }

    @Test
    @JUnitGroup(name = "regression")
    @KafkaFromClasspathYaml
    public void testRackAware() {
        testDockerImagesForKafkaCluster(CLUSTER_NAME, 1, 1, true);

        String kafkaPodName = kafkaPodName(CLUSTER_NAME, 0);
        kubeClient.waitForPod(kafkaPodName);

        String rackId = kubeClient.exec(kafkaPodName, "/bin/bash", "-c", "cat /opt/kafka/rack/rack.id").out();
        assertEquals("zone", rackId);

        String brokerRack = kubeClient.exec(kafkaPodName, "/bin/bash", "-c", "cat /tmp/strimzi.properties | grep broker.rack").out();
        assertTrue(brokerRack.contains("broker.rack=zone"));

        List<Event> events = getEvents("Pod", kafkaPodName);
        assertThat(events, hasAllOfReasons(Scheduled, Pulled, Created, Started));
        assertThat(events, hasNoneOfReasons(Failed, Unhealthy, FailedSync, FailedValidation));
    }
}
