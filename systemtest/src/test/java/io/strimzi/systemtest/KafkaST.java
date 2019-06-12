/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.EntityOperatorSpec;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaClusterSpec;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.ZookeeperClusterSpec;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationScramSha512;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.listener.KafkaListenerPlain;
import io.strimzi.api.kafka.model.listener.KafkaListenerTls;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.JbodStorageBuilder;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageBuilder;
import io.strimzi.systemtest.annotations.OpenShiftOnly;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.Oc;
import io.strimzi.test.timemeasuring.Operation;
import io.strimzi.test.timemeasuring.TimeMeasuringSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.strimzi.api.kafka.model.KafkaResources.kafkaStatefulSetName;
import static io.strimzi.api.kafka.model.KafkaResources.zookeeperStatefulSetName;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.WAIT_FOR_ROLLING_UPDATE_TIMEOUT;
import static io.strimzi.systemtest.k8s.Events.Created;
import static io.strimzi.systemtest.k8s.Events.Killing;
import static io.strimzi.systemtest.k8s.Events.Pulled;
import static io.strimzi.systemtest.k8s.Events.Scheduled;
import static io.strimzi.systemtest.k8s.Events.Started;
import static io.strimzi.systemtest.k8s.Events.SuccessfulDelete;
import static io.strimzi.systemtest.matchers.Matchers.hasAllOfReasons;
import static io.strimzi.test.TestUtils.fromYamlString;
import static io.strimzi.test.TestUtils.map;
import static io.strimzi.test.TestUtils.waitFor;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Tag(REGRESSION)
class KafkaST extends MessagingBaseST {

    private static final Logger LOGGER = LogManager.getLogger(KafkaST.class);

    public static final String NAMESPACE = "kafka-cluster-test";
    private static final String TOPIC_NAME = "test-topic";
    private static final Pattern ZK_SERVER_STATE = Pattern.compile("zk_server_state\\s+(leader|follower)");

    @Test
    @Tag(REGRESSION)
    @OpenShiftOnly
    void testDeployKafkaClusterViaTemplate() {
        createCustomResources("../examples/templates/cluster-operator");
        Oc oc = (Oc) cmdKubeClient();
        String clusterName = "openshift-my-cluster";
        oc.newApp("strimzi-ephemeral", map("CLUSTER_NAME", clusterName));
        StUtils.waitForAllStatefulSetPodsReady(zookeeperClusterName(clusterName), 3);
        StUtils.waitForAllStatefulSetPodsReady(kafkaClusterName(clusterName), 3);
        StUtils.waitForDeploymentReady(entityOperatorDeploymentName(clusterName), 1);

        //Testing docker images
        testDockerImagesForKafkaCluster(clusterName, 3, 3, false);

        LOGGER.info("Deleting Kafka cluster {} after test", clusterName);
        oc.deleteByName("Kafka", clusterName);

        //Wait for kafka deletion
        oc.waitForResourceDeletion("Kafka", clusterName);
        kubeClient().listPods().stream()
            .filter(p -> p.getMetadata().getName().startsWith(clusterName))
            .forEach(p -> StUtils.waitForPodDeletion(p.getMetadata().getName()));

        StUtils.waitForStatefulSetDeletion(kafkaClusterName(clusterName));
        StUtils.waitForStatefulSetDeletion(zookeeperClusterName(clusterName));
        StUtils.waitForDeploymentDeletion(entityOperatorDeploymentName(clusterName));
        deleteCustomResources("../examples/templates/cluster-operator");
    }

    @Test
    void testKafkaAndZookeeperScaleUpScaleDown() throws Exception {
        operationID = startTimeMeasuring(Operation.SCALE_UP);
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewKafkaListenerExternalLoadBalancer()
                            .withTls(false)
                        .endKafkaListenerExternalLoadBalancer()
                    .endListeners()
                    .addToConfig(singletonMap("default.replication.factor", 3))
                    .addToConfig("auto.create.topics.enable", "false")
                .endKafka()
            .endSpec().done();

        testDockerImagesForKafkaCluster(CLUSTER_NAME, 3, 1, false);
        // kafka cluster already deployed
        LOGGER.info("Running kafkaScaleUpScaleDown {}", CLUSTER_NAME);

        final int initialReplicas = kubeClient().getStatefulSet(kafkaClusterName(CLUSTER_NAME)).getStatus().getReplicas();
        assertEquals(3, initialReplicas);
        // scale up
        final int scaleTo = initialReplicas + 1;
        final int newPodId = initialReplicas;
        final String newPodName = kafkaPodName(CLUSTER_NAME,  newPodId);
        LOGGER.info("Scaling up to {}", scaleTo);
        // Create snapshot of current cluster
        String kafkaSsName = kafkaStatefulSetName(CLUSTER_NAME);
        Map<String, String> kafkaPods = StUtils.ssSnapshot(kafkaSsName);
        replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getKafka().setReplicas(scaleTo));
        kafkaPods = StUtils.waitTillSsHasRolled(kafkaSsName, scaleTo, kafkaPods);

        String firstTopicName = "test-topic";
        testMethodResources().topic(CLUSTER_NAME, firstTopicName, scaleTo, scaleTo, NAMESPACE).done();

        //Test that the new pod does not have errors or failures in events
        String uid = kubeClient().getPodUid(newPodName);
        List<Event> events = getEvents(uid);
        assertThat(events, hasAllOfReasons(Scheduled, Pulled, Created, Started));
        waitForClusterAvailability(NAMESPACE, firstTopicName);
        //Test that CO doesn't have any exceptions in log
        TimeMeasuringSystem.stopOperation(operationID);
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, operationID));

        // scale down
        LOGGER.info("Scaling down");
        // Get kafka new pod uid before deletion
        uid = kubeClient().getPodUid(newPodName);
        operationID = startTimeMeasuring(Operation.SCALE_DOWN);
        replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getKafka().setReplicas(initialReplicas));
        StUtils.waitTillSsHasRolled(kafkaSsName, initialReplicas, kafkaPods);

        final int finalReplicas = kubeClient().getStatefulSet(kafkaClusterName(CLUSTER_NAME)).getStatus().getReplicas();
        assertEquals(initialReplicas, finalReplicas);

        //Test that the new broker has event 'Killing'
        assertThat(getEvents(uid), hasAllOfReasons(Killing));
        //Test that stateful set has event 'SuccessfulDelete'
        uid = kubeClient().getSsUid(kafkaClusterName(CLUSTER_NAME));
        assertThat(getEvents(uid), hasAllOfReasons(SuccessfulDelete));
        //Test that CO doesn't have any exceptions in log
        TimeMeasuringSystem.stopOperation(operationID);
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, operationID));

        String secondTopicName = "test-topic-2";
        testMethodResources().topic(CLUSTER_NAME, secondTopicName, finalReplicas, finalReplicas, NAMESPACE).done();
        waitForClusterAvailability(NAMESPACE, secondTopicName);
    }

    @Test
    void testEODeletion() {
        // Deploy kafka cluster with EO
        Kafka kafka = testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3).done();

        // Get pod name to check termination process
        Optional<Pod> pod = kubeClient().listPods()
                .stream().filter(p -> p.getMetadata().getName().startsWith(entityOperatorDeploymentName(CLUSTER_NAME)))
                .findFirst();

        assertTrue(pod.isPresent(), "EO pod does not exist");

        // Remove EO from Kafka DTO
        kafka.getSpec().setEntityOperator(null);
        // Replace Kafka configuration with removed EO
        testMethodResources.kafka(kafka).done();

        // Wait when EO(UO + TO) will be removed
        StUtils.waitForDeploymentDeletion(entityOperatorDeploymentName(CLUSTER_NAME));
        StUtils.waitForPodDeletion(pod.get().getMetadata().getName());
    }

    @Test
    void testZookeeperScaleUpScaleDown() {
        operationID = startTimeMeasuring(Operation.SCALE_UP);
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3).done();
        // kafka cluster already deployed
        LOGGER.info("Running zookeeperScaleUpScaleDown with cluster {}", CLUSTER_NAME);
        final int initialZkReplicas = kubeClient().getStatefulSet(zookeeperClusterName(CLUSTER_NAME)).getStatus().getReplicas();
        assertEquals(3, initialZkReplicas);

        final int scaleZkTo = initialZkReplicas + 4;
        final List<String> newZkPodNames = new ArrayList<String>() {{
                for (int i = initialZkReplicas; i < scaleZkTo; i++) {
                    add(zookeeperPodName(CLUSTER_NAME, i));
                }
            }};

        LOGGER.info("Scaling up to {}", scaleZkTo);
        replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getZookeeper().setReplicas(scaleZkTo));

        waitForZkPods(newZkPodNames);
        // check the new node is either in leader or follower state
        waitForZkMntr(ZK_SERVER_STATE, 0, 1, 2, 3, 4, 5, 6);
        checkZkPodsLog(newZkPodNames);

        //Test that CO doesn't have any exceptions in log
        TimeMeasuringSystem.stopOperation(operationID);
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, operationID));

        // scale down
        LOGGER.info("Scaling down");
        // Get zk-3 uid before deletion
        String uid = kubeClient().getPodUid(newZkPodNames.get(3));
        operationID = startTimeMeasuring(Operation.SCALE_DOWN);
        replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getZookeeper().setReplicas(initialZkReplicas));

        for (String name : newZkPodNames) {
            StUtils.waitForPodDeletion(name);
        }

        // Wait for one zk pods will became leader and others follower state
        waitForZkMntr(ZK_SERVER_STATE, 0, 1, 2);

        //Test that the second pod has event 'Killing'
        assertThat(getEvents(uid), hasAllOfReasons(Killing));
        //Test that stateful set has event 'SuccessfulDelete'
        uid = kubeClient().getStatefulUid(zookeeperClusterName(CLUSTER_NAME));
        assertThat(getEvents(uid), hasAllOfReasons(SuccessfulDelete));
        // Stop measuring
        TimeMeasuringSystem.stopOperation(operationID);
        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, operationID));
    }

    @Test
    @SuppressWarnings("checkstyle:MethodLength")
    void testCustomAndUpdatedValues() {
        Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put("offsets.topic.replication.factor", "1");
        kafkaConfig.put("transaction.state.log.replication.factor", "1");
        kafkaConfig.put("default.replication.factor", "1");

        Map<String, Object> zookeeperConfig = new HashMap<>();
        zookeeperConfig.put("tickTime", "2000");
        zookeeperConfig.put("initLimit", "5");
        zookeeperConfig.put("syncLimit", "2");
        zookeeperConfig.put("autopurge.purgeInterval", "1");
        int initialDelaySeconds = 30;
        int timeoutSeconds = 10;
        int updatedInitialDelaySeconds = 31;
        int updatedTimeoutSeconds = 11;

        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 2)
            .editSpec()
                .editKafka()
                    .withNewTlsSidecar()
                        .withNewReadinessProbe()
                            .withInitialDelaySeconds(initialDelaySeconds)
                            .withTimeoutSeconds(timeoutSeconds)
                        .endReadinessProbe()
                        .withNewLivenessProbe()
                            .withInitialDelaySeconds(initialDelaySeconds)
                            .withTimeoutSeconds(timeoutSeconds)
                        .endLivenessProbe()
                    .endTlsSidecar()
                    .withNewReadinessProbe()
                        .withInitialDelaySeconds(initialDelaySeconds)
                        .withTimeoutSeconds(timeoutSeconds)
                    .endReadinessProbe()
                    .withNewLivenessProbe()
                        .withInitialDelaySeconds(initialDelaySeconds)
                        .withTimeoutSeconds(timeoutSeconds)
                    .endLivenessProbe()
                    .withConfig(kafkaConfig)
                .endKafka()
                .editZookeeper()
                    .withNewTlsSidecar()
                        .withNewReadinessProbe()
                            .withInitialDelaySeconds(initialDelaySeconds)
                            .withTimeoutSeconds(timeoutSeconds)
                        .endReadinessProbe()
                        .withNewLivenessProbe()
                            .withInitialDelaySeconds(initialDelaySeconds)
                            .withTimeoutSeconds(timeoutSeconds)
                        .endLivenessProbe()
                    .endTlsSidecar()
                .withReplicas(2)
                    .withNewReadinessProbe()
                       .withInitialDelaySeconds(initialDelaySeconds)
                        .withTimeoutSeconds(timeoutSeconds)
                    .endReadinessProbe()
                        .withNewLivenessProbe()
                        .withInitialDelaySeconds(initialDelaySeconds)
                        .withTimeoutSeconds(timeoutSeconds)
                    .endLivenessProbe()
                    .withConfig(zookeeperConfig)
                .endZookeeper()
                .editEntityOperator()
                    .editUserOperator()
                        .withNewReadinessProbe()
                            .withInitialDelaySeconds(initialDelaySeconds)
                            .withTimeoutSeconds(timeoutSeconds)
                        .endReadinessProbe()
                            .withNewLivenessProbe()
                            .withInitialDelaySeconds(initialDelaySeconds)
                            .withTimeoutSeconds(timeoutSeconds)
                        .endLivenessProbe()
                    .endUserOperator()
                    .editTopicOperator()
                        .withNewReadinessProbe()
                            .withInitialDelaySeconds(initialDelaySeconds)
                            .withTimeoutSeconds(timeoutSeconds)
                        .endReadinessProbe()
                        .withNewLivenessProbe()
                            .withInitialDelaySeconds(initialDelaySeconds)
                            .withTimeoutSeconds(timeoutSeconds)
                        .endLivenessProbe()
                    .endTopicOperator()
                    .withNewTlsSidecar()
                        .withNewReadinessProbe()
                            .withInitialDelaySeconds(initialDelaySeconds)
                            .withTimeoutSeconds(timeoutSeconds)
                        .endReadinessProbe()
                        .withNewLivenessProbe()
                            .withInitialDelaySeconds(initialDelaySeconds)
                            .withTimeoutSeconds(timeoutSeconds)
                        .endLivenessProbe()
                    .endTlsSidecar()
                .endEntityOperator()
                .endSpec()
            .done();

        Map<String, String> kafkaSnapshot = StUtils.ssSnapshot(kafkaClusterName(CLUSTER_NAME));
        Map<String, String> zkSnapshot = StUtils.ssSnapshot(zookeeperClusterName(CLUSTER_NAME));
        Map<String, String> eoPod = StUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME));

        LOGGER.info("Verify values before update");
        checkReadinessLivenessProbe(kafkaStatefulSetName(CLUSTER_NAME), "kafka", initialDelaySeconds, timeoutSeconds);
        checkContainerConfiguration(kafkaStatefulSetName(CLUSTER_NAME), "kafka", "KAFKA_CONFIGURATION",
                "default.replication.factor=1\noffsets.topic.replication.factor=1\ntransaction.state.log.replication.factor=1\n");
        checkReadinessLivenessProbe(kafkaStatefulSetName(CLUSTER_NAME), "tls-sidecar", initialDelaySeconds, timeoutSeconds);

        LOGGER.info("Testing Zookeepers");
        checkReadinessLivenessProbe(zookeeperStatefulSetName(CLUSTER_NAME), "zookeeper", initialDelaySeconds, timeoutSeconds);
        checkContainerConfiguration(zookeeperStatefulSetName(CLUSTER_NAME), "zookeeper", "ZOOKEEPER_CONFIGURATION",
                "autopurge.purgeInterval=1\ntickTime=2000\ninitLimit=5\nsyncLimit=2\n");
        checkReadinessLivenessProbe(zookeeperStatefulSetName(CLUSTER_NAME), "tls-sidecar", initialDelaySeconds, timeoutSeconds);

        LOGGER.info("Checking configuration of TO and UO");
        checkReadinessLivenessProbe(entityOperatorDeploymentName(CLUSTER_NAME), "topic-operator", initialDelaySeconds, timeoutSeconds);
        checkReadinessLivenessProbe(entityOperatorDeploymentName(CLUSTER_NAME), "user-operator", initialDelaySeconds, timeoutSeconds);
        checkReadinessLivenessProbe(entityOperatorDeploymentName(CLUSTER_NAME), "tls-sidecar", initialDelaySeconds, timeoutSeconds);

        LOGGER.info("Updating configuration of Kafka cluster");
        replaceKafkaResource(CLUSTER_NAME, k -> {
            KafkaClusterSpec kafkaClusterSpec = k.getSpec().getKafka();
            kafkaClusterSpec.getLivenessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            kafkaClusterSpec.getReadinessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            kafkaClusterSpec.getLivenessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            kafkaClusterSpec.getReadinessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            kafkaClusterSpec.setConfig(TestUtils.fromJson("{\"default.replication.factor\": 2,\"offsets.topic.replication.factor\": 2,\"transaction.state.log.replication.factor\": 2}", Map.class));
            kafkaClusterSpec.getTlsSidecar().getLivenessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            kafkaClusterSpec.getTlsSidecar().getReadinessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            kafkaClusterSpec.getTlsSidecar().getLivenessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            kafkaClusterSpec.getTlsSidecar().getReadinessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            ZookeeperClusterSpec zookeeperClusterSpec = k.getSpec().getZookeeper();
            zookeeperClusterSpec.getLivenessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            zookeeperClusterSpec.getReadinessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            zookeeperClusterSpec.getLivenessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            zookeeperClusterSpec.getReadinessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            zookeeperClusterSpec.setConfig(TestUtils.fromJson("{\"tickTime\": 2100, \"initLimit\": 6, \"syncLimit\": 3}", Map.class));
            zookeeperClusterSpec.getTlsSidecar().getLivenessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            zookeeperClusterSpec.getTlsSidecar().getReadinessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            zookeeperClusterSpec.getTlsSidecar().getLivenessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            zookeeperClusterSpec.getTlsSidecar().getReadinessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            // Configuring TO and UO to use new values for InitialDelaySeconds and TimeoutSeconds
            EntityOperatorSpec entityOperatorSpec = k.getSpec().getEntityOperator();
            entityOperatorSpec.getTopicOperator().getLivenessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            entityOperatorSpec.getTopicOperator().getReadinessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            entityOperatorSpec.getTopicOperator().getLivenessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            entityOperatorSpec.getTopicOperator().getReadinessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            entityOperatorSpec.getUserOperator().getLivenessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            entityOperatorSpec.getUserOperator().getReadinessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            entityOperatorSpec.getUserOperator().getLivenessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            entityOperatorSpec.getUserOperator().getReadinessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            entityOperatorSpec.getTlsSidecar().getLivenessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            entityOperatorSpec.getTlsSidecar().getReadinessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            entityOperatorSpec.getTlsSidecar().getLivenessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            entityOperatorSpec.getTlsSidecar().getReadinessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
        });

        StUtils.waitTillSsHasRolled(kafkaClusterName(CLUSTER_NAME), 2, kafkaSnapshot);
        StUtils.waitTillSsHasRolled(zookeeperClusterName(CLUSTER_NAME), 2, zkSnapshot);
        StUtils.waitTillDepHasRolled(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), 1, eoPod);

        LOGGER.info("Verify values after update");
        checkReadinessLivenessProbe(kafkaStatefulSetName(CLUSTER_NAME), "kafka", updatedInitialDelaySeconds, updatedTimeoutSeconds);
        checkContainerConfiguration(kafkaStatefulSetName(CLUSTER_NAME), "kafka", "KAFKA_CONFIGURATION",
                "default.replication.factor=2\noffsets.topic.replication.factor=2\ntransaction.state.log.replication.factor=2\n");
        checkReadinessLivenessProbe(kafkaStatefulSetName(CLUSTER_NAME), "tls-sidecar", updatedInitialDelaySeconds, updatedTimeoutSeconds);

        LOGGER.info("Testing Zookeepers");
        checkReadinessLivenessProbe(zookeeperStatefulSetName(CLUSTER_NAME), "zookeeper", updatedInitialDelaySeconds, updatedTimeoutSeconds);
        checkContainerConfiguration(zookeeperStatefulSetName(CLUSTER_NAME), "zookeeper", "ZOOKEEPER_CONFIGURATION",
                "autopurge.purgeInterval=1\ntickTime=2100\ninitLimit=6\nsyncLimit=3\n");
        checkReadinessLivenessProbe(zookeeperStatefulSetName(CLUSTER_NAME), "tls-sidecar", updatedInitialDelaySeconds, updatedTimeoutSeconds);

        LOGGER.info("Getting entity operator to check configuration of TO and UO");
        checkReadinessLivenessProbe(entityOperatorDeploymentName(CLUSTER_NAME), "topic-operator", updatedInitialDelaySeconds, updatedTimeoutSeconds);
        checkReadinessLivenessProbe(entityOperatorDeploymentName(CLUSTER_NAME), "user-operator", updatedInitialDelaySeconds, updatedTimeoutSeconds);
        checkReadinessLivenessProbe(entityOperatorDeploymentName(CLUSTER_NAME), "tls-sidecar", updatedInitialDelaySeconds, updatedTimeoutSeconds);
    }

    /**
     * Verifies readinessProbe and livenessProbe properties in expected container
     * @param podNamePrefix Prexif of pod name where container is located
     * @param containerName The container where verifying is expected
     * @param initialDelaySeconds expected value for property initialDelaySeconds
     * @param timeoutSeconds expected value for property timeoutSeconds
     */
    void checkReadinessLivenessProbe(String podNamePrefix, String containerName, int initialDelaySeconds, int timeoutSeconds) {
        LOGGER.info("Getting pods by prefix {} in pod name", podNamePrefix);
        List<Pod> pods = kubeClient().listPodsByPrefixInName(podNamePrefix);

        if (pods.size() != 0) {
            LOGGER.info("Testing configuration for container {}", containerName);
            pods.forEach(pod -> {
                pod.getSpec().getContainers().stream().filter(c -> c.getName().equals(containerName))
                    .forEach(container -> {
                        assertEquals(initialDelaySeconds, container.getLivenessProbe().getInitialDelaySeconds());
                        assertEquals(initialDelaySeconds, container.getReadinessProbe().getInitialDelaySeconds());
                        assertEquals(timeoutSeconds, container.getLivenessProbe().getTimeoutSeconds());
                        assertEquals(timeoutSeconds, container.getReadinessProbe().getTimeoutSeconds());
                    });
            });
        } else {
            fail("Pod with prefix " + podNamePrefix + " in name, not found");
        }
    }

    /**
     * Verifies container configuration by environment key
     * @param podNamePrefix Name of pod where container is located
     * @param containerName The container where verifying is expected
     * @param configKey Expected configuration key
     * @param config Expected configuration
     */
    void checkContainerConfiguration(String podNamePrefix, String containerName, String configKey, String config) {
        LOGGER.info("Getting pods by prefix in name {}", podNamePrefix);
        List<Pod> pods = kubeClient().listPodsByPrefixInName(podNamePrefix);

        if (pods.size() != 0) {
            LOGGER.info("Testing configuration for container {}", containerName);
            pods.forEach(pod -> {
                pod.getSpec().getContainers().stream().filter(c -> c.getName().equals(containerName))
                    .forEach(container -> {
                        container.getEnv().stream().filter(
                            envVar -> envVar.getName().equals(configKey))
                                .forEach(envVar -> assertEquals(config, envVar.getValue()));
                    });
            });
        } else {
            fail("Pod with prefix " + podNamePrefix + " in name, not found");
        }
    }

    /**
     * Test sending messages over plain transport, without auth
     */
    @Test
    void testSendMessagesPlainAnonymous() throws Exception {
        int messagesCount = 200;
        String topicName = TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);

        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3).done();
        testMethodResources().topic(CLUSTER_NAME, topicName, NAMESPACE).done();

        testMethodResources().deployKafkaClients(CLUSTER_NAME).done();

        availabilityTest(messagesCount, Constants.TIMEOUT_AVAILABILITY_TEST, CLUSTER_NAME, false, topicName, null);
    }

    /**
     * Test sending messages over tls transport using mutual tls auth
     */
    @Test
    void testSendMessagesTlsAuthenticated() throws Exception {
        String kafkaUser = "my-user";
        int messagesCount = 200;
        String topicName = TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);

        KafkaListenerAuthenticationTls auth = new KafkaListenerAuthenticationTls();
        KafkaListenerTls listenerTls = new KafkaListenerTls();
        listenerTls.setAuth(auth);

        // Use a Kafka with plain listener disabled
        testMethodResources().kafka(testMethodResources().defaultKafka(CLUSTER_NAME, 3)
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withTls(listenerTls)
                            .withNewTls()
                            .endTls()
                        .endListeners()
                    .endKafka()
                .endSpec().build()).done();
        testMethodResources().topic(CLUSTER_NAME, topicName, NAMESPACE).done();
        KafkaUser user = testMethodResources().tlsUser(CLUSTER_NAME, kafkaUser).done();
        waitTillSecretExists(kafkaUser);

        testMethodResources().deployKafkaClients(true, CLUSTER_NAME, user).done();
        availabilityTest(messagesCount, Constants.TIMEOUT_AVAILABILITY_TEST, CLUSTER_NAME, true, topicName, user);
    }

    /**
     * Test sending messages over plain transport using scram sha auth
     */
    @Test
    void testSendMessagesPlainScramSha() throws Exception {
        String kafkaUser = "my-user";
        int messagesCount = 200;
        String topicName = TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);

        KafkaListenerAuthenticationScramSha512 auth = new KafkaListenerAuthenticationScramSha512();
        KafkaListenerPlain listenerTls = new KafkaListenerPlain();
        listenerTls.setAuthentication(auth);

        // Use a Kafka with plain listener disabled
        testMethodResources().kafka(testMethodResources().defaultKafka(CLUSTER_NAME, 1)
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withPlain(listenerTls)
                        .endListeners()
                    .endKafka()
                .endSpec().build()).done();
        testMethodResources().topic(CLUSTER_NAME, topicName, NAMESPACE).done();
        KafkaUser user = testMethodResources().scramShaUser(CLUSTER_NAME, kafkaUser).done();
        waitTillSecretExists(kafkaUser);
        String brokerPodLog = kubeClient().logs(CLUSTER_NAME + "-kafka-0", "kafka");
        Pattern p = Pattern.compile("^.*" + Pattern.quote(kafkaUser) + ".*$", Pattern.MULTILINE);
        Matcher m = p.matcher(brokerPodLog);
        boolean found = false;
        while (m.find()) {
            found = true;
            LOGGER.info("Broker pod log line about user {}: {}", kafkaUser, m.group());
        }
        if (!found) {
            LOGGER.warn("No broker pod log lines about user {}", kafkaUser);
            LOGGER.info("Broker pod log:\n----\n{}\n----\n", brokerPodLog);
        }

        testMethodResources().deployKafkaClients(false, CLUSTER_NAME, user).done();
        availabilityTest(messagesCount, Constants.TIMEOUT_AVAILABILITY_TEST, CLUSTER_NAME, false, topicName, user);
    }

    /**
     * Test sending messages over tls transport using scram sha auth
     */
    @Test
    void testSendMessagesTlsScramSha() throws Exception {
        String kafkaUser = "my-user";
        int messagesCount = 200;
        String topicName = TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);

        KafkaListenerTls listenerTls = new KafkaListenerTls();
        listenerTls.setAuth(new KafkaListenerAuthenticationScramSha512());

        // Use a Kafka with plain listener disabled
        testMethodResources().kafka(testMethodResources().defaultKafka(CLUSTER_NAME, 3)
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withNewTls().withAuth(new KafkaListenerAuthenticationScramSha512()).endTls()
                        .endListeners()
                    .endKafka()
                .endSpec().build()).done();
        testMethodResources().topic(CLUSTER_NAME, topicName, NAMESPACE).done();
        KafkaUser user = testMethodResources().scramShaUser(CLUSTER_NAME, kafkaUser).done();
        waitTillSecretExists(kafkaUser);

        testMethodResources().deployKafkaClients(true, CLUSTER_NAME, user).done();
        availabilityTest(messagesCount, 180000, CLUSTER_NAME, true, topicName, user);
    }

    @Test
    void testJvmAndResources() {
        Map<String, String> jvmOptionsXX = new HashMap<>();
        jvmOptionsXX.put("UseG1GC", "true");

        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 1)
            .editSpec()
                .editKafka()
                    .withResources(new ResourceRequirementsBuilder()
                            .addToLimits("memory", new Quantity("1.5Gi"))
                            .addToLimits("cpu", new Quantity("1"))
                            .addToRequests("memory", new Quantity("1Gi"))
                            .addToRequests("cpu", new Quantity("500m"))
                            .build())
                    .withNewJvmOptions()
                        .withXmx("1g")
                        .withXms("512m")
                        .withServer(true)
                        .withXx(jvmOptionsXX)
                    .endJvmOptions()
                .endKafka()
                .editZookeeper()
                    .withResources(new ResourceRequirementsBuilder()
                            .addToLimits("memory", new Quantity("1G"))
                            .addToLimits("cpu", new Quantity("0.5"))
                            .addToRequests("memory", new Quantity("0.5G"))
                            .addToRequests("cpu", new Quantity("250m"))
                            .build())
                    .withNewJvmOptions()
                        .withXmx("1G")
                        .withXms("512M")
                        .withServer(true)
                        .withXx(jvmOptionsXX)
                    .endJvmOptions()
                .endZookeeper()
                .withNewEntityOperator()
                    .withNewTopicOperator()
                        .withResources(new ResourceRequirementsBuilder()
                                .addToLimits("memory", new Quantity("1024Mi"))
                                .addToLimits("cpu", new Quantity("500m"))
                                .addToRequests("memory", new Quantity("512Mi"))
                                .addToRequests("cpu", new Quantity("0.25"))
                                .build())
                    .endTopicOperator()
                    .withNewUserOperator()
                        .withResources(new ResourceRequirementsBuilder()
                                .addToLimits("memory", new Quantity("512M"))
                                .addToLimits("cpu", new Quantity("300m"))
                                .addToRequests("memory", new Quantity("256M"))
                                .addToRequests("cpu", new Quantity("300m"))
                                .build())
                    .endUserOperator()
                .endEntityOperator()
            .endSpec().done();

        operationID = startTimeMeasuring(Operation.NEXT_RECONCILIATION);

        // Make snapshots for Kafka cluster to meke sure that there is no rolling update after CO reconciliation
        String zkSsName = KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME);
        String kafkaSsName = kafkaStatefulSetName(CLUSTER_NAME);
        String eoDepName = KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME);
        Map<String, String> zkPods = StUtils.ssSnapshot(zkSsName);
        Map<String, String> kafkaPods = StUtils.ssSnapshot(kafkaSsName);
        Map<String, String> eoPods = StUtils.depSnapshot(eoDepName);

        assertResources(cmdKubeClient().namespace(), kafkaPodName(CLUSTER_NAME, 0), "kafka",
                "1536Mi", "1", "1Gi", "500m");
        assertExpectedJavaOpts(kafkaPodName(CLUSTER_NAME, 0), "kafka",
                "-Xmx1g", "-Xms512m", "-server", "-XX:+UseG1GC");

        assertResources(cmdKubeClient().namespace(), zookeeperPodName(CLUSTER_NAME, 0), "zookeeper",
                "1G", "500m", "500M", "250m");
        assertExpectedJavaOpts(zookeeperPodName(CLUSTER_NAME, 0), "zookeeper",
                "-Xmx1G", "-Xms512M", "-server", "-XX:+UseG1GC");

        Optional<Pod> pod = kubeClient().listPods()
                .stream().filter(p -> p.getMetadata().getName().startsWith(entityOperatorDeploymentName(CLUSTER_NAME)))
                .findFirst();
        assertTrue(pod.isPresent(), "EO pod does not exist");

        assertResources(cmdKubeClient().namespace(), pod.get().getMetadata().getName(), "topic-operator",
                "1Gi", "500m", "512Mi", "250m");
        assertResources(cmdKubeClient().namespace(), pod.get().getMetadata().getName(), "user-operator",
                "512M", "300m", "256M", "300m");

        TestUtils.waitFor("Wait till reconciliation timeout", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> !cmdKubeClient().searchInLog("deploy", "strimzi-cluster-operator", TimeMeasuringSystem.getCurrentDuration(testClass, testName, operationID), "\"Assembly reconciled\"").isEmpty());

        // Checking no rolling update after last CO reconciliation
        LOGGER.info("Checking no rolling update for Kafka cluster");
        assertFalse(StUtils.ssHasRolled(zkSsName, zkPods));
        assertFalse(StUtils.ssHasRolled(kafkaSsName, kafkaPods));
        assertFalse(StUtils.depHasRolled(eoDepName, eoPods));
        TimeMeasuringSystem.stopOperation(operationID);
    }

    @Test
    void testForTopicOperator() throws InterruptedException {
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3).done();

        //Creating topics for testing
        cmdKubeClient().create(TOPIC_CM);
        TestUtils.waitFor("wait for 'my-topic' to be created in Kafka", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_TOPIC_CREATION, () -> {
            List<String> topics = listTopicsUsingPodCLI(CLUSTER_NAME, 0);
            return topics.contains("my-topic");
        });

        assertThat(listTopicsUsingPodCLI(CLUSTER_NAME, 0), hasItem("my-topic"));

        createTopicUsingPodCLI(CLUSTER_NAME, 0, "topic-from-cli", 1, 1);
        assertThat(listTopicsUsingPodCLI(CLUSTER_NAME, 0), hasItems("my-topic", "topic-from-cli"));
        assertThat(cmdKubeClient().list("kafkatopic"), hasItems("my-topic", "topic-from-cli", "my-topic"));

        //Updating first topic using pod CLI
        updateTopicPartitionsCountUsingPodCLI(CLUSTER_NAME, 0, "my-topic", 2);
        assertThat(describeTopicUsingPodCLI(CLUSTER_NAME, 0, "my-topic"),
                hasItems("PartitionCount:2"));
        KafkaTopic testTopic = fromYamlString(cmdKubeClient().get("kafkatopic", "my-topic"), KafkaTopic.class);
        assertNotNull(testTopic);
        assertNotNull(testTopic.getSpec());
        assertEquals(Integer.valueOf(2), testTopic.getSpec().getPartitions());

        //Updating second topic via KafkaTopic update
        replaceTopicResource("topic-from-cli", topic -> {
            topic.getSpec().setPartitions(2);
        });
        assertThat(describeTopicUsingPodCLI(CLUSTER_NAME, 0, "topic-from-cli"),
                hasItems("PartitionCount:2"));
        testTopic = fromYamlString(cmdKubeClient().get("kafkatopic", "topic-from-cli"), KafkaTopic.class);
        assertNotNull(testTopic);
        assertNotNull(testTopic.getSpec());
        assertEquals(Integer.valueOf(2), testTopic.getSpec().getPartitions());

        //Deleting first topic by deletion of CM
        cmdKubeClient().deleteByName("kafkatopic", "topic-from-cli");

        //Deleting another topic using pod CLI
        deleteTopicUsingPodCLI(CLUSTER_NAME, 0, "my-topic");
        StUtils.waitForKafkaTopicDeletion("my-topic");

        //Checking all topics were deleted
        Thread.sleep(Constants.TIMEOUT_TEARDOWN);
        List<String> topics = listTopicsUsingPodCLI(CLUSTER_NAME, 0);
        assertThat(topics, not(hasItems("my-topic")));
        assertThat(topics, not(hasItems("topic-from-cli")));
    }

    @Test
    void testTopicWithoutLabels() {
        // Negative scenario: creating topic without any labels and make sure that TO can't handle this topic
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3).done();

        // Creating topic without any label
        testMethodResources().topic(CLUSTER_NAME, "topic-without-labels", NAMESPACE)
            .editMetadata()
                .withLabels(null)
            .endMetadata()
            .done();

        // Checking that resource was created
        assertThat(cmdKubeClient().list("kafkatopic"), hasItems("topic-without-labels"));
        // Checking that TO didn't handl new topic and zk pods don't contain new topic
        assertThat(listTopicsUsingPodCLI(CLUSTER_NAME, 0), not(hasItems("topic-without-labels")));

        // Checking TO logs
        String tOPodName = cmdKubeClient().listResourcesByLabel("pod", "strimzi.io/name=my-cluster-entity-operator").get(0);
        String tOlogs = kubeClient().logs(tOPodName, "topic-operator");
        assertThat(tOlogs, not(containsString("Created topic 'topic-without-labels'")));

        //Deleting topic
        cmdKubeClient().deleteByName("kafkatopic", "topic-without-labels");
        StUtils.waitForKafkaTopicDeletion("topic-without-labels");

        //Checking all topics were deleted
        List<String> topics = listTopicsUsingPodCLI(CLUSTER_NAME, 0);
        assertThat(topics, not(hasItems("topic-without-labels")));
    }

    private void testDockerImagesForKafkaCluster(String clusterName, int kafkaPods, int zkPods, boolean rackAwareEnabled) {
        LOGGER.info("Verifying docker image names");
        //Verifying docker image for cluster-operator

        Map<String, String> imgFromDeplConf = getImagesFromConfig();

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
            String kafkaVersion = Crds.kafkaOperation(kubeClient().getClient()).inNamespace(NAMESPACE).withName(clusterName).get().getSpec().getKafka().getVersion();
            if (kafkaVersion == null) {
                kafkaVersion = Environment.ST_KAFKA_VERSION;
            }
            assertEquals(TestUtils.parseImageMap(imgFromDeplConf.get(KAFKA_IMAGE_MAP)).get(kafkaVersion), imgFromPod);
            imgFromPod = getContainerImageNameFromPod(kafkaPodName(clusterName, i), "tls-sidecar");
            assertEquals(imgFromDeplConf.get(TLS_SIDECAR_KAFKA_IMAGE), imgFromPod);
            if (rackAwareEnabled) {
                String initContainerImage = getInitContainerImageName(kafkaPodName(clusterName, i));
                assertEquals(imgFromDeplConf.get(KAFKA_INIT_IMAGE), initContainerImage);
            }
        }

        //Verifying docker image for entity-operator
        String entityOperatorPodName = cmdKubeClient().listResourcesByLabel("pod",
                "strimzi.io/name=" + clusterName + "-entity-operator").get(0);
        String imgFromPod = getContainerImageNameFromPod(entityOperatorPodName, "topic-operator");
        assertEquals(imgFromDeplConf.get(TO_IMAGE), imgFromPod);
        imgFromPod = getContainerImageNameFromPod(entityOperatorPodName, "user-operator");
        assertEquals(imgFromDeplConf.get(UO_IMAGE), imgFromPod);
        imgFromPod = getContainerImageNameFromPod(entityOperatorPodName, "tls-sidecar");
        assertEquals(imgFromDeplConf.get(TLS_SIDECAR_EO_IMAGE), imgFromPod);

        LOGGER.info("Docker images verified");
    }

    @Test
    void testRackAware() throws Exception {
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 1, 1)
            .editSpec()
                .editKafka()
                    .withNewRack()
                        .withTopologyKey("rack-key")
                    .endRack()
                    .editListeners()
                        .withNewKafkaListenerExternalLoadBalancer()
                            .withTls(false)
                        .endKafkaListenerExternalLoadBalancer()
                    .endListeners()
                .endKafka()
            .endSpec().done();

        testDockerImagesForKafkaCluster(CLUSTER_NAME, 1, 1, true);

        String kafkaPodName = kafkaPodName(CLUSTER_NAME, 0);
        StUtils.waitForPod(kafkaPodName);

        String rackId = cmdKubeClient().execInPod(kafkaPodName, "/bin/bash", "-c", "cat /opt/kafka/init/rack.id").out();
        assertEquals("zone", rackId.trim());

        String brokerRack = cmdKubeClient().execInPod(kafkaPodName, "/bin/bash", "-c", "cat /tmp/strimzi.properties | grep broker.rack").out();
        assertTrue(brokerRack.contains("broker.rack=zone"));

        String uid = kubeClient().getPodUid(kafkaPodName);
        List<Event> events = getEvents(uid);
        assertThat(events, hasAllOfReasons(Scheduled, Pulled, Created, Started));
        waitForClusterAvailability(NAMESPACE);
    }

    @Test
    void testManualTriggeringRollingUpdate() {
        String coPodName = kubeClient().listPods("name", "strimzi-cluster-operator").get(0).getMetadata().getName();
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 1).done();

        // rolling update for kafka
        operationID = startTimeMeasuring(Operation.ROLLING_UPDATE);
        // set annotation to trigger Kafka rolling update
        kubeClient().statefulSet(kafkaClusterName(CLUSTER_NAME)).cascading(false).edit()
                .editMetadata()
                    .addToAnnotations("strimzi.io/manual-rolling-update", "true")
                .endMetadata().done();

        // check annotation to trigger rolling update
        assertTrue(Boolean.parseBoolean(kubeClient().getStatefulSet(kafkaClusterName(CLUSTER_NAME))
                .getMetadata().getAnnotations().get("strimzi.io/manual-rolling-update")));

        // wait when annotation will be removed
        waitFor("CO removes rolling update annotation", Constants.WAIT_FOR_ROLLING_UPDATE_INTERVAL, WAIT_FOR_ROLLING_UPDATE_TIMEOUT,
            () -> getAnnotationsForSS(kafkaClusterName(CLUSTER_NAME)) == null
                || !getAnnotationsForSS(kafkaClusterName(CLUSTER_NAME)).containsKey("strimzi.io/manual-rolling-update"));

        // check rolling update messages in CO log
        String coLog = kubeClient().logs(coPodName);
        assertThat(coLog, containsString("Rolling Kafka pod " + kafkaClusterName(CLUSTER_NAME) + "-0" + " due to manual rolling update"));

        // rolling update for zookeeper
        operationID = startTimeMeasuring(Operation.ROLLING_UPDATE);
        // set annotation to trigger Zookeeper rolling update
        kubeClient().statefulSet(zookeeperClusterName(CLUSTER_NAME)).cascading(false).edit()
                .editMetadata()
                    .addToAnnotations("strimzi.io/manual-rolling-update", "true")
                .endMetadata().done();

        // check annotation to trigger rolling update
        assertTrue(Boolean.parseBoolean(kubeClient().getStatefulSet(zookeeperClusterName(CLUSTER_NAME))
                .getMetadata().getAnnotations().get("strimzi.io/manual-rolling-update")));

        // wait when annotation will be removed
        waitFor("CO removes rolling update annotation", Constants.WAIT_FOR_ROLLING_UPDATE_INTERVAL, WAIT_FOR_ROLLING_UPDATE_TIMEOUT,
            () -> getAnnotationsForSS(zookeeperClusterName(CLUSTER_NAME)) == null
                || !getAnnotationsForSS(zookeeperClusterName(CLUSTER_NAME)).containsKey("strimzi.io/manual-rolling-update"));

        // check rolling update messages in CO log
        coLog = kubeClient().logs(coPodName);
        assertThat(coLog, containsString("Rolling Zookeeper pod " + zookeeperClusterName(CLUSTER_NAME) + "-0" + " to manual rolling update"));
    }

    @Test
    void testNodePort() throws Exception {
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3, 1)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewKafkaListenerExternalNodePort()
                            .withTls(false)
                        .endKafkaListenerExternalNodePort()
                    .endListeners()
                    .withConfig(singletonMap("default.replication.factor", 3))
                .endKafka()
            .endSpec()
            .done();

        waitForClusterAvailability(NAMESPACE);
    }

    @Test
    void testNodePortTls() throws Exception {
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3, 1)
            .editSpec()
                .editKafka()
                    .editListeners()
                    .withNewKafkaListenerExternalNodePort()
                    .endKafkaListenerExternalNodePort()
                    .endListeners()
                    .withConfig(singletonMap("default.replication.factor", 3))
                .endKafka()
            .endSpec()
            .done();

        String userName = "alice";
        testMethodResources().tlsUser(CLUSTER_NAME, userName).done();
        waitFor("Wait for secrets became available", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_GET_SECRETS,
            () -> kubeClient().getSecret("alice") != null,
            () -> LOGGER.error("Couldn't find user secret {}", kubeClient().listSecrets()));

        waitForClusterAvailabilityTls(userName, NAMESPACE);
    }

    @Test
    void testLoadBalancer() throws Exception {
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewKafkaListenerExternalLoadBalancer()
                            .withTls(false)
                        .endKafkaListenerExternalLoadBalancer()
                    .endListeners()
                    .withConfig(singletonMap("default.replication.factor", 3))
                .endKafka()
            .endSpec()
            .done();

        waitForClusterAvailability(NAMESPACE);
    }

    @Test
    void testLoadBalancerTls() throws Exception {
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewKafkaListenerExternalLoadBalancer()
                        .endKafkaListenerExternalLoadBalancer()
                    .endListeners()
                    .withConfig(singletonMap("default.replication.factor", 3))
                .endKafka()
            .endSpec()
            .done();

        String userName = "alice";
        testMethodResources().tlsUser(CLUSTER_NAME, userName).done();
        waitFor("Wait for secrets became available", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_GET_SECRETS,
            () -> kubeClient().getSecret("alice") != null,
            () -> LOGGER.error("Couldn't find user secret {}", kubeClient().listSecrets()));

        waitForClusterAvailabilityTls(userName, NAMESPACE);
    }

    private Map<String, String> getAnnotationsForSS(String ssName) {
        return kubeClient().getStatefulSet(ssName).getMetadata().getAnnotations();
    }

    void waitForZkRollUp() {
        LOGGER.info("Waiting for cluster stability");
        Map<String, String>[] zkPods = new Map[1];
        AtomicInteger count = new AtomicInteger();
        zkPods[0] = StUtils.ssSnapshot(zookeeperStatefulSetName(CLUSTER_NAME));
        TestUtils.waitFor("Cluster stable and ready", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_ZK_CLUSTER_STABILIZATION, () -> {
            Map<String, String> zkSnapshot = StUtils.ssSnapshot(zookeeperStatefulSetName(CLUSTER_NAME));
            boolean zkSameAsLast = zkSnapshot.equals(zkPods[0]);
            if (!zkSameAsLast) {
                LOGGER.info("ZK Cluster not stable");
            }
            if (zkSameAsLast) {
                int c = count.getAndIncrement();
                LOGGER.info("All stable for {} polls", c);
                return c > 60;
            }
            zkPods[0] = zkSnapshot;
            count.set(0);
            return false;
        });
    }

    void checkZkPodsLog(List<String> newZkPodNames) {
        for (String name : newZkPodNames) {
            //Test that second pod does not have errors or failures in events
            LOGGER.info("Checking logs fro pod {}", name);
            String uid = kubeClient().getPodUid(name);
            List<Event> eventsForSecondPod = getEvents(uid);
            assertThat(eventsForSecondPod, hasAllOfReasons(Scheduled, Pulled, Created, Started));
        }
    }

    void waitForZkPods(List<String> newZkPodNames) {
        for (String name : newZkPodNames) {
            StUtils.waitForPod(name);
            LOGGER.info("Pod {} is ready", name);
        }
        waitForZkRollUp();
    }

    @Test
    @Tag(REGRESSION)
    void testKafkaJBODDeleteClaimsTrueFalse() {
        int kafkaReplicas = 2;
        int diskSizeGi = 10;

        JbodStorage jbodStorage = new JbodStorageBuilder().withVolumes(
            new PersistentClaimStorageBuilder().withDeleteClaim(true).withId(0).withSize(diskSizeGi + "Gi").build(),
            new PersistentClaimStorageBuilder().withDeleteClaim(false).withId(1).withSize(diskSizeGi + "Gi").build()).build();

        testMethodResources().kafkaJBOD(CLUSTER_NAME, kafkaReplicas, jbodStorage).done();
        // kafka cluster already deployed
        verifyVolumeNamesAndLabels(2, 2, 10);
        LOGGER.info("Deleting cluster");
        cmdKubeClient().deleteByName("kafka", CLUSTER_NAME).waitForResourceDeletion("pod", kafkaPodName(CLUSTER_NAME, 0));
        verifyPVCDeletion(2, jbodStorage);
    }

    @Test
    @Tag(REGRESSION)
    void testKafkaJBODDeleteClaimsTrue() {
        int kafkaReplicas = 2;
        int diskSizeGi = 10;

        JbodStorage jbodStorage = new JbodStorageBuilder().withVolumes(
            new PersistentClaimStorageBuilder().withDeleteClaim(true).withId(0).withSize(diskSizeGi + "Gi").build(),
            new PersistentClaimStorageBuilder().withDeleteClaim(true).withId(1).withSize(diskSizeGi + "Gi").build()).build();

        testMethodResources().kafkaJBOD(CLUSTER_NAME, kafkaReplicas, jbodStorage).done();
        // kafka cluster already deployed

        verifyVolumeNamesAndLabels(kafkaReplicas, jbodStorage.getVolumes().size(), diskSizeGi);
        LOGGER.info("Deleting Kafka cluster {}", CLUSTER_NAME);
        cmdKubeClient().deleteByName("kafka", CLUSTER_NAME).waitForResourceDeletion("Kafka", CLUSTER_NAME);
        LOGGER.info("Waiting for Kafka pods deletion");
        StUtils.waitForKafkaClusterPodsDeletion(CLUSTER_NAME);
        verifyPVCDeletion(kafkaReplicas, jbodStorage);
    }

    @Test
    @Tag(REGRESSION)
    void testKafkaJBODDeleteClaimsFalse() {
        int kafkaReplicas = 2;
        int diskSizeGi = 10;

        JbodStorage jbodStorage = new JbodStorageBuilder().withVolumes(
            new PersistentClaimStorageBuilder().withDeleteClaim(false).withId(0).withSize(diskSizeGi + "Gi").build(),
            new PersistentClaimStorageBuilder().withDeleteClaim(false).withId(1).withSize(diskSizeGi + "Gi").build()).build();

        testMethodResources().kafkaJBOD(CLUSTER_NAME, kafkaReplicas, jbodStorage).done();
        // kafka cluster already deployed
        verifyVolumeNamesAndLabels(kafkaReplicas, jbodStorage.getVolumes().size(), diskSizeGi);
        LOGGER.info("Deleting cluster");
        cmdKubeClient().deleteByName("kafka", CLUSTER_NAME).waitForResourceDeletion("Kafka", CLUSTER_NAME);
        LOGGER.info("Waiting for Kafka pods deletion");
        StUtils.waitForKafkaClusterPodsDeletion(CLUSTER_NAME);
        verifyPVCDeletion(kafkaReplicas, jbodStorage);
    }

    void verifyPVCDeletion(int kafkaReplicas, JbodStorage jbodStorage) {
        List<String> pvcs = kubeClient().listPersistentVolumeClaims().stream()
                .map(pvc -> pvc.getMetadata().getName())
                .collect(Collectors.toList());

        jbodStorage.getVolumes().forEach(singleVolumeStorage -> {
            for (int i = 0; i < kafkaReplicas; i++) {
                String volumeName = "data-" + singleVolumeStorage.getId() + "-" + CLUSTER_NAME + "-kafka-" + i;
                LOGGER.info("Verifying volume: " + volumeName);
                if (((PersistentClaimStorage) singleVolumeStorage).isDeleteClaim()) {
                    assertThat(pvcs, not(hasItem(volumeName)));
                } else {
                    assertThat(pvcs, hasItem(volumeName));
                }
            }
        });
    }

    void verifyVolumeNamesAndLabels(int kafkaReplicas, int diskCountPerReplica, int diskSizeGi) {

        ArrayList pvcs = new ArrayList();

        kubeClient().listPersistentVolumeClaims().stream()
                .forEach(volume -> {
                    String volumeName = volume.getMetadata().getName();
                    pvcs.add(volumeName);
                    LOGGER.info("Checking labels for volume:" + volumeName);
                    assertEquals(CLUSTER_NAME, volume.getMetadata().getLabels().get("strimzi.io/cluster"));
                    assertEquals("Kafka", volume.getMetadata().getLabels().get("strimzi.io/kind"));
                    assertEquals(CLUSTER_NAME.concat("-kafka"), volume.getMetadata().getLabels().get("strimzi.io/name"));
                    assertEquals(diskSizeGi + "Gi", volume.getSpec().getResources().getRequests().get("storage").getAmount());
                });

        LOGGER.info("Checking PVC names included in JBOD array");
        for (int i = 0; i < kafkaReplicas; i++) {
            for (int j = 0; j < diskCountPerReplica; j++) {
                pvcs.contains("data-" + j + "-" + CLUSTER_NAME + "-kafka-" + i);
            }
        }

        LOGGER.info("Checking PVC on Kafka pods");
        for (int i = 0; i < kafkaReplicas; i++) {
            ArrayList dataSourcesOnPod = new ArrayList();
            ArrayList pvcsOnPod = new ArrayList();

            LOGGER.info("Getting list of mounted data sources and PVCs on Kafka pod " + i);
            for (int j = 0; j < diskCountPerReplica; j++) {
                dataSourcesOnPod.add(kubeClient().getPod(CLUSTER_NAME.concat("-kafka-" + i))
                        .getSpec().getVolumes().get(j).getName());
                pvcsOnPod.add(kubeClient().getPod(CLUSTER_NAME.concat("-kafka-" + i))
                        .getSpec().getVolumes().get(j).getPersistentVolumeClaim().getClaimName());
            }

            LOGGER.info("Verifying mounted data sources and PVCs on Kafka pod " + i);
            for (int j = 0; j < diskCountPerReplica; j++) {
                dataSourcesOnPod.contains("data-" + j);
                pvcsOnPod.contains("data-" + j + "-" + CLUSTER_NAME + "-kafka-" + i);
            }
        }
    }

    @BeforeEach
    void createTestResources() throws Exception {
        createTestMethodResources();
        testMethodResources.createServiceResource(Constants.KAFKA_CLIENTS, Environment.INGRESS_DEFAULT_PORT, NAMESPACE).done();
        testMethodResources.createIngress(Constants.KAFKA_CLIENTS, Environment.INGRESS_DEFAULT_PORT, CONFIG.getMasterUrl(), NAMESPACE).done();
    }

    @BeforeAll
    void setupEnvironment() {
        LOGGER.info("Creating resources before the test class");
        prepareEnvForOperator(NAMESPACE);

        createTestClassResources();
        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        testClassResources.clusterOperator(NAMESPACE).done();
    }

    @Override
    void tearDownEnvironmentAfterEach() throws Exception {
        deleteTestMethodResources();
        waitForDeletion(Constants.TIMEOUT_TEARDOWN);
    }
}
