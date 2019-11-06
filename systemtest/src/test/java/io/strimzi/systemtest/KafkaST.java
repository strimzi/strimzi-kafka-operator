/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.EntityOperatorSpec;
import io.strimzi.api.kafka.model.EntityTopicOperatorSpec;
import io.strimzi.api.kafka.model.EntityUserOperatorSpec;
import io.strimzi.api.kafka.model.KafkaClusterSpec;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.ZookeeperClusterSpec;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationScramSha512;
import io.strimzi.api.kafka.model.listener.KafkaListenerTls;
import io.strimzi.api.kafka.model.listener.NodePortListenerBrokerOverride;
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
import org.apache.kafka.common.config.TopicConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.strimzi.api.kafka.model.KafkaResources.kafkaStatefulSetName;
import static io.strimzi.api.kafka.model.KafkaResources.zookeeperStatefulSetName;
import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.LOADBALANCER_SUPPORTED;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
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
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag(REGRESSION)
class KafkaST extends MessagingBaseST {

    private static final Logger LOGGER = LogManager.getLogger(KafkaST.class);

    public static final String NAMESPACE = "kafka-cluster-test";
    private static final String TOPIC_NAME = "test-topic";
    private static final Pattern ZK_SERVER_STATE = Pattern.compile("zk_server_state\\s+(leader|follower)");

    @Test
    @OpenShiftOnly
    void testDeployKafkaClusterViaTemplate() {
        createCustomResources("../examples/templates/cluster-operator");
        String appName = "strimzi-ephemeral";
        Oc oc = (Oc) cmdKubeClient();
        String clusterName = "openshift-my-cluster";
        oc.newApp(appName, map("CLUSTER_NAME", clusterName));
        StUtils.waitForAllStatefulSetPodsReady(KafkaResources.zookeeperStatefulSetName(clusterName), 3);
        StUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(clusterName), 3);
        StUtils.waitForDeploymentReady(KafkaResources.entityOperatorDeploymentName(clusterName), 1);

        //Testing docker images
        testDockerImagesForKafkaCluster(clusterName, 3, 3, false);

        //Testing labels
        verifyLabelsForKafkaCluster(clusterName, appName);

        LOGGER.info("Deleting Kafka cluster {} after test", clusterName);
        oc.deleteByName("Kafka", clusterName);

        //Wait for kafka deletion
        oc.waitForResourceDeletion("Kafka", clusterName);
        kubeClient().listPods().stream()
            .filter(p -> p.getMetadata().getName().startsWith(clusterName))
            .forEach(p -> StUtils.waitForPodDeletion(p.getMetadata().getName()));

        StUtils.waitForStatefulSetDeletion(KafkaResources.kafkaStatefulSetName(clusterName));
        StUtils.waitForStatefulSetDeletion(KafkaResources.zookeeperStatefulSetName(clusterName));
        StUtils.waitForDeploymentDeletion(KafkaResources.entityOperatorDeploymentName(clusterName));
        deleteCustomResources("../examples/templates/cluster-operator");
    }

    @Test
    @Tag(ACCEPTANCE)
    @Tag(LOADBALANCER_SUPPORTED)
    void testKafkaAndZookeeperScaleUpScaleDown() throws Exception {
        setOperationID(startTimeMeasuring(Operation.SCALE_UP));
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

        final int initialReplicas = kubeClient().getStatefulSet(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME)).getStatus().getReplicas();

        assertEquals(3, initialReplicas);

        // Create topic before scale up to ensure no partitions created on last broker (which will mess up scale down)
        String firstTopicName = "test-topic";
        testMethodResources().topic(CLUSTER_NAME, firstTopicName, 3, initialReplicas)
                .editSpec()
                    .addToConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, initialReplicas - 1)
                .endSpec()
            .done();

        // scale up
        final int scaleTo = initialReplicas + 1;
        final int newPodId = initialReplicas;
        final String newPodName = KafkaResources.kafkaPodName(CLUSTER_NAME,  newPodId);
        LOGGER.info("Scaling up to {}", scaleTo);
        // Create snapshot of current cluster
        String kafkaSsName = kafkaStatefulSetName(CLUSTER_NAME);
        Map<String, String> kafkaPods = StUtils.ssSnapshot(kafkaSsName);
        replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getKafka().setReplicas(scaleTo));
        kafkaPods = StUtils.waitTillSsHasRolled(kafkaSsName, scaleTo, kafkaPods);
        LOGGER.info("Scaled up to {}", scaleTo);

        //Test that the new pod does not have errors or failures in events
        String uid = kubeClient().getPodUid(newPodName);
        List<Event> events = kubeClient().listEvents(uid);
        assertThat(events, hasAllOfReasons(Scheduled, Pulled, Created, Started));
        waitForClusterAvailability(NAMESPACE, firstTopicName);
        LOGGER.info("Could produce/consume with topic {}", firstTopicName);
        //Test that CO doesn't have any exceptions in log
        TimeMeasuringSystem.stopOperation(getOperationID());
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, getOperationID()));

        // scale down
        LOGGER.info("Scaling down");
        // Get kafka new pod uid before deletion
        uid = kubeClient().getPodUid(newPodName);
        setOperationID(startTimeMeasuring(Operation.SCALE_DOWN));
        replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getKafka().setReplicas(initialReplicas));
        kafkaPods = StUtils.waitTillSsHasRolled(kafkaSsName, initialReplicas, kafkaPods);
        LOGGER.info("Scaled down to {}", initialReplicas);

        final int finalReplicas = kubeClient().getStatefulSet(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME)).getStatus().getReplicas();
        assertThat(finalReplicas, is(initialReplicas));

        //Test that the new broker has event 'Killing'
        assertThat(kubeClient().listEvents(uid), hasAllOfReasons(Killing));
        //Test that stateful set has event 'SuccessfulDelete'
        uid = kubeClient().getStatefulSetUid(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));
        assertThat(kubeClient().listEvents(uid), hasAllOfReasons(SuccessfulDelete));
        //Test that CO doesn't have any exceptions in log
        TimeMeasuringSystem.stopOperation(getOperationID());
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, getOperationID()));

        String secondTopicName = "test-topic-2";
        testMethodResources().topic(CLUSTER_NAME, secondTopicName, finalReplicas, finalReplicas).done();
        waitForClusterAvailability(NAMESPACE, secondTopicName);
        LOGGER.info("Could produce/consume with topic {}", secondTopicName);
    }

    @Test
    void testEODeletion() {
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3).done();

        // Get pod name to check termination process
        Pod pod = kubeClient().listPods().stream()
                .filter(p -> p.getMetadata().getName().startsWith(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME)))
                .findAny()
                .get();

        assertThat("Entity operator pod does not exist", pod, notNullValue());

        LOGGER.info("Setting entity operator to null");

        replaceKafkaResource(CLUSTER_NAME, kafka -> {
            kafka.getSpec().setEntityOperator(null);
        });

        // Wait when EO(UO + TO) will be removed
        StUtils.waitForDeploymentDeletion(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME));
        StUtils.waitForPodDeletion(pod.getMetadata().getName());

        LOGGER.info("Entity operator was deleted");
    }

    @Test
    void testZookeeperScaleUpScaleDown() {
        setOperationID(startTimeMeasuring(Operation.SCALE_UP));
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3).done();
        // kafka cluster already deployed
        LOGGER.info("Running zookeeperScaleUpScaleDown with cluster {}", CLUSTER_NAME);
        final int initialZkReplicas = kubeClient().getStatefulSet(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME)).getStatus().getReplicas();
        assertThat(initialZkReplicas, is(3));

        final int scaleZkTo = initialZkReplicas + 4;
        final List<String> newZkPodNames = new ArrayList<String>() {{
                for (int i = initialZkReplicas; i < scaleZkTo; i++) {
                    add(KafkaResources.zookeeperPodName(CLUSTER_NAME, i));
                }
            }};

        LOGGER.info("Scaling up to {}", scaleZkTo);
        replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getZookeeper().setReplicas(scaleZkTo));

        waitForZkPods(newZkPodNames);
        // check the new node is either in leader or follower state
        waitForZkMntr(ZK_SERVER_STATE, 0, 1, 2, 3, 4, 5, 6);
        checkZkPodsLog(newZkPodNames);

        //Test that CO doesn't have any exceptions in log
        TimeMeasuringSystem.stopOperation(getOperationID());
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, getOperationID()));

        // scale down
        LOGGER.info("Scaling down");
        // Get zk-3 uid before deletion
        String uid = kubeClient().getPodUid(newZkPodNames.get(3));
        setOperationID(startTimeMeasuring(Operation.SCALE_DOWN));
        replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getZookeeper().setReplicas(initialZkReplicas));

        for (String name : newZkPodNames) {
            StUtils.waitForPodDeletion(name);
        }

        // Wait for one zk pods will became leader and others follower state
        waitForZkMntr(ZK_SERVER_STATE, 0, 1, 2);

        //Test that the second pod has event 'Killing'
        assertThat(kubeClient().listEvents(uid), hasAllOfReasons(Killing));
        //Test that stateful set has event 'SuccessfulDelete'
        uid = kubeClient().getStatefulSetUid(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME));
        assertThat(kubeClient().listEvents(uid), hasAllOfReasons(SuccessfulDelete));
        // Stop measuring
        TimeMeasuringSystem.stopOperation(getOperationID());
        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, getOperationID()));
    }

    @Test
    @SuppressWarnings({"checkstyle:MethodLength", "checkstyle:JavaNCSS"})
    void testCustomAndUpdatedValues() {
        LinkedHashMap<String, String> envVarGeneral = new LinkedHashMap<>();
        envVarGeneral.put("TEST_ENV_1", "test.env.one");
        envVarGeneral.put("TEST_ENV_2", "test.env.two");

        LinkedHashMap<String, String> envVarUpdated = new LinkedHashMap<>();
        envVarUpdated.put("TEST_ENV_2", "updated.test.env.two");
        envVarUpdated.put("TEST_ENV_3", "test.env.three");

        // Kafka Broker config
        Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put("offsets.topic.replication.factor", "1");
        kafkaConfig.put("transaction.state.log.replication.factor", "1");
        kafkaConfig.put("default.replication.factor", "1");

        Map<String, Object> updatedKafkaConfig = new HashMap<>();
        updatedKafkaConfig.put("offsets.topic.replication.factor", "2");
        updatedKafkaConfig.put("transaction.state.log.replication.factor", "2");
        updatedKafkaConfig.put("default.replication.factor", "2");

        // Zookeeper Config
        Map<String, Object> zookeeperConfig = new HashMap<>();
        zookeeperConfig.put("tickTime", "2000");
        zookeeperConfig.put("initLimit", "5");
        zookeeperConfig.put("syncLimit", "2");
        zookeeperConfig.put("autopurge.purgeInterval", "1");

        Map<String, Object> updatedZookeeperConfig = new HashMap<>();
        updatedZookeeperConfig.put("tickTime", "2500");
        updatedZookeeperConfig.put("initLimit", "3");
        updatedZookeeperConfig.put("syncLimit", "5");

        int initialDelaySeconds = 30;
        int timeoutSeconds = 10;
        int updatedInitialDelaySeconds = 31;
        int updatedTimeoutSeconds = 11;
        int periodSeconds = 10;
        int successThreshold = 1;
        int failureThreshold = 3;
        int updatedPeriodSeconds = 5;
        int updatedFailureThreshold = 1;

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
                        .withPeriodSeconds(periodSeconds)
                        .withSuccessThreshold(successThreshold)
                        .withFailureThreshold(failureThreshold)
                    .endReadinessProbe()
                    .withNewLivenessProbe()
                        .withInitialDelaySeconds(initialDelaySeconds)
                        .withTimeoutSeconds(timeoutSeconds)
                        .withPeriodSeconds(periodSeconds)
                        .withSuccessThreshold(successThreshold)
                        .withFailureThreshold(failureThreshold)
                    .endLivenessProbe()
                    .withConfig(kafkaConfig)
                    .withNewTemplate()
                        .withNewKafkaContainer()
                            .withEnv(StUtils.createContainerEnvVarsFromMap(envVarGeneral))
                        .endKafkaContainer()
                        .withNewTlsSidecarContainer()
                            .withEnv(StUtils.createContainerEnvVarsFromMap(envVarGeneral))
                        .endTlsSidecarContainer()
                    .endTemplate()
                .endKafka()
                .editZookeeper()
                    .withNewTlsSidecar()
                        .withNewReadinessProbe()
                            .withInitialDelaySeconds(initialDelaySeconds)
                            .withTimeoutSeconds(timeoutSeconds)
                            .withPeriodSeconds(periodSeconds)
                            .withSuccessThreshold(successThreshold)
                            .withFailureThreshold(failureThreshold)
                        .endReadinessProbe()
                        .withNewLivenessProbe()
                            .withInitialDelaySeconds(initialDelaySeconds)
                            .withTimeoutSeconds(timeoutSeconds)
                            .withPeriodSeconds(periodSeconds)
                            .withSuccessThreshold(successThreshold)
                            .withFailureThreshold(failureThreshold)
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
                    .withNewTemplate()
                        .withNewZookeeperContainer()
                            .withEnv(StUtils.createContainerEnvVarsFromMap(envVarGeneral))
                        .endZookeeperContainer()
                        .withNewTlsSidecarContainer()
                            .withEnv(StUtils.createContainerEnvVarsFromMap(envVarGeneral))
                        .endTlsSidecarContainer()
                    .endTemplate()
                .endZookeeper()
                .editEntityOperator()
                    .withNewTemplate()
                        .withNewTopicOperatorContainer()
                            .withEnv(StUtils.createContainerEnvVarsFromMap(envVarGeneral))
                        .endTopicOperatorContainer()
                        .withNewUserOperatorContainer()
                            .withEnv(StUtils.createContainerEnvVarsFromMap(envVarGeneral))
                        .endUserOperatorContainer()
                        .withNewTlsSidecarContainer()
                            .withEnv(StUtils.createContainerEnvVarsFromMap(envVarGeneral))
                        .endTlsSidecarContainer()
                    .endTemplate()
                    .editUserOperator()
                        .withNewReadinessProbe()
                            .withInitialDelaySeconds(initialDelaySeconds)
                            .withTimeoutSeconds(timeoutSeconds)
                            .withPeriodSeconds(periodSeconds)
                            .withSuccessThreshold(successThreshold)
                            .withFailureThreshold(failureThreshold)
                        .endReadinessProbe()
                            .withNewLivenessProbe()
                            .withInitialDelaySeconds(initialDelaySeconds)
                            .withTimeoutSeconds(timeoutSeconds)
                            .withPeriodSeconds(periodSeconds)
                            .withSuccessThreshold(successThreshold)
                            .withFailureThreshold(failureThreshold)
                        .endLivenessProbe()
                    .endUserOperator()
                    .editTopicOperator()
                        .withNewReadinessProbe()
                            .withInitialDelaySeconds(initialDelaySeconds)
                            .withTimeoutSeconds(timeoutSeconds)
                            .withPeriodSeconds(periodSeconds)
                            .withSuccessThreshold(successThreshold)
                            .withFailureThreshold(failureThreshold)
                        .endReadinessProbe()
                        .withNewLivenessProbe()
                            .withInitialDelaySeconds(initialDelaySeconds)
                            .withTimeoutSeconds(timeoutSeconds)
                            .withPeriodSeconds(periodSeconds)
                            .withSuccessThreshold(successThreshold)
                            .withFailureThreshold(failureThreshold)
                        .endLivenessProbe()
                    .endTopicOperator()
                    .withNewTlsSidecar()
                        .withNewReadinessProbe()
                            .withInitialDelaySeconds(initialDelaySeconds)
                            .withTimeoutSeconds(timeoutSeconds)
                            .withPeriodSeconds(periodSeconds)
                            .withSuccessThreshold(successThreshold)
                            .withFailureThreshold(failureThreshold)
                        .endReadinessProbe()
                        .withNewLivenessProbe()
                            .withInitialDelaySeconds(initialDelaySeconds)
                            .withTimeoutSeconds(timeoutSeconds)
                            .withPeriodSeconds(periodSeconds)
                            .withSuccessThreshold(successThreshold)
                            .withFailureThreshold(failureThreshold)
                        .endLivenessProbe()
                    .endTlsSidecar()
                .endEntityOperator()
                .endSpec()
            .done();

        Map<String, String> kafkaSnapshot = StUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));
        Map<String, String> zkSnapshot = StUtils.ssSnapshot(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME));
        Map<String, String> eoPod = StUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME));

        LOGGER.info("Verify values before update");
        checkReadinessLivenessProbe(kafkaStatefulSetName(CLUSTER_NAME), "kafka", initialDelaySeconds, timeoutSeconds,
                periodSeconds, successThreshold, failureThreshold);
        checkComponentConfiguration(kafkaStatefulSetName(CLUSTER_NAME), "kafka", "KAFKA_CONFIGURATION", kafkaConfig);
        checkSpecificVariablesInContainer(kafkaStatefulSetName(CLUSTER_NAME), "kafka", envVarGeneral);
        checkReadinessLivenessProbe(kafkaStatefulSetName(CLUSTER_NAME), "tls-sidecar", initialDelaySeconds, timeoutSeconds,
                periodSeconds, successThreshold, failureThreshold);
        checkSpecificVariablesInContainer(kafkaStatefulSetName(CLUSTER_NAME), "tls-sidecar", envVarGeneral);

        LOGGER.info("Testing Zookeepers");
        checkReadinessLivenessProbe(zookeeperStatefulSetName(CLUSTER_NAME), "zookeeper", initialDelaySeconds, timeoutSeconds,
                periodSeconds, successThreshold, failureThreshold);
        checkComponentConfiguration(zookeeperStatefulSetName(CLUSTER_NAME), "zookeeper", "ZOOKEEPER_CONFIGURATION", zookeeperConfig);
        checkSpecificVariablesInContainer(zookeeperStatefulSetName(CLUSTER_NAME), "zookeeper", envVarGeneral);
        checkReadinessLivenessProbe(zookeeperStatefulSetName(CLUSTER_NAME), "tls-sidecar", initialDelaySeconds, timeoutSeconds,
                periodSeconds, successThreshold, failureThreshold);
        checkSpecificVariablesInContainer(zookeeperStatefulSetName(CLUSTER_NAME), "tls-sidecar", envVarGeneral);

        LOGGER.info("Checking configuration of TO and UO");
        checkReadinessLivenessProbe(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), "topic-operator", initialDelaySeconds, timeoutSeconds,
                periodSeconds, successThreshold, failureThreshold);
        checkSpecificVariablesInContainer(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), "topic-operator", envVarGeneral);
        checkReadinessLivenessProbe(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), "user-operator", initialDelaySeconds, timeoutSeconds,
                periodSeconds, successThreshold, failureThreshold);
        checkSpecificVariablesInContainer(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), "user-operator", envVarGeneral);
        checkReadinessLivenessProbe(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), "tls-sidecar", initialDelaySeconds, timeoutSeconds,
                periodSeconds, successThreshold, failureThreshold);
        checkSpecificVariablesInContainer(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), "tls-sidecar", envVarGeneral);

        LOGGER.info("Updating configuration of Kafka cluster");
        replaceKafkaResource(CLUSTER_NAME, k -> {
            KafkaClusterSpec kafkaClusterSpec = k.getSpec().getKafka();
            kafkaClusterSpec.getLivenessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            kafkaClusterSpec.getReadinessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            kafkaClusterSpec.getLivenessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            kafkaClusterSpec.getReadinessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            kafkaClusterSpec.getLivenessProbe().setPeriodSeconds(updatedPeriodSeconds);
            kafkaClusterSpec.getReadinessProbe().setPeriodSeconds(updatedPeriodSeconds);
            kafkaClusterSpec.getLivenessProbe().setFailureThreshold(updatedFailureThreshold);
            kafkaClusterSpec.getReadinessProbe().setFailureThreshold(updatedFailureThreshold);
            kafkaClusterSpec.setConfig(updatedKafkaConfig);
            kafkaClusterSpec.getTlsSidecar().getLivenessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            kafkaClusterSpec.getTlsSidecar().getReadinessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            kafkaClusterSpec.getTlsSidecar().getLivenessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            kafkaClusterSpec.getTlsSidecar().getReadinessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            kafkaClusterSpec.getTlsSidecar().getLivenessProbe().setPeriodSeconds(updatedPeriodSeconds);
            kafkaClusterSpec.getTlsSidecar().getReadinessProbe().setPeriodSeconds(updatedPeriodSeconds);
            kafkaClusterSpec.getTlsSidecar().getLivenessProbe().setFailureThreshold(updatedFailureThreshold);
            kafkaClusterSpec.getTlsSidecar().getReadinessProbe().setFailureThreshold(updatedFailureThreshold);
            kafkaClusterSpec.getTemplate().getKafkaContainer().setEnv(StUtils.createContainerEnvVarsFromMap(envVarUpdated));
            kafkaClusterSpec.getTemplate().getTlsSidecarContainer().setEnv(StUtils.createContainerEnvVarsFromMap(envVarUpdated));
            ZookeeperClusterSpec zookeeperClusterSpec = k.getSpec().getZookeeper();
            zookeeperClusterSpec.getLivenessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            zookeeperClusterSpec.getReadinessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            zookeeperClusterSpec.getLivenessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            zookeeperClusterSpec.getReadinessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            zookeeperClusterSpec.getLivenessProbe().setPeriodSeconds(updatedPeriodSeconds);
            zookeeperClusterSpec.getReadinessProbe().setPeriodSeconds(updatedPeriodSeconds);
            zookeeperClusterSpec.getLivenessProbe().setFailureThreshold(updatedFailureThreshold);
            zookeeperClusterSpec.getReadinessProbe().setFailureThreshold(updatedFailureThreshold);
            zookeeperClusterSpec.setConfig(updatedZookeeperConfig);
            zookeeperClusterSpec.getTlsSidecar().getLivenessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            zookeeperClusterSpec.getTlsSidecar().getReadinessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            zookeeperClusterSpec.getTlsSidecar().getLivenessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            zookeeperClusterSpec.getTlsSidecar().getReadinessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            zookeeperClusterSpec.getTlsSidecar().getLivenessProbe().setPeriodSeconds(updatedPeriodSeconds);
            zookeeperClusterSpec.getTlsSidecar().getReadinessProbe().setPeriodSeconds(updatedPeriodSeconds);
            zookeeperClusterSpec.getTlsSidecar().getLivenessProbe().setFailureThreshold(updatedFailureThreshold);
            zookeeperClusterSpec.getTlsSidecar().getReadinessProbe().setFailureThreshold(updatedFailureThreshold);
            zookeeperClusterSpec.getTemplate().getZookeeperContainer().setEnv(StUtils.createContainerEnvVarsFromMap(envVarUpdated));
            zookeeperClusterSpec.getTemplate().getTlsSidecarContainer().setEnv(StUtils.createContainerEnvVarsFromMap(envVarUpdated));
            // Configuring TO and UO to use new values for InitialDelaySeconds and TimeoutSeconds
            EntityOperatorSpec entityOperatorSpec = k.getSpec().getEntityOperator();
            entityOperatorSpec.getTopicOperator().getLivenessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            entityOperatorSpec.getTopicOperator().getReadinessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            entityOperatorSpec.getTopicOperator().getLivenessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            entityOperatorSpec.getTopicOperator().getReadinessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            entityOperatorSpec.getTopicOperator().getLivenessProbe().setPeriodSeconds(updatedPeriodSeconds);
            entityOperatorSpec.getTopicOperator().getReadinessProbe().setPeriodSeconds(updatedPeriodSeconds);
            entityOperatorSpec.getTopicOperator().getLivenessProbe().setFailureThreshold(updatedFailureThreshold);
            entityOperatorSpec.getTopicOperator().getReadinessProbe().setFailureThreshold(updatedFailureThreshold);
            entityOperatorSpec.getUserOperator().getLivenessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            entityOperatorSpec.getUserOperator().getReadinessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            entityOperatorSpec.getUserOperator().getLivenessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            entityOperatorSpec.getUserOperator().getReadinessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            entityOperatorSpec.getUserOperator().getLivenessProbe().setPeriodSeconds(updatedPeriodSeconds);
            entityOperatorSpec.getUserOperator().getReadinessProbe().setPeriodSeconds(updatedPeriodSeconds);
            entityOperatorSpec.getUserOperator().getLivenessProbe().setFailureThreshold(updatedFailureThreshold);
            entityOperatorSpec.getUserOperator().getReadinessProbe().setFailureThreshold(updatedFailureThreshold);
            entityOperatorSpec.getTlsSidecar().getLivenessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            entityOperatorSpec.getTlsSidecar().getReadinessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
            entityOperatorSpec.getTlsSidecar().getLivenessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            entityOperatorSpec.getTlsSidecar().getReadinessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
            entityOperatorSpec.getTlsSidecar().getLivenessProbe().setPeriodSeconds(updatedPeriodSeconds);
            entityOperatorSpec.getTlsSidecar().getReadinessProbe().setPeriodSeconds(updatedPeriodSeconds);
            entityOperatorSpec.getTlsSidecar().getLivenessProbe().setFailureThreshold(updatedFailureThreshold);
            entityOperatorSpec.getTlsSidecar().getReadinessProbe().setFailureThreshold(updatedFailureThreshold);
            entityOperatorSpec.getTemplate().getTopicOperatorContainer().setEnv(StUtils.createContainerEnvVarsFromMap(envVarUpdated));
            entityOperatorSpec.getTemplate().getUserOperatorContainer().setEnv(StUtils.createContainerEnvVarsFromMap(envVarUpdated));
            entityOperatorSpec.getTemplate().getTlsSidecarContainer().setEnv(StUtils.createContainerEnvVarsFromMap(envVarUpdated));
        });

        StUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 2, kafkaSnapshot);
        StUtils.waitTillSsHasRolled(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME), 2, zkSnapshot);
        StUtils.waitTillDepHasRolled(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), 1, eoPod);

        LOGGER.info("Verify values after update");
        checkReadinessLivenessProbe(kafkaStatefulSetName(CLUSTER_NAME), "kafka", updatedInitialDelaySeconds, updatedTimeoutSeconds,
                updatedPeriodSeconds, successThreshold, updatedFailureThreshold);
        checkComponentConfiguration(kafkaStatefulSetName(CLUSTER_NAME), "kafka", "KAFKA_CONFIGURATION", updatedKafkaConfig);
        checkSpecificVariablesInContainer(kafkaStatefulSetName(CLUSTER_NAME), "kafka", envVarUpdated);
        checkReadinessLivenessProbe(kafkaStatefulSetName(CLUSTER_NAME), "tls-sidecar", updatedInitialDelaySeconds, updatedTimeoutSeconds,
                updatedPeriodSeconds, successThreshold, updatedFailureThreshold);
        checkSpecificVariablesInContainer(kafkaStatefulSetName(CLUSTER_NAME), "tls-sidecar", envVarUpdated);

        LOGGER.info("Testing Zookeepers");
        checkReadinessLivenessProbe(zookeeperStatefulSetName(CLUSTER_NAME), "zookeeper", updatedInitialDelaySeconds, updatedTimeoutSeconds,
                updatedPeriodSeconds, successThreshold, updatedFailureThreshold);
        checkComponentConfiguration(zookeeperStatefulSetName(CLUSTER_NAME), "zookeeper", "ZOOKEEPER_CONFIGURATION", updatedZookeeperConfig);
        checkSpecificVariablesInContainer(zookeeperStatefulSetName(CLUSTER_NAME), "zookeeper", envVarUpdated);
        checkReadinessLivenessProbe(zookeeperStatefulSetName(CLUSTER_NAME), "tls-sidecar", updatedInitialDelaySeconds, updatedTimeoutSeconds,
                updatedPeriodSeconds, successThreshold, updatedFailureThreshold);
        checkSpecificVariablesInContainer(zookeeperStatefulSetName(CLUSTER_NAME), "tls-sidecar", envVarUpdated);

        LOGGER.info("Getting entity operator to check configuration of TO and UO");
        checkReadinessLivenessProbe(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), "topic-operator", updatedInitialDelaySeconds, updatedTimeoutSeconds,
                updatedPeriodSeconds, successThreshold, updatedFailureThreshold);
        checkSpecificVariablesInContainer(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), "topic-operator", envVarUpdated);
        checkReadinessLivenessProbe(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), "user-operator", updatedInitialDelaySeconds, updatedTimeoutSeconds,
                updatedPeriodSeconds, successThreshold, updatedFailureThreshold);
        checkSpecificVariablesInContainer(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), "user-operator", envVarUpdated);
        checkReadinessLivenessProbe(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), "tls-sidecar", updatedInitialDelaySeconds, updatedTimeoutSeconds,
                updatedPeriodSeconds, successThreshold, updatedFailureThreshold);
        checkSpecificVariablesInContainer(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), "tls-sidecar", envVarUpdated);
    }

    /**
     * Test sending messages over plain transport, without auth
     */
    @Test
    void testSendMessagesPlainAnonymous() throws Exception {
        int messagesCount = 200;
        String topicName = TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);

        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3).done();
        testMethodResources().topic(CLUSTER_NAME, topicName).done();

        testMethodResources().deployKafkaClients(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).done();

        availabilityTest(messagesCount, CLUSTER_NAME, false, topicName, null);
    }

    /**
     * Test sending messages over tls transport using mutual tls auth
     */
    @Test
    void testSendMessagesTlsAuthenticated() throws Exception {
        String kafkaUser = "my-user";
        int messagesCount = 200;
        String topicName = TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);

        // Use a Kafka with plain listener disabled
        testMethodResources().kafka(testMethodResources().defaultKafka(CLUSTER_NAME, 3)
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withNewTls()
                                .withNewKafkaListenerAuthenticationTlsAuth()
                                .endKafkaListenerAuthenticationTlsAuth()
                            .endTls()
                        .endListeners()
                    .endKafka()
                .endSpec().build()).done();
        testMethodResources().topic(CLUSTER_NAME, topicName).done();
        KafkaUser user = testMethodResources().tlsUser(CLUSTER_NAME, kafkaUser).done();
        StUtils.waitForSecretReady(kafkaUser);

        testMethodResources().deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, user).done();
        availabilityTest(messagesCount, CLUSTER_NAME, true, topicName, user);
    }

    /**
     * Test sending messages over plain transport using scram sha auth
     */
    @Test
    @Tag(ACCEPTANCE)
    void testSendMessagesPlainScramSha() throws Exception {
        String kafkaUser = "my-user";
        int messagesCount = 200;
        String topicName = TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);

        // Use a Kafka with plain listener disabled
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 1)
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .withNewPlain()
                                .withNewKafkaListenerAuthenticationScramSha512Auth()
                                .endKafkaListenerAuthenticationScramSha512Auth()
                            .endPlain()
                        .endListeners()
                    .endKafka()
                .endSpec().done();
        testMethodResources().topic(CLUSTER_NAME, topicName).done();
        KafkaUser user = testMethodResources().scramShaUser(CLUSTER_NAME, kafkaUser).done();
        StUtils.waitForSecretReady(kafkaUser);
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

        testMethodResources().deployKafkaClients(false, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, user).done();
        availabilityTest(messagesCount, CLUSTER_NAME, false, topicName, user);
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
        testMethodResources().topic(CLUSTER_NAME, topicName).done();
        KafkaUser user = testMethodResources().scramShaUser(CLUSTER_NAME, kafkaUser).done();
        StUtils.waitForSecretReady(kafkaUser);

        testMethodResources().deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, user).done();
        availabilityTest(messagesCount, CLUSTER_NAME, true, topicName, user);
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

        setOperationID(startTimeMeasuring(Operation.NEXT_RECONCILIATION));

        // Make snapshots for Kafka cluster to meke sure that there is no rolling update after CO reconciliation
        String zkSsName = KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME);
        String kafkaSsName = kafkaStatefulSetName(CLUSTER_NAME);
        String eoDepName = KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME);
        Map<String, String> zkPods = StUtils.ssSnapshot(zkSsName);
        Map<String, String> kafkaPods = StUtils.ssSnapshot(kafkaSsName);
        Map<String, String> eoPods = StUtils.depSnapshot(eoDepName);

        assertResources(cmdKubeClient().namespace(), KafkaResources.kafkaPodName(CLUSTER_NAME, 0), "kafka",
                "1536Mi", "1", "1Gi", "500m");
        assertExpectedJavaOpts(KafkaResources.kafkaPodName(CLUSTER_NAME, 0), "kafka",
                "-Xmx1g", "-Xms512m", "-server", "-XX:+UseG1GC");

        assertResources(cmdKubeClient().namespace(), KafkaResources.zookeeperPodName(CLUSTER_NAME, 0), "zookeeper",
                "1G", "500m", "500M", "250m");
        assertExpectedJavaOpts(KafkaResources.zookeeperPodName(CLUSTER_NAME, 0), "zookeeper",
                "-Xmx1G", "-Xms512M", "-server", "-XX:+UseG1GC");

        Optional<Pod> pod = kubeClient().listPods()
                .stream().filter(p -> p.getMetadata().getName().startsWith(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME)))
                .findFirst();
        assertThat("EO pod does not exist", pod.isPresent(), is(true));

        assertResources(cmdKubeClient().namespace(), pod.get().getMetadata().getName(), "topic-operator",
                "1Gi", "500m", "512Mi", "250m");
        assertResources(cmdKubeClient().namespace(), pod.get().getMetadata().getName(), "user-operator",
                "512M", "300m", "256M", "300m");

        StUtils.waitForReconciliation(testClass, testName, NAMESPACE);

        // Checking no rolling update after last CO reconciliation
        LOGGER.info("Checking no rolling update for Kafka cluster");
        assertThat(StUtils.ssHasRolled(zkSsName, zkPods), is(false));
        assertThat(StUtils.ssHasRolled(kafkaSsName, kafkaPods), is(false));
        assertThat(StUtils.depHasRolled(eoDepName, eoPods), is(false));
        TimeMeasuringSystem.stopOperation(getOperationID());
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
        assertThat(testTopic, is(CoreMatchers.notNullValue()));
        assertThat(testTopic.getSpec(), is(CoreMatchers.notNullValue()));
        assertThat(testTopic.getSpec().getPartitions(), is(Integer.valueOf(2)));

        //Updating second topic via KafkaTopic update
        replaceTopicResource("topic-from-cli", topic -> {
            topic.getSpec().setPartitions(2);
        });
        assertThat(describeTopicUsingPodCLI(CLUSTER_NAME, 0, "topic-from-cli"),
                hasItems("PartitionCount:2"));
        testTopic = fromYamlString(cmdKubeClient().get("kafkatopic", "topic-from-cli"), KafkaTopic.class);
        assertThat(testTopic, is(CoreMatchers.notNullValue()));
        assertThat(testTopic.getSpec(), is(CoreMatchers.notNullValue()));
        assertThat(testTopic.getSpec().getPartitions(), is(Integer.valueOf(2)));

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
    void testRemoveTopicOperatorFromEntityOperator() {
        LOGGER.info("Deploying Kafka cluster {}", CLUSTER_NAME);
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3).done();
        String eoPodName = kubeClient().listPodsByPrefixInName(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME))
            .get(0).getMetadata().getName();

        replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getEntityOperator().setTopicOperator(null));
        //Waiting when EO pod will be recreated without TO
        StUtils.waitForPodDeletion(eoPodName);
        StUtils.waitForDeploymentReady(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), 1);
        StUtils.waitUntilPodContainersCount(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), 2);

        //Checking that TO was removed
        kubeClient().listPodsByPrefixInName(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME)).forEach(pod -> {
            pod.getSpec().getContainers().forEach(container -> {
                assertThat(container.getName(), not(containsString("topic-operator")));
            });
        });

        eoPodName = kubeClient().listPodsByPrefixInName(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME))
                .get(0).getMetadata().getName();

        replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getEntityOperator().setTopicOperator(new EntityTopicOperatorSpec()));
        //Waiting when EO pod will be recreated with TO
        StUtils.waitForPodDeletion(eoPodName);
        StUtils.waitForDeploymentReady(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), 1);

        //Checking that TO was created
        kubeClient().listPodsByPrefixInName(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME)).forEach(pod -> {
            pod.getSpec().getContainers().forEach(container -> {
                assertThat(container.getName(), anyOf(
                    containsString("topic-operator"),
                    containsString("user-operator"),
                    containsString("tls-sidecar"))
                );
            });
        });
    }

    @Test
    void testRemoveUserOperatorFromEntityOperator() {
        LOGGER.info("Deploying Kafka cluster {}", CLUSTER_NAME);
        setOperationID(startTimeMeasuring(Operation.CLUSTER_DEPLOYMENT));
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3).done();
        String eoPodName = kubeClient().listPodsByPrefixInName(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME))
                .get(0).getMetadata().getName();

        replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getEntityOperator().setUserOperator(null));

        //Waiting when EO pod will be recreated without UO
        StUtils.waitForPodDeletion(eoPodName);
        StUtils.waitForDeploymentReady(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), 1);
        StUtils.waitUntilPodContainersCount(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), 2);

        //Checking that UO was removed
        kubeClient().listPodsByPrefixInName(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME)).forEach(pod -> {
            pod.getSpec().getContainers().forEach(container -> {
                assertThat(container.getName(), not(containsString("user-operator")));
            });
        });

        eoPodName = kubeClient().listPodsByPrefixInName(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME))
                .get(0).getMetadata().getName();

        replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getEntityOperator().setUserOperator(new EntityUserOperatorSpec()));
        //Waiting when EO pod will be recreated with UO
        StUtils.waitForPodDeletion(eoPodName);
        StUtils.waitForDeploymentReady(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), 1);

        //Checking that UO was created
        kubeClient().listPodsByPrefixInName(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME)).forEach(pod -> {
            pod.getSpec().getContainers().forEach(container -> {
                assertThat(container.getName(), anyOf(
                        containsString("topic-operator"),
                        containsString("user-operator"),
                        containsString("tls-sidecar"))
                );
            });
        });

        TimeMeasuringSystem.stopOperation(getOperationID());
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, getOperationID()));
    }

    @Test
    void testRemoveUserAndTopicOperatorsFromEntityOperator() {
        LOGGER.info("Deploying Kafka cluster {}", CLUSTER_NAME);
        setOperationID(startTimeMeasuring(Operation.CLUSTER_DEPLOYMENT));
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3).done();

        String eoPodName = kubeClient().listPodsByPrefixInName(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME))
                .get(0).getMetadata().getName();

        replaceKafkaResource(CLUSTER_NAME, k -> {
            EntityOperatorSpec entityOperatorSpec = k.getSpec().getEntityOperator();
            entityOperatorSpec.setTopicOperator(null);
            entityOperatorSpec.setUserOperator(null);
            k.getSpec().setEntityOperator(entityOperatorSpec);
        });

        //Waiting when EO pod will be deleted
        StUtils.waitForDeploymentDeletion(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME));
        StUtils.waitForReplicaSetDeletion(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME));
        StUtils.waitForPodDeletion(eoPodName);

        //Checking that EO was removed
        assertThat(kubeClient().listPodsByPrefixInName(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME)).size(), is(0));

        replaceKafkaResource(CLUSTER_NAME, k -> {
            EntityOperatorSpec entityOperatorSpec = k.getSpec().getEntityOperator();
            entityOperatorSpec.setTopicOperator(new EntityTopicOperatorSpec());
            entityOperatorSpec.setUserOperator(new EntityUserOperatorSpec());
            k.getSpec().setEntityOperator(entityOperatorSpec);
        });
        StUtils.waitForDeploymentReady(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME));

        //Checking that EO was created
        kubeClient().listPodsByPrefixInName(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME)).forEach(pod -> {
            pod.getSpec().getContainers().forEach(container -> {
                assertThat(container.getName(), anyOf(
                    containsString("topic-operator"),
                    containsString("user-operator"),
                    containsString("tls-sidecar"))
                );
            });
        });

        TimeMeasuringSystem.stopOperation(getOperationID());
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, getOperationID()));
    }

    @Test
    void testEntityOperatorWithoutTopicOperator() {
        LOGGER.info("Deploying Kafka cluster without TO in EO");
        setOperationID(startTimeMeasuring(Operation.CLUSTER_DEPLOYMENT));
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3)
            .editSpec()
                .withNewEntityOperator()
                    .withNewUserOperator()
                    .endUserOperator()
                .endEntityOperator()
            .endSpec()
        .done();

        TimeMeasuringSystem.stopOperation(getOperationID());
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, getOperationID()));

        //Checking that TO was not deployed
        kubeClient().listPodsByPrefixInName(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME)).forEach(pod -> {
            pod.getSpec().getContainers().forEach(container -> {
                assertThat(container.getName(), not(containsString("topic-operator")));
            });
        });
    }

    @Test
    void testEntityOperatorWithoutUserOperator() {
        LOGGER.info("Deploying Kafka cluster without UO in EO");
        setOperationID(startTimeMeasuring(Operation.CLUSTER_DEPLOYMENT));
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3)
            .editSpec()
                .withNewEntityOperator()
                    .withNewTopicOperator()
                    .endTopicOperator()
                .endEntityOperator()
            .endSpec()
        .done();

        TimeMeasuringSystem.stopOperation(getOperationID());
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, getOperationID()));

        //Checking that UO was not deployed
        kubeClient().listPodsByPrefixInName(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME)).forEach(pod -> {
            pod.getSpec().getContainers().forEach(container -> {
                assertThat(container.getName(), not(containsString("user-operator")));
            });
        });
    }

    @Test
    void testEntityOperatorWithoutUserAndTopicOperators() {
        LOGGER.info("Deploying Kafka cluster without UO and TO in EO");
        setOperationID(startTimeMeasuring(Operation.CLUSTER_DEPLOYMENT));
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3)
            .editSpec()
                .withNewEntityOperator()
                .endEntityOperator()
            .endSpec()
        .done();

        TimeMeasuringSystem.stopOperation(getOperationID());
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, getOperationID()));

        //Checking that EO was not deployed
        assertThat("EO should not be deployed", kubeClient().listPodsByPrefixInName(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME)).size(), is(0));
    }

    @Test
    void testTopicWithoutLabels() {
        // Negative scenario: creating topic without any labels and make sure that TO can't handle this topic
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3).done();

        // Creating topic without any label
        testMethodResources().topic(CLUSTER_NAME, "topic-without-labels")
            .editMetadata()
                .withLabels(null)
            .endMetadata()
            .done();

        // Checking that resource was created
        assertThat(cmdKubeClient().list("kafkatopic"), hasItems("topic-without-labels"));
        // Checking that TO didn't handle new topic and zk pods don't contain new topic
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
            String imgFromPod = getContainerImageNameFromPod(KafkaResources.zookeeperPodName(clusterName, i), "zookeeper");
            assertThat("Zookeeper pod " + i + " uses wrong image", imgFromDeplConf.get(ZK_IMAGE), is(imgFromPod));
            imgFromPod = getContainerImageNameFromPod(KafkaResources.zookeeperPodName(clusterName, i), "tls-sidecar");
            assertThat("Zookeeper TLS side car for pod " + i + " uses wrong image", imgFromDeplConf.get(TLS_SIDECAR_ZOOKEEPER_IMAGE), is(imgFromPod));
        }

        //Verifying docker image for kafka pods
        for (int i = 0; i < kafkaPods; i++) {
            String imgFromPod = getContainerImageNameFromPod(KafkaResources.kafkaPodName(clusterName, i), "kafka");
            String kafkaVersion = Crds.kafkaOperation(kubeClient().getClient()).inNamespace(NAMESPACE).withName(clusterName).get().getSpec().getKafka().getVersion();
            if (kafkaVersion == null) {
                kafkaVersion = Environment.ST_KAFKA_VERSION;
            }
            assertThat("Kafka pod " + i + " uses wrong image", TestUtils.parseImageMap(imgFromDeplConf.get(KAFKA_IMAGE_MAP)).get(kafkaVersion), is(imgFromPod));
            imgFromPod = getContainerImageNameFromPod(KafkaResources.kafkaPodName(clusterName, i), "tls-sidecar");
            assertThat("Kafka TLS side car for pod " + i + " uses wrong image", imgFromDeplConf.get(TLS_SIDECAR_KAFKA_IMAGE), is(imgFromPod));
            if (rackAwareEnabled) {
                String initContainerImage = getInitContainerImageName(KafkaResources.kafkaPodName(clusterName, i));
                assertThat(initContainerImage, is(imgFromDeplConf.get(KAFKA_INIT_IMAGE)));
            }
        }

        //Verifying docker image for entity-operator
        String entityOperatorPodName = cmdKubeClient().listResourcesByLabel("pod",
                "strimzi.io/name=" + clusterName + "-entity-operator").get(0);
        String imgFromPod = getContainerImageNameFromPod(entityOperatorPodName, "topic-operator");
        assertThat(imgFromPod, is(imgFromDeplConf.get(TO_IMAGE)));
        imgFromPod = getContainerImageNameFromPod(entityOperatorPodName, "user-operator");
        assertThat(imgFromPod, is(imgFromDeplConf.get(UO_IMAGE)));
        imgFromPod = getContainerImageNameFromPod(entityOperatorPodName, "tls-sidecar");
        assertThat(imgFromPod, is(imgFromDeplConf.get(TLS_SIDECAR_EO_IMAGE)));

        LOGGER.info("Docker images verified");
    }

    @Test
    void testManualTriggeringRollingUpdate() {
        String coPodName = kubeClient().listPods("name", "strimzi-cluster-operator").get(0).getMetadata().getName();
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 1).done();

        // rolling update for kafka
        setOperationID(startTimeMeasuring(Operation.ROLLING_UPDATE));
        // set annotation to trigger Kafka rolling update
        kubeClient().statefulSet(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME)).cascading(false).edit()
                .editMetadata()
                    .addToAnnotations("strimzi.io/manual-rolling-update", "true")
                .endMetadata().done();

        // check annotation to trigger rolling update
        assertThat(Boolean.parseBoolean(kubeClient().getStatefulSet(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME))
                .getMetadata().getAnnotations().get("strimzi.io/manual-rolling-update")), is(true));

        // wait when annotation will be removed
        waitFor("CO removes rolling update annotation", Constants.WAIT_FOR_ROLLING_UPDATE_INTERVAL, WAIT_FOR_ROLLING_UPDATE_TIMEOUT,
            () -> getAnnotationsForSS(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME)) == null
                || !getAnnotationsForSS(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME)).containsKey("strimzi.io/manual-rolling-update"));

        // check rolling update messages in CO log
        String coLog = kubeClient().logs(coPodName);
        assertThat(coLog, containsString("Rolling Kafka pod " + KafkaResources.kafkaStatefulSetName(CLUSTER_NAME) + "-0" + " due to manual rolling update"));

        // rolling update for zookeeper
        setOperationID(startTimeMeasuring(Operation.ROLLING_UPDATE));
        // set annotation to trigger Zookeeper rolling update
        kubeClient().statefulSet(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME)).cascading(false).edit()
                .editMetadata()
                    .addToAnnotations("strimzi.io/manual-rolling-update", "true")
                .endMetadata().done();

        // check annotation to trigger rolling update
        assertThat(Boolean.parseBoolean(kubeClient().getStatefulSet(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME))
                .getMetadata().getAnnotations().get("strimzi.io/manual-rolling-update")), is(true));

        // wait when annotation will be removed
        waitFor("CO removes rolling update annotation", Constants.WAIT_FOR_ROLLING_UPDATE_INTERVAL, WAIT_FOR_ROLLING_UPDATE_TIMEOUT,
            () -> getAnnotationsForSS(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME)) == null
                || !getAnnotationsForSS(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME)).containsKey("strimzi.io/manual-rolling-update"));

        // check rolling update messages in CO log
        coLog = kubeClient().logs(coPodName);
        assertThat(coLog, containsString("Rolling Zookeeper pod " + KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME) + "-0" + " to manual rolling update"));
    }

    @Test
    @Tag(NODEPORT_SUPPORTED)
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
    @Tag(NODEPORT_SUPPORTED)
    void testOverrideNodePortConfiguration() throws Exception {
        int brokerNodePort = 32000;
        int brokerId = 0;

        NodePortListenerBrokerOverride nodePortListenerBrokerOverride = new NodePortListenerBrokerOverride();
        nodePortListenerBrokerOverride.setBroker(brokerId);
        nodePortListenerBrokerOverride.setNodePort(brokerNodePort);
        LOGGER.info("Setting nodePort to {} for broker {}", nodePortListenerBrokerOverride.getNodePort(),
                nodePortListenerBrokerOverride.getBroker());

        int clusterBootstrapNodePort = 32100;
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3, 1)
                .editSpec()
                    .editKafka()
                        .editListeners()
                            .withNewKafkaListenerExternalNodePort()
                            .withTls(false)
                                .withNewOverrides()
                                    .withNewBootstrap()
                                        .withNodePort(clusterBootstrapNodePort)
                                    .endBootstrap()
                                    .withBrokers(nodePortListenerBrokerOverride)
                                .endOverrides()
                            .endKafkaListenerExternalNodePort()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .done();

        assertThat(kubeClient().getService(KafkaResources.externalBootstrapServiceName(CLUSTER_NAME))
                .getSpec().getPorts().get(0).getNodePort(), is(clusterBootstrapNodePort));
        LOGGER.info("Checking nodePort to {} for bootstrap service {}", clusterBootstrapNodePort,
                KafkaResources.externalBootstrapServiceName(CLUSTER_NAME));
        assertThat(kubeClient().getService(KafkaResources.kafkaPodName(CLUSTER_NAME, brokerId))
                .getSpec().getPorts().get(0).getNodePort(), is(brokerNodePort));
        LOGGER.info("Checking nodePort to {} for kafka-broker service {}", brokerNodePort,
                KafkaResources.kafkaPodName(CLUSTER_NAME, brokerId));

        waitForClusterAvailability(NAMESPACE);
    }

    @Test
    @Tag(ACCEPTANCE)
    @Tag(NODEPORT_SUPPORTED)
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

        waitForClusterAvailabilityTls(userName, NAMESPACE, CLUSTER_NAME);
    }

    @Test
    @Tag(LOADBALANCER_SUPPORTED)
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

        StUtils.waitUntilAddressIsReachable(kubeClient().getService(KafkaResources.externalBootstrapServiceName(CLUSTER_NAME)).getStatus().getLoadBalancer().getIngress().get(0).getHostname());

        waitForClusterAvailability(NAMESPACE);
    }

    @Test
    @Tag(ACCEPTANCE)
    @Tag(LOADBALANCER_SUPPORTED)
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

        StUtils.waitUntilAddressIsReachable(kubeClient().getService(KafkaResources.externalBootstrapServiceName(CLUSTER_NAME)).getStatus().getLoadBalancer().getIngress().get(0).getHostname());

        waitForClusterAvailabilityTls(userName, NAMESPACE, CLUSTER_NAME);
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
            List<Event> eventsForSecondPod = kubeClient().listEvents(uid);
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
        cmdKubeClient().deleteByName("kafka", CLUSTER_NAME).waitForResourceDeletion("pod", KafkaResources.kafkaPodName(CLUSTER_NAME, 0));
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
                    assertThat(volume.getMetadata().getLabels().get("strimzi.io/cluster"), is(CLUSTER_NAME));
                    assertThat(volume.getMetadata().getLabels().get("strimzi.io/kind"), is("Kafka"));
                    assertThat(volume.getMetadata().getLabels().get("strimzi.io/name"), is(CLUSTER_NAME.concat("-kafka")));
                    assertThat(volume.getSpec().getResources().getRequests().get("storage").getAmount(), is(diskSizeGi + "Gi"));
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

    @Test
    @Tag(NODEPORT_SUPPORTED)
    void testPersistentStorageSize() throws Exception {
        String[] diskSizes = {"70Gi", "20Gi"};
        int kafkaRepl = 2;
        int diskCount = 2;

        JbodStorage jbodStorage =  new JbodStorageBuilder()
                .withVolumes(
                        new PersistentClaimStorageBuilder().withDeleteClaim(false).withId(0).withSize(diskSizes[0]).build(),
                        new PersistentClaimStorageBuilder().withDeleteClaim(false).withId(1).withSize(diskSizes[1]).build()
                ).build();

        testMethodResources().kafka(testMethodResources().defaultKafka(CLUSTER_NAME, kafkaRepl)
                .editSpec()
                    .editKafka()
                        .editListeners()
                            .withNewKafkaListenerExternalNodePort()
                                .withTls(false)
                                .withNewOverrides()
                                    .withNewBootstrap()
                                    .endBootstrap()
                                .endOverrides()
                            .endKafkaListenerExternalNodePort()
                        .endListeners()
                        .withStorage(jbodStorage)
                    .endKafka()
                    .editZookeeper().
                        withReplicas(1)
                    .endZookeeper()
                .endSpec()
                .build())
                .done();

        List<PersistentVolumeClaim> volumes = kubeClient().listPersistentVolumeClaims();

        checkStorageSizeForVolumes(volumes, diskSizes, kafkaRepl, diskCount);

        waitForClusterAvailability(NAMESPACE);
    }

    void checkStorageSizeForVolumes(List<PersistentVolumeClaim> volumes, String[] diskSizes, int kafkaRepl, int diskCount) {
        int k = 0;
        for (int i = 0; i < kafkaRepl; i++) {
            for (int j = 0; j < diskCount; j++) {
                LOGGER.info("Checking volume {} and size of storage {}", volumes.get(k).getMetadata().getName(),
                        volumes.get(k).getSpec().getResources().getRequests().get("storage").getAmount());
                assertThat(volumes.get(k).getSpec().getResources().getRequests().get("storage").getAmount(), is(diskSizes[i]));
                k++;
            }
        }
    }

    @Test
    @Tag(LOADBALANCER_SUPPORTED)
    void testRegenerateCertExternalAddressChange() throws InterruptedException {
        LOGGER.info("Creating kafka without external listener");
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3, 1).done();

        String brokerSecret = "my-cluster-kafka-brokers";

        Secret secretsWithoutExt = kubeClient().getSecret(brokerSecret);

        LOGGER.info("Editing kafka with external listener");
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3, 1)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewKafkaListenerExternalLoadBalancer()
                        .endKafkaListenerExternalLoadBalancer()
                    .endListeners()
                .endKafka()
            .endSpec()
            .done();

        LOGGER.info("Waiting until secrets will change");
        Thread.sleep(4000);

        Secret secretsWithExt = kubeClient().getSecret(brokerSecret);

        LOGGER.info("Checking secrets");
        assertThat(secretsWithExt.getData().get("my-cluster-kafka-0.crt"), is(not(secretsWithoutExt.getData().get("my-cluster-kafka-0.crt"))));
        assertThat(secretsWithExt.getData().get("my-cluster-kafka-0.key"), is(not(secretsWithoutExt.getData().get("my-cluster-kafka-0.key"))));
        assertThat(secretsWithExt.getData().get("my-cluster-kafka-1.crt"), is(not(secretsWithoutExt.getData().get("my-cluster-kafka-1.crt"))));
        assertThat(secretsWithExt.getData().get("my-cluster-kafka-1.key"), is(not(secretsWithoutExt.getData().get("my-cluster-kafka-1.key"))));
        assertThat(secretsWithExt.getData().get("my-cluster-kafka-2.crt"), is(not(secretsWithoutExt.getData().get("my-cluster-kafka-2.crt"))));
        assertThat(secretsWithExt.getData().get("my-cluster-kafka-2.key"), is(not(secretsWithoutExt.getData().get("my-cluster-kafka-2.key"))));
    }

    @Test
    @Tag(NODEPORT_SUPPORTED)
    void testLabelModificationDoesNotBreakCluster() throws Exception {
        Map<String, String> labels = new HashMap<>();
        String[] labelKeys = {"label-name-1", "label-name-2", ""};
        String[] labelValues = {"name-of-the-label-1", "name-of-the-label-2", ""};
        String brokerServiceName = "my-cluster-kafka-brokers";
        String configMapName = "my-cluster-kafka-config";

        labels.put(labelKeys[0], labelValues[0]);
        labels.put(labelKeys[1], labelValues[1]);

        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3, 1)
                .editSpec()
                    .editKafka()
                        .editListeners()
                            .withNewKafkaListenerExternalNodePort()
                                .withTls(false)
                            .endKafkaListenerExternalNodePort()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .editMetadata()
                    .withLabels(labels)
                .endMetadata()
                .done();

        Map<String, String> kafkaPods = StUtils.ssSnapshot(kafkaStatefulSetName(CLUSTER_NAME));

        LOGGER.info("Waiting for kafka stateful set labels changed {}", labels);
        StUtils.waitForKafkaStatefulSetLabelsChange(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), labels);

        LOGGER.info("Getting labels from stateful set resource");
        StatefulSet statefulSet = kubeClient().getStatefulSet(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));
        LOGGER.info("Verifying default labels in the Kafka CR");

        assertThat("Label exists in stateful set with concrete value",
                labelValues[0].equals(statefulSet.getSpec().getTemplate().getMetadata().getLabels().get(labelKeys[0])));
        assertThat("Label exists in stateful set with concrete value",
                labelValues[1].equals(statefulSet.getSpec().getTemplate().getMetadata().getLabels().get(labelKeys[1])));

        labelValues[0] = "new-name-of-the-label-1";
        labelValues[1] = "new-name-of-the-label-2";
        labelKeys[2] = "label-name-3";
        labelValues[2] = "name-of-the-label-3";
        LOGGER.info("Setting new values of labels from {} to {} | from {} to {} and adding one {} with value {}",
                "name-of-the-label-1", labelValues[0], "name-of-the-label-2", labelValues[1], labelKeys[2], labelValues[2]);

        LOGGER.info("Edit kafka labels in Kafka CR");
        replaceKafkaResource(CLUSTER_NAME, resource -> {
            resource.getMetadata().getLabels().put(labelKeys[0], labelValues[0]);
            resource.getMetadata().getLabels().put(labelKeys[1], labelValues[1]);
            resource.getMetadata().getLabels().put(labelKeys[2], labelValues[2]);
        });


        labels.put(labelKeys[0], labelValues[0]);
        labels.put(labelKeys[1], labelValues[1]);
        labels.put(labelKeys[2], labelValues[2]);

        LOGGER.info("Waiting for kafka service labels changed {}", labels);
        StUtils.waitForKafkaServiceLabelsChange(brokerServiceName, labels);

        LOGGER.info("Verifying kafka labels via services");
        Service service = kubeClient().getService(brokerServiceName);

        verifyPresentLabels(labels, service);

        LOGGER.info("Waiting for kafka config map labels changed {}", labels);
        StUtils.waitForKafkaConfigMapLabelsChange(configMapName, labels);

        LOGGER.info("Verifying kafka labels via config maps");
        ConfigMap configMap = kubeClient().getConfigMap(configMapName);

        verifyPresentLabels(labels, configMap);

        LOGGER.info("Waiting for kafka stateful set labels changed {}", labels);
        StUtils.waitForKafkaStatefulSetLabelsChange(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), labels);

        LOGGER.info("Verifying kafka labels via stateful set");
        statefulSet = kubeClient().getStatefulSet(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        verifyPresentLabels(labels, statefulSet);

        StUtils.waitForReconciliation(testClass, testName, NAMESPACE);
        StUtils.waitTillSsHasRolled(kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaPods);

        LOGGER.info("Verifying via kafka pods");
        labels = kubeClient().getPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0)).getMetadata().getLabels();

        assertThat("Label exists in kafka pods", labelValues[0].equals(labels.get(labelKeys[0])));
        assertThat("Label exists in kafka pods", labelValues[1].equals(labels.get(labelKeys[1])));
        assertThat("Label exists in kafka pods", labelValues[2].equals(labels.get(labelKeys[2])));

        LOGGER.info("Removing labels: {} -> {}, {} -> {}, {} -> {}", labelKeys[0], labels.get(labelKeys[0]),
                labelKeys[1], labels.get(labelKeys[1]), labelKeys[2], labels.get(labelKeys[2]));
        replaceKafkaResource(CLUSTER_NAME, resource -> {
            resource.getMetadata().getLabels().remove(labelKeys[0]);
            resource.getMetadata().getLabels().remove(labelKeys[1]);
            resource.getMetadata().getLabels().remove(labelKeys[2]);
        });

        labels.remove(labelKeys[0]);
        labels.remove(labelKeys[1]);
        labels.remove(labelKeys[2]);

        LOGGER.info("Waiting for kafka service labels deletion {}", labels.toString());
        StUtils.waitForKafkaServiceLabelsDeletion(brokerServiceName, labelKeys[0], labelKeys[1], labelKeys[2]);

        LOGGER.info("Verifying kafka labels via services");
        service = kubeClient().getService(brokerServiceName);

        verifyNullLabels(labelKeys, service);

        LOGGER.info("Verifying kafka labels via config maps");
        StUtils.waitForKafkaConfigMapLabelsDeletion(configMapName, labelKeys[0], labelKeys[1], labelKeys[2]);

        configMap = kubeClient().getConfigMap(configMapName);

        verifyNullLabels(labelKeys, configMap);

        LOGGER.info("Waiting for kafka stateful set labels changed {}", labels);
        String statefulSetName = kubeClient().getStatefulSet(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME)).getMetadata().getName();
        StUtils.waitForKafkaStatefulSetLabelsDeletion(statefulSetName, labelKeys[0], labelKeys[1], labelKeys[2]);

        statefulSet = kubeClient().getStatefulSet(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        LOGGER.info("Verifying kafka labels via stateful set");
        verifyNullLabels(labelKeys, statefulSet);

        StUtils.waitForReconciliation(testClass, testName, NAMESPACE);
        StUtils.waitTillSsHasRolled(kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaPods);

        LOGGER.info("Waiting for kafka pod labels deletion {}", labels.toString());
        StUtils.waitUntilKafkaPodLabelsDeletion(KafkaResources.kafkaPodName(CLUSTER_NAME, 0), labelKeys[0], labelKeys[1], labelKeys[2]);

        labels = kubeClient().getPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0)).getMetadata().getLabels();

        LOGGER.info("Verifying via kafka pods");
        verifyNullLabels(labelKeys, labels);

        waitForClusterAvailability(NAMESPACE);
    }

    void verifyPresentLabels(Map<String, String> labels, HasMetadata resources) {
        for (Map.Entry<String, String> label : labels.entrySet()) {
            assertThat("Label exists with concrete value in HasMetadata(Services, CM, SS) resources",
                    label.getValue().equals(resources.getMetadata().getLabels().get(label.getKey())));
        }
    }

    void verifyNullLabels(String[] labelKeys, Map<String, String> labels) {
        for (String labelKey : labelKeys) {
            assertThat(labels.get(labelKey), nullValue());
        }
    }

    void verifyNullLabels(String[] labelKeys, HasMetadata resources) {
        for (String labelKey : labelKeys) {
            assertThat(resources.getMetadata().getLabels().get(labelKey), nullValue());
        }
    }

    @Test
    @Tag(NODEPORT_SUPPORTED)
    void testAppDomainLabels() throws Exception {
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3, 1)
                .editSpec()
                    .editKafka()
                        .editListeners()
                            .withNewKafkaListenerExternalNodePort()
                                .withTls(false)
                            .endKafkaListenerExternalNodePort()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .done();

        Map<String, String> labels;

        LOGGER.info("---> PODS <---");

        List<Pod> pods = kubeClient().listPods().stream()
                .filter(pod -> pod.getMetadata().getName().startsWith(CLUSTER_NAME))
                .collect(Collectors.toList());

        for (Pod pod : pods) {
            LOGGER.info("Getting labels from {} pod", pod.getMetadata().getName());
            verifyAppLabels(pod.getMetadata().getLabels());
        }

        LOGGER.info("---> STATEFUL SETS <---");

        LOGGER.info("Getting labels from stateful set of kafka resource");
        labels = kubeClient().getStatefulSet(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME)).getMetadata().getLabels();

        verifyAppLabels(labels);

        LOGGER.info("Getting labels from stateful set of zookeeper resource");
        labels = kubeClient().getStatefulSet(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME)).getMetadata().getLabels();

        verifyAppLabels(labels);


        LOGGER.info("---> SERVICES <---");

        List<Service> services = kubeClient().listServices().stream()
                .filter(service -> service.getMetadata().getName().startsWith(CLUSTER_NAME))
                .collect(Collectors.toList());

        for (Service service : services) {
            LOGGER.info("Getting labels from {} service", service.getMetadata().getName());
            verifyAppLabels(service.getMetadata().getLabels());
        }

        LOGGER.info("---> SECRETS <---");

        List<Secret> secrets = kubeClient().listSecrets().stream()
                .filter(secret -> secret.getMetadata().getName().startsWith(CLUSTER_NAME) && secret.getType().equals("Opaque"))
                .collect(Collectors.toList());

        for (Secret secret : secrets) {
            LOGGER.info("Getting labels from {} secret", secret.getMetadata().getName());
            verifyAppLabelsForSecretsAndConfigMaps(secret.getMetadata().getLabels());
        }

        LOGGER.info("---> CONFIG MAPS <---");

        List<ConfigMap> configMaps = kubeClient().listConfigMaps();

        for (ConfigMap configMap : configMaps) {
            LOGGER.info("Getting labels from {} config map", configMap.getMetadata().getName());
            verifyAppLabelsForSecretsAndConfigMaps(configMap.getMetadata().getLabels());
        }

        waitForClusterAvailability(NAMESPACE);
    }

    void verifyAppLabels(Map<String, String> labels) {
        LOGGER.info("Verifying labels {}", labels);
        assertThat("Label strimzi.io/cluster is present", labels.containsKey("strimzi.io/cluster"));
        assertThat("Label strimzi.io/kind is present", labels.containsKey("strimzi.io/kind"));
        assertThat("Label strimzi.io/name is present", labels.containsKey("strimzi.io/name"));
    }

    void verifyAppLabelsForSecretsAndConfigMaps(Map<String, String> labels) {
        LOGGER.info("Verifying labels {}", labels);
        assertThat("Label strimzi.io/cluster is present", labels.containsKey("strimzi.io/cluster"));
        assertThat("Label strimzi.io/kind is present", labels.containsKey("strimzi.io/kind"));
    }

    @Test
    void testUOListeningOnlyUsersInSameCluster() {
        final String firstClusterName = "my-cluster-1";
        final String secondClusterName = "my-cluster-2";
        final String userName = "user-example";

        testMethodResources().kafkaEphemeral(firstClusterName, 3, 1).done();

        testMethodResources().kafkaEphemeral(secondClusterName, 3, 1).done();

        testMethodResources().tlsUser(firstClusterName, userName).done();
        StUtils.waitForSecretReady(userName);

        LOGGER.info("Verifying that user {} in cluster {} is created", userName, firstClusterName);
        String entityOperatorPodName = kubeClient().listPods("strimzi.io/name", KafkaResources.entityOperatorDeploymentName(firstClusterName)).get(0).getMetadata().getName();
        String uOLogs = kubeClient().logs(entityOperatorPodName, "user-operator");
        assertThat(uOLogs, containsString("User " + userName + " in namespace " + NAMESPACE + " was ADDED"));

        LOGGER.info("Verifying that user {} in cluster {} is not created", userName, secondClusterName);
        entityOperatorPodName = kubeClient().listPods("strimzi.io/name", KafkaResources.entityOperatorDeploymentName(secondClusterName)).get(0).getMetadata().getName();
        uOLogs = kubeClient().logs(entityOperatorPodName, "user-operator");
        assertThat(uOLogs, not(containsString("User " + userName + " in namespace " + NAMESPACE + " was ADDED")));

        LOGGER.info("Verifying that user belongs to {} cluster", firstClusterName);
        String kafkaUserResource = cmdKubeClient().getResourceAsYaml("kafkauser", userName);
        assertThat(kafkaUserResource, containsString("strimzi.io/cluster: " + firstClusterName));
    }

    @Test
    void testMessagesAreStoredInDisk() throws Exception {
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 1, 1)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewKafkaListenerExternalNodePort()
                            .withTls(false)
                        .endKafkaListenerExternalNodePort()
                    .endListeners()
                .endKafka()
            .endSpec()
            .done();

        Map<String, String> kafkaPodsSnapshot = StUtils.ssSnapshot(kafkaStatefulSetName(CLUSTER_NAME));

        testMethodResources().topic(CLUSTER_NAME, TEST_TOPIC_NAME, 1, 1).done();

        TestUtils.waitFor("Wait for kafka topic creation inside kafka pod", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> !cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0), "/bin/bash",
                        "-c", "cd /var/lib/kafka/data/kafka-log0; ls -1 | sed -n '/test/p'").out().equals(""));

        String topicNameInPod = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0), "/bin/bash",
                "-c", "cd /var/lib/kafka/data/kafka-log0; ls -1 | sed -n '/test/p'").out();

        LOGGER.info("This is topic in kafka broker itself {}", topicNameInPod);

        String commandToGetDataFromTopic =
                "cd /var/lib/kafka/data/kafka-log0/" + topicNameInPod + "/;cat 00000000000000000000.log";

        LOGGER.info("Executing command {} in {}", commandToGetDataFromTopic, KafkaResources.kafkaPodName(CLUSTER_NAME, 0));
        String topicData = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0),
                "/bin/bash", "-c", commandToGetDataFromTopic).out();

        LOGGER.info("Topic {} is present in kafka broker {} with no data", TEST_TOPIC_NAME, KafkaResources.kafkaPodName(CLUSTER_NAME, 0));
        assertThat("Topic contains data", topicData, isEmptyOrNullString());

        waitForClusterAvailability(NAMESPACE, TEST_TOPIC_NAME);

        LOGGER.info("Executing command {} in {}", commandToGetDataFromTopic, KafkaResources.kafkaPodName(CLUSTER_NAME, 0));
        topicData = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0), "/bin/bash", "-c",
                commandToGetDataFromTopic).out();

        assertThat("Topic has no data", topicData, notNullValue());

        List<Pod> kafkaPods = kubeClient().listPodsByPrefixInName(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        for (Pod kafkaPod : kafkaPods) {
            LOGGER.info("Deleting kafka pod {}", kafkaPod.getMetadata().getName());
            kubeClient().deletePod(kafkaPod);
        }

        LOGGER.info("Wait for kafka to rolling restart ...");
        StUtils.waitTillSsHasRolled(kafkaStatefulSetName(CLUSTER_NAME), 1, kafkaPodsSnapshot);

        LOGGER.info("Executing command {} in {}", commandToGetDataFromTopic, KafkaResources.kafkaPodName(CLUSTER_NAME, 0));
        topicData = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0), "/bin/bash", "-c",
                commandToGetDataFromTopic).out();

        assertThat("Topic has no data", topicData, notNullValue());
    }

    @Test
    void testConsumerOffsetFiles() throws Exception {
        Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put("offsets.topic.replication.factor", "3");
        kafkaConfig.put("offsets.topic.num.partitions", "100");

        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3, 1)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewKafkaListenerExternalNodePort()
                            .withTls(false)
                        .endKafkaListenerExternalNodePort()
                    .endListeners()
                    .withConfig(kafkaConfig)
                .endKafka()
            .endSpec()
            .done();

        testMethodResources().topic(CLUSTER_NAME, TEST_TOPIC_NAME, 3, 1).done();

        String commandToGetFiles =  "cd /var/lib/kafka/data/kafka-log0/;" +
                "ls -1 | sed -n \"s#__consumer_offsets-\\([0-9]*\\)#\\1#p\" | sort -V";

        LOGGER.info("Executing command {} in {}", commandToGetFiles, KafkaResources.kafkaPodName(CLUSTER_NAME, 0));
        String result = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0),
                "/bin/bash", "-c", commandToGetFiles).out();

        assertThat("Folder kafka-log0 has data in files", result.equals(""));

        waitForClusterAvailability(NAMESPACE, TEST_TOPIC_NAME);

        LOGGER.info("Executing command {} in {}", commandToGetFiles, KafkaResources.kafkaPodName(CLUSTER_NAME, 0));
        result = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0),
                "/bin/bash", "-c", commandToGetFiles).out();

        StringBuilder stringToMatch = new StringBuilder();

        for (int i = 0; i < 100; i++) {
            stringToMatch.append(i).append("\n");
        }

        assertThat("Folder kafka-log0 doesn't contains 100 files", result, containsString(stringToMatch.toString()));
    }

    @Test
    void testLabelsAndAnnotationforPVC() {
        final String labelAnnotationKey = "testKey";

        Map<String, String> pvcLabel = new HashMap<>();
        pvcLabel.put(labelAnnotationKey, "testValue");
        Map<String, String> pvcAnnotation = pvcLabel;

        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3, 1)
                .editSpec()
                    .editKafka()
                        .withNewTemplate()
                            .withNewPersistentVolumeClaim()
                                .withNewMetadata()
                                    .addToLabels(pvcLabel)
                                    .addToAnnotations(pvcAnnotation)
                                .endMetadata()
                            .endPersistentVolumeClaim()
                        .endTemplate()
                        .withStorage(new JbodStorageBuilder().withVolumes(
                            new PersistentClaimStorageBuilder()
                                    .withDeleteClaim(false)
                                    .withId(0)
                                    .withSize("20Gi")
                                    .build(),
                            new PersistentClaimStorageBuilder()
                                    .withDeleteClaim(true)
                                    .withId(1)
                                    .withSize("10Gi")
                                    .build())
                            .build())
                    .endKafka()
                    .editZookeeper()
                        .withNewTemplate()
                            .withNewPersistentVolumeClaim()
                                .withNewMetadata()
                                    .addToLabels(pvcLabel)
                                    .addToAnnotations(pvcAnnotation)
                                .endMetadata()
                            .endPersistentVolumeClaim()
                        .endTemplate()
                        .withNewPersistentClaimStorage()
                            .withDeleteClaim(false)
                            .withId(0)
                            .withSize("3Gi")
                        .endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .done();

        List<PersistentVolumeClaim> pvcs = kubeClient().listPersistentVolumeClaims();

        assertThat(7, is(kubeClient().listPersistentVolumeClaims().size()));

        for (PersistentVolumeClaim pvc : pvcs) {
            LOGGER.info("Verifying that PVC label {} - {} = {}", pvc.getMetadata().getName(), "testValue", pvc.getMetadata().getLabels().get("testKey"));

            assertThat("testValue", is(pvc.getMetadata().getLabels().get("testKey")));
            assertThat("testValue", is(pvc.getMetadata().getAnnotations().get("testKey")));
        }

        pvcLabel.put(labelAnnotationKey, "editedTestValue");
        pvcAnnotation.put(labelAnnotationKey, "editedTestValue");

        replaceKafkaResource(CLUSTER_NAME, kafka -> {
            LOGGER.info("Replacing kafka && zookeeper labels and annotaions from {} to {}", "testKey", "editedTestValue");
            kafka.getSpec().getKafka().getTemplate().getPersistentVolumeClaim().getMetadata().setLabels(pvcLabel);
            kafka.getSpec().getKafka().getTemplate().getPersistentVolumeClaim().getMetadata().setAnnotations(pvcAnnotation);

            kafka.getSpec().getZookeeper().getTemplate().getPersistentVolumeClaim().getMetadata().setLabels(pvcLabel);
            kafka.getSpec().getZookeeper().getTemplate().getPersistentVolumeClaim().getMetadata().setAnnotations(pvcAnnotation);
        });

        StUtils.waitUntilPVCLabelsChange(pvcLabel, labelAnnotationKey);
        StUtils.waitUntilPVCAnnotationChange(pvcAnnotation, labelAnnotationKey);

        pvcs = kubeClient().listPersistentVolumeClaims();

        assertThat(7, is(kubeClient().listPersistentVolumeClaims().size()));

        for (PersistentVolumeClaim pvc : pvcs) {
            LOGGER.info("Verifying replaced PVC label {} - {} = {}", pvc.getMetadata().getName(), "testValue", pvc.getMetadata().getLabels().get("testKey"));

            assertThat("editedTestValue", is(pvc.getMetadata().getLabels().get("testKey")));
            assertThat("editedTestValue", is(pvc.getMetadata().getAnnotations().get("testKey")));
        }
    }

    @BeforeEach
    void createTestResources() {
        createTestMethodResources();
    }

    @AfterEach
    void deleteTestResources() throws Exception {
        kubeClient().getClient().persistentVolumeClaims().inNamespace(NAMESPACE).delete();
        deleteTestMethodResources();
        waitForDeletion(Constants.TIMEOUT_TEARDOWN);
    }

    @BeforeAll
    void setupEnvironment() {
        LOGGER.info("Creating resources before the test class");
        prepareEnvForOperator(NAMESPACE);

        createTestClassResources();
        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        testClassResources().clusterOperator(NAMESPACE).done();
    }

    @Override
    protected void tearDownEnvironmentAfterEach() throws Exception {
        deleteTestMethodResources();
        waitForDeletion(Constants.TIMEOUT_TEARDOWN);
    }
}
