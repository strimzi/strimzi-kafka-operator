/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.HostAlias;
import io.fabric8.kubernetes.api.model.HostAliasBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaTopicList;
import io.strimzi.api.kafka.model.DoneableKafkaTopic;
import io.strimzi.api.kafka.model.EntityOperatorSpec;
import io.strimzi.api.kafka.model.EntityTopicOperatorSpec;
import io.strimzi.api.kafka.model.EntityUserOperatorSpec;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaClusterSpec;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.SystemProperty;
import io.strimzi.api.kafka.model.SystemPropertyBuilder;
import io.strimzi.api.kafka.model.ZookeeperClusterSpec;
import io.strimzi.api.kafka.model.listener.arraylistener.ArrayOrObjectKafkaListeners;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.JbodStorageBuilder;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageBuilder;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.OpenShiftOnly;
import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.ConfigMapUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PersistentVolumeClaimUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.ServiceUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.executor.ExecResult;
import io.strimzi.test.timemeasuring.Operation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static io.strimzi.api.kafka.model.KafkaResources.kafkaStatefulSetName;
import static io.strimzi.api.kafka.model.KafkaResources.zookeeperStatefulSetName;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.LOADBALANCER_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.utils.StUtils.configMap2Properties;
import static io.strimzi.systemtest.utils.StUtils.stringToProperties;
import static io.strimzi.test.TestUtils.fromYamlString;
import static io.strimzi.test.TestUtils.map;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

@Tag(REGRESSION)
@SuppressWarnings("checkstyle:ClassFanOutComplexity")
class KafkaST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(KafkaST.class);
    private static final String TEMPLATE_PATH = TestUtils.USER_PATH + "/../examples/templates/cluster-operator";
    public static final String NAMESPACE = "kafka-cluster-test";
    private static final String OPENSHIFT_CLUSTER_NAME = "openshift-my-cluster";

    @Test
    @OpenShiftOnly
    void testDeployKafkaClusterViaTemplate() {
        cluster.createCustomResources(TEMPLATE_PATH);
        String templateName = "strimzi-ephemeral";
        cmdKubeClient().createResourceAndApply(templateName, map("CLUSTER_NAME", OPENSHIFT_CLUSTER_NAME));

        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.zookeeperStatefulSetName(OPENSHIFT_CLUSTER_NAME), 3);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(OPENSHIFT_CLUSTER_NAME), 3);
        DeploymentUtils.waitForDeploymentAndPodsReady(KafkaResources.entityOperatorDeploymentName(OPENSHIFT_CLUSTER_NAME), 1);

        //Testing docker images
        testDockerImagesForKafkaCluster(OPENSHIFT_CLUSTER_NAME, NAMESPACE, 3, 3, false);

        //Testing labels
        verifyLabelsForKafkaCluster(OPENSHIFT_CLUSTER_NAME, templateName);

        LOGGER.info("Deleting Kafka cluster {} after test", OPENSHIFT_CLUSTER_NAME);
        cmdKubeClient().deleteByName("Kafka", OPENSHIFT_CLUSTER_NAME);

        //Wait for kafka deletion
        cmdKubeClient().waitForResourceDeletion(Kafka.RESOURCE_KIND, OPENSHIFT_CLUSTER_NAME);
        kubeClient().listPods().stream()
            .filter(p -> p.getMetadata().getName().startsWith(OPENSHIFT_CLUSTER_NAME))
            .forEach(p -> PodUtils.deletePodWithWait(p.getMetadata().getName()));

        StatefulSetUtils.waitForStatefulSetDeletion(KafkaResources.kafkaStatefulSetName(OPENSHIFT_CLUSTER_NAME));
        StatefulSetUtils.waitForStatefulSetDeletion(KafkaResources.zookeeperStatefulSetName(OPENSHIFT_CLUSTER_NAME));
        DeploymentUtils.waitForDeploymentDeletion(KafkaResources.entityOperatorDeploymentName(OPENSHIFT_CLUSTER_NAME));
    }

    @Test
    void testEODeletion() {
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3).done();

        // Get pod name to check termination process
        Pod pod = kubeClient().listPods().stream()
                .filter(p -> p.getMetadata().getName().startsWith(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME)))
                .findAny()
                .get();

        assertThat("Entity operator pod does not exist", pod, notNullValue());

        LOGGER.info("Setting entity operator to null");

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            kafka.getSpec().setEntityOperator(null);
        });

        // Wait when EO(UO + TO) will be removed
        DeploymentUtils.waitForDeploymentDeletion(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME));
        PodUtils.deletePodWithWait(pod.getMetadata().getName());

        LOGGER.info("Entity operator was deleted");
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

        KafkaResource.kafkaPersistent(CLUSTER_NAME, 2)
            .editSpec()
                .editKafka()
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
                    .endTemplate()
                .endKafka()
                .editZookeeper()
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

        Map<String, String> kafkaSnapshot = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));
        Map<String, String> zkSnapshot = StatefulSetUtils.ssSnapshot(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME));
        Map<String, String> eoPod = DeploymentUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME));

        LOGGER.info("Verify values before update");
        checkReadinessLivenessProbe(kafkaStatefulSetName(CLUSTER_NAME), "kafka", initialDelaySeconds, timeoutSeconds,
                periodSeconds, successThreshold, failureThreshold);
        checkKafkaConfiguration(kafkaStatefulSetName(CLUSTER_NAME), kafkaConfig, CLUSTER_NAME);
        checkSpecificVariablesInContainer(kafkaStatefulSetName(CLUSTER_NAME), "kafka", envVarGeneral);

        String kafkaConfiguration = kubeClient().getConfigMap(KafkaResources.kafkaMetricsAndLogConfigMapName(CLUSTER_NAME)).getData().get("server.config");
        assertThat(kafkaConfiguration, containsString("offsets.topic.replication.factor=1"));
        assertThat(kafkaConfiguration, containsString("transaction.state.log.replication.factor=1"));
        assertThat(kafkaConfiguration, containsString("default.replication.factor=1"));

        String kafkaConfigurationFromPod = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0), "cat", "/tmp/strimzi.properties").out();
        assertThat(kafkaConfigurationFromPod, containsString("offsets.topic.replication.factor=1"));
        assertThat(kafkaConfigurationFromPod, containsString("transaction.state.log.replication.factor=1"));
        assertThat(kafkaConfigurationFromPod, containsString("default.replication.factor=1"));

        LOGGER.info("Testing Zookeepers");
        checkReadinessLivenessProbe(zookeeperStatefulSetName(CLUSTER_NAME), "zookeeper", initialDelaySeconds, timeoutSeconds,
                periodSeconds, successThreshold, failureThreshold);
        checkComponentConfiguration(zookeeperStatefulSetName(CLUSTER_NAME), "zookeeper", "ZOOKEEPER_CONFIGURATION", zookeeperConfig);
        checkSpecificVariablesInContainer(zookeeperStatefulSetName(CLUSTER_NAME), "zookeeper", envVarGeneral);


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
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
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
            kafkaClusterSpec.getTemplate().getKafkaContainer().setEnv(StUtils.createContainerEnvVarsFromMap(envVarUpdated));
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
            zookeeperClusterSpec.getTemplate().getZookeeperContainer().setEnv(StUtils.createContainerEnvVarsFromMap(envVarUpdated));
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

        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME), 2, zkSnapshot);
        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 2, kafkaSnapshot);
        DeploymentUtils.waitTillDepHasRolled(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), 1, eoPod);
        KafkaUtils.waitForKafkaReady(CLUSTER_NAME);

        LOGGER.info("Verify values after update");
        checkReadinessLivenessProbe(kafkaStatefulSetName(CLUSTER_NAME), "kafka", updatedInitialDelaySeconds, updatedTimeoutSeconds,
                updatedPeriodSeconds, successThreshold, updatedFailureThreshold);
        checkKafkaConfiguration(kafkaStatefulSetName(CLUSTER_NAME), updatedKafkaConfig, CLUSTER_NAME);
        checkSpecificVariablesInContainer(kafkaStatefulSetName(CLUSTER_NAME), "kafka", envVarUpdated);

        kafkaConfiguration = kubeClient().getConfigMap(KafkaResources.kafkaMetricsAndLogConfigMapName(CLUSTER_NAME)).getData().get("server.config");
        assertThat(kafkaConfiguration, containsString("offsets.topic.replication.factor=2"));
        assertThat(kafkaConfiguration, containsString("transaction.state.log.replication.factor=2"));
        assertThat(kafkaConfiguration, containsString("default.replication.factor=2"));

        kafkaConfigurationFromPod = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0), "cat", "/tmp/strimzi.properties").out();
        assertThat(kafkaConfigurationFromPod, containsString("offsets.topic.replication.factor=2"));
        assertThat(kafkaConfigurationFromPod, containsString("transaction.state.log.replication.factor=2"));
        assertThat(kafkaConfigurationFromPod, containsString("default.replication.factor=2"));

        LOGGER.info("Testing Zookeepers");
        checkReadinessLivenessProbe(zookeeperStatefulSetName(CLUSTER_NAME), "zookeeper", updatedInitialDelaySeconds, updatedTimeoutSeconds,
                updatedPeriodSeconds, successThreshold, updatedFailureThreshold);
        checkComponentConfiguration(zookeeperStatefulSetName(CLUSTER_NAME), "zookeeper", "ZOOKEEPER_CONFIGURATION", updatedZookeeperConfig);
        checkSpecificVariablesInContainer(zookeeperStatefulSetName(CLUSTER_NAME), "zookeeper", envVarUpdated);

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

    @Test
    void testJvmAndResources() {
        ArrayList<SystemProperty> javaSystemProps = new ArrayList<>();
        javaSystemProps.add(new SystemPropertyBuilder().withName("javax.net.debug")
                .withValue("verbose").build());

        Map<String, String> jvmOptionsXX = new HashMap<>();
        jvmOptionsXX.put("UseG1GC", "true");

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 1, 1)
            .editSpec()
                .editKafka()
                    .withResources(new ResourceRequirementsBuilder()
                            .addToLimits("memory", new Quantity("1.5Gi"))
                            .addToLimits("cpu", new Quantity("1"))
                            .addToRequests("memory", new Quantity("1Gi"))
                            .addToRequests("cpu", new Quantity("50m"))
                            .build())
                    .withNewJvmOptions()
                        .withXmx("1g")
                        .withXms("512m")
                        .withXx(jvmOptionsXX)
                    .endJvmOptions()
                .endKafka()
                .editZookeeper()
                    .withResources(
                        new ResourceRequirementsBuilder()
                            .addToLimits("memory", new Quantity("1G"))
                            .addToLimits("cpu", new Quantity("0.5"))
                            .addToRequests("memory", new Quantity("0.5G"))
                            .addToRequests("cpu", new Quantity("25m"))
                            .build())
                    .withNewJvmOptions()
                        .withXmx("1G")
                        .withXms("512M")
                        .withXx(jvmOptionsXX)
                    .endJvmOptions()
                .endZookeeper()
                .withNewEntityOperator()
                    .withNewTopicOperator()
                        .withResources(
                            new ResourceRequirementsBuilder()
                                .addToLimits("memory", new Quantity("1024Mi"))
                                .addToLimits("cpu", new Quantity("500m"))
                                .addToRequests("memory", new Quantity("384Mi"))
                                .addToRequests("cpu", new Quantity("0.025"))
                                .build())
                        .withNewJvmOptions()
                            .withXmx("2G")
                            .withXms("1024M")
                            .withJavaSystemProperties(javaSystemProps)
                        .endJvmOptions()
                    .endTopicOperator()
                    .withNewUserOperator()
                        .withResources(
                            new ResourceRequirementsBuilder()
                                .addToLimits("memory", new Quantity("512M"))
                                .addToLimits("cpu", new Quantity("300m"))
                                .addToRequests("memory", new Quantity("256M"))
                                .addToRequests("cpu", new Quantity("30m"))
                                .build())
                        .withNewJvmOptions()
                            .withXmx("1G")
                            .withXms("512M")
                            .withJavaSystemProperties(javaSystemProps)
                        .endJvmOptions()
                    .endUserOperator()
                .endEntityOperator()
            .endSpec().done();

        // Make snapshots for Kafka cluster to meke sure that there is no rolling update after CO reconciliation
        String zkStsName = KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME);
        String kafkaStsName = kafkaStatefulSetName(CLUSTER_NAME);
        String eoDepName = KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME);
        Map<String, String> zkPods = StatefulSetUtils.ssSnapshot(zkStsName);
        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(kafkaStsName);
        Map<String, String> eoPods = DeploymentUtils.depSnapshot(eoDepName);

        assertResources(cmdKubeClient().namespace(), KafkaResources.kafkaPodName(CLUSTER_NAME, 0), "kafka",
                "1536Mi", "1", "1Gi", "50m");
        assertExpectedJavaOpts(KafkaResources.kafkaPodName(CLUSTER_NAME, 0), "kafka",
                "-Xmx1g", "-Xms512m", "-XX:+UseG1GC");

        assertResources(cmdKubeClient().namespace(), KafkaResources.zookeeperPodName(CLUSTER_NAME, 0), "zookeeper",
                "1G", "500m", "500M", "25m");
        assertExpectedJavaOpts(KafkaResources.zookeeperPodName(CLUSTER_NAME, 0), "zookeeper",
                "-Xmx1G", "-Xms512M", "-XX:+UseG1GC");

        Optional<Pod> pod = kubeClient().listPods()
                .stream().filter(p -> p.getMetadata().getName().startsWith(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME)))
                .findFirst();
        assertThat("EO pod does not exist", pod.isPresent(), is(true));

        assertResources(cmdKubeClient().namespace(), pod.get().getMetadata().getName(), "topic-operator",
                "1Gi", "500m", "384Mi", "25m");
        assertResources(cmdKubeClient().namespace(), pod.get().getMetadata().getName(), "user-operator",
                "512M", "300m", "256M", "30m");
        assertExpectedJavaOpts(pod.get().getMetadata().getName(), "topic-operator",
                "-Xmx2G", "-Xms1024M", null);
        assertExpectedJavaOpts(pod.get().getMetadata().getName(), "user-operator",
                "-Xmx1G", "-Xms512M", null);

        String eoPod = eoPods.keySet().toArray()[0].toString();
        kubeClient().getPod(eoPod).getSpec().getContainers().forEach(container -> {
            if (!container.getName().equals("tls-sidecar")) {
                LOGGER.info("Check if -D java options are present in {}", container.getName());

                String javaSystemProp = container.getEnv().stream().filter(envVar ->
                    envVar.getName().equals("STRIMZI_JAVA_SYSTEM_PROPERTIES")).findFirst().get().getValue();
                String javaOpts = container.getEnv().stream().filter(envVar ->
                    envVar.getName().equals("STRIMZI_JAVA_OPTS")).findFirst().get().getValue();

                assertThat(javaSystemProp, is("-Djavax.net.debug=verbose"));

                if (container.getName().equals("topic-operator")) {
                    assertThat(javaOpts, is("-Xms1024M -Xmx2G"));
                }

                if (container.getName().equals("user-operator")) {
                    assertThat(javaOpts, is("-Xms512M -Xmx1G"));
                }
            }
        });

        LOGGER.info("Checking no rolling update for Kafka cluster");
        StatefulSetUtils.waitForNoRollingUpdate(zkStsName, zkPods);
        StatefulSetUtils.waitForNoRollingUpdate(kafkaStsName, kafkaPods);
        DeploymentUtils.waitForNoRollingUpdate(eoDepName, eoPods);
    }

    @Test
    void testForTopicOperator() throws InterruptedException {
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3).done();
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();
        String cliTopicName = "topic-from-cli";

        //Creating topics for testing
        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();

        KafkaTopicUtils.waitForKafkaTopicReady(topicName);

        assertThat(KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).withName(topicName).get().getMetadata().getName(), is(topicName));
        assertThat(KafkaCmdClient.listTopicsUsingPodCli(CLUSTER_NAME, 0), hasItem(topicName));

        KafkaCmdClient.createTopicUsingPodCli(CLUSTER_NAME, 0, cliTopicName, 1, 1);
        assertThat(KafkaCmdClient.listTopicsUsingPodCli(CLUSTER_NAME, 0), hasItems(topicName, cliTopicName));
        assertThat(cmdKubeClient().list(KafkaTopic.RESOURCE_KIND), hasItems(cliTopicName, topicName));

        //Updating first topic using pod CLI
        KafkaCmdClient.updateTopicPartitionsCountUsingPodCli(CLUSTER_NAME, 0, topicName, 2);

        KafkaUtils.waitForKafkaReady(CLUSTER_NAME);

        assertThat(KafkaCmdClient.describeTopicUsingPodCli(CLUSTER_NAME, 0, topicName),
                hasItems("PartitionCount:2"));
        KafkaTopic testTopic = fromYamlString(cmdKubeClient().get(KafkaTopic.RESOURCE_KIND, topicName), KafkaTopic.class);
        assertThat(testTopic, is(CoreMatchers.notNullValue()));
        assertThat(testTopic.getSpec(), is(CoreMatchers.notNullValue()));
        assertThat(testTopic.getSpec().getPartitions(), is(Integer.valueOf(2)));

        //Updating second topic via KafkaTopic update
        KafkaTopicResource.replaceTopicResource(cliTopicName, topic -> {
            topic.getSpec().setPartitions(2);
        });

        KafkaUtils.waitForKafkaReady(CLUSTER_NAME);

        assertThat(KafkaCmdClient.describeTopicUsingPodCli(CLUSTER_NAME, 0, cliTopicName),
                hasItems("PartitionCount:2"));
        testTopic = fromYamlString(cmdKubeClient().get(KafkaTopic.RESOURCE_KIND, cliTopicName), KafkaTopic.class);
        assertThat(testTopic, is(CoreMatchers.notNullValue()));
        assertThat(testTopic.getSpec(), is(CoreMatchers.notNullValue()));
        assertThat(testTopic.getSpec().getPartitions(), is(Integer.valueOf(2)));

        //Deleting first topic by deletion of CM
        cmdKubeClient().deleteByName(KafkaTopic.RESOURCE_KIND, cliTopicName);

        //Deleting another topic using pod CLI
        KafkaCmdClient.deleteTopicUsingPodCli(CLUSTER_NAME, 0, topicName);
        KafkaTopicUtils.waitForKafkaTopicDeletion(topicName);

        //Checking all topics were deleted
        Thread.sleep(Constants.TIMEOUT_TEARDOWN);
        List<String> topics = KafkaCmdClient.listTopicsUsingPodCli(CLUSTER_NAME, 0);
        assertThat(topics, not(hasItems(topicName)));
        assertThat(topics, not(hasItems(cliTopicName)));
    }

    @Test
    void testRemoveTopicOperatorFromEntityOperator() {
        LOGGER.info("Deploying Kafka cluster {}", CLUSTER_NAME);
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3).done();
        String eoPodName = kubeClient().listPodsByPrefixInName(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME))
            .get(0).getMetadata().getName();

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getEntityOperator().setTopicOperator(null));
        //Waiting when EO pod will be recreated without TO
        PodUtils.deletePodWithWait(eoPodName);
        DeploymentUtils.waitForDeploymentAndPodsReady(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), 1);
        PodUtils.waitUntilPodContainersCount(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), 2);

        //Checking that TO was removed
        kubeClient().listPodsByPrefixInName(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME)).forEach(pod -> {
            pod.getSpec().getContainers().forEach(container -> {
                assertThat(container.getName(), not(containsString("topic-operator")));
            });
        });

        eoPodName = kubeClient().listPodsByPrefixInName(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME))
                .get(0).getMetadata().getName();

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getEntityOperator().setTopicOperator(new EntityTopicOperatorSpec()));
        //Waiting when EO pod will be recreated with TO
        PodUtils.deletePodWithWait(eoPodName);
        DeploymentUtils.waitForDeploymentAndPodsReady(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), 1);

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
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_DEPLOYMENT));
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3).done();
        String eoPodName = kubeClient().listPodsByPrefixInName(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME))
                .get(0).getMetadata().getName();

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getEntityOperator().setUserOperator(null));

        //Waiting when EO pod will be recreated without UO
        PodUtils.deletePodWithWait(eoPodName);
        DeploymentUtils.waitForDeploymentAndPodsReady(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), 1);
        PodUtils.waitUntilPodContainersCount(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), 2);

        //Checking that UO was removed
        kubeClient().listPodsByPrefixInName(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME)).forEach(pod -> {
            pod.getSpec().getContainers().forEach(container -> {
                assertThat(container.getName(), not(containsString("user-operator")));
            });
        });

        eoPodName = kubeClient().listPodsByPrefixInName(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME))
                .get(0).getMetadata().getName();

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getEntityOperator().setUserOperator(new EntityUserOperatorSpec()));
        //Waiting when EO pod will be recreated with UO
        PodUtils.deletePodWithWait(eoPodName);
        DeploymentUtils.waitForDeploymentAndPodsReady(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), 1);

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

        timeMeasuringSystem.stopOperation(timeMeasuringSystem.getOperationID());
        assertNoCoErrorsLogged(timeMeasuringSystem.getDurationInSeconds(testClass, testName, timeMeasuringSystem.getOperationID()));
    }

    @Test
    void testRemoveUserAndTopicOperatorsFromEntityOperator() {
        LOGGER.info("Deploying Kafka cluster {}", CLUSTER_NAME);
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_DEPLOYMENT));
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3).done();

        String eoDeploymentName = KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME);

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec().getEntityOperator().setTopicOperator(null);
            k.getSpec().getEntityOperator().setUserOperator(null);
        });

        PodUtils.waitUntilPodStabilityReplicasCount(eoDeploymentName, 0);

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec().getEntityOperator().setTopicOperator(new EntityTopicOperatorSpec());
            k.getSpec().getEntityOperator().setUserOperator(new EntityUserOperatorSpec());
        });
        DeploymentUtils.waitForDeploymentReady(eoDeploymentName);

        //Checking that EO was created
        kubeClient().listPodsByPrefixInName(eoDeploymentName).forEach(pod -> {
            pod.getSpec().getContainers().forEach(container -> {
                assertThat(container.getName(), anyOf(
                    containsString("topic-operator"),
                    containsString("user-operator"),
                    containsString("tls-sidecar"))
                );
            });
        });

        timeMeasuringSystem.stopOperation(timeMeasuringSystem.getOperationID());
        assertNoCoErrorsLogged(timeMeasuringSystem.getDurationInSeconds(testClass, testName, timeMeasuringSystem.getOperationID()));
    }

    @Test
    void testEntityOperatorWithoutTopicOperator() {
        LOGGER.info("Deploying Kafka cluster without TO in EO");
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_DEPLOYMENT));
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3)
            .editSpec()
                .withNewEntityOperator()
                    .withNewUserOperator()
                    .endUserOperator()
                .endEntityOperator()
            .endSpec()
            .done();

        timeMeasuringSystem.stopOperation(timeMeasuringSystem.getOperationID());
        assertNoCoErrorsLogged(timeMeasuringSystem.getDurationInSeconds(testClass, testName, timeMeasuringSystem.getOperationID()));

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
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_DEPLOYMENT));
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3)
            .editSpec()
                .withNewEntityOperator()
                    .withNewTopicOperator()
                    .endTopicOperator()
                .endEntityOperator()
            .endSpec()
            .done();

        timeMeasuringSystem.stopOperation(timeMeasuringSystem.getOperationID());
        assertNoCoErrorsLogged(timeMeasuringSystem.getDurationInSeconds(testClass, testName, timeMeasuringSystem.getOperationID()));

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
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_DEPLOYMENT));
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3)
            .editSpec()
                .withNewEntityOperator()
                .endEntityOperator()
            .endSpec()
            .done();

        timeMeasuringSystem.stopOperation(timeMeasuringSystem.getOperationID());
        assertNoCoErrorsLogged(timeMeasuringSystem.getDurationInSeconds(testClass, testName, timeMeasuringSystem.getOperationID()));

        //Checking that EO was not deployed
        assertThat("EO should not be deployed", kubeClient().listPodsByPrefixInName(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME)).size(), is(0));
    }

    @Test
    void testTopicWithoutLabels() {
        // Negative scenario: creating topic without any labels and make sure that TO can't handle this topic
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3).done();

        // Creating topic without any label
        KafkaTopicResource.topicWithoutWait(KafkaTopicResource.defaultTopic(CLUSTER_NAME, "topic-without-labels", 1, 1, 1)
            .editMetadata()
                .withLabels(null)
            .endMetadata()
            .build());

        // Checking that resource was created
        assertThat(cmdKubeClient().list("kafkatopic"), hasItems("topic-without-labels"));
        // Checking that TO didn't handle new topic and zk pods don't contain new topic
        assertThat(KafkaCmdClient.listTopicsUsingPodCli(CLUSTER_NAME, 0), not(hasItems("topic-without-labels")));

        // Checking TO logs
        String tOPodName = cmdKubeClient().listResourcesByLabel("pod", Labels.STRIMZI_NAME_LABEL + "=my-cluster-entity-operator").get(0);
        String tOlogs = kubeClient().logs(tOPodName, "topic-operator");
        assertThat(tOlogs, not(containsString("Created topic 'topic-without-labels'")));

        //Deleting topic
        cmdKubeClient().deleteByName("kafkatopic", "topic-without-labels");
        KafkaTopicUtils.waitForKafkaTopicDeletion("topic-without-labels");

        //Checking all topics were deleted
        List<String> topics = KafkaCmdClient.listTopicsUsingPodCli(CLUSTER_NAME, 0);
        assertThat(topics, not(hasItems("topic-without-labels")));
    }

    @Test
    @Tag(REGRESSION)
    void testKafkaJBODDeleteClaimsTrueFalse() {
        int kafkaReplicas = 2;
        String diskSizeGi = "10";

        JbodStorage jbodStorage = new JbodStorageBuilder().withVolumes(
            new PersistentClaimStorageBuilder().withDeleteClaim(true).withId(0).withSize(diskSizeGi + "Gi").build(),
            new PersistentClaimStorageBuilder().withDeleteClaim(false).withId(1).withSize(diskSizeGi + "Gi").build()).build();

        KafkaResource.kafkaJBOD(CLUSTER_NAME, kafkaReplicas, jbodStorage).done();
        // kafka cluster already deployed
        verifyVolumeNamesAndLabels(kafkaReplicas, 2, diskSizeGi);

        LOGGER.info("Deleting cluster");
        cmdKubeClient().deleteByName("kafka", CLUSTER_NAME);

        LOGGER.info("Waiting for PVC deletion");
        PersistentVolumeClaimUtils.waitForPVCDeletion(kafkaReplicas, jbodStorage, CLUSTER_NAME);

        LOGGER.info("Waiting for Kafka pods deletion");
        verifyPVCDeletion(kafkaReplicas, jbodStorage);
    }

    @Test
    void testKafkaJBODDeleteClaimsTrue() {
        int kafkaReplicas = 2;
        String diskSizeGi = "10";

        JbodStorage jbodStorage = new JbodStorageBuilder().withVolumes(
            new PersistentClaimStorageBuilder().withDeleteClaim(true).withId(0).withSize(diskSizeGi + "Gi").build(),
            new PersistentClaimStorageBuilder().withDeleteClaim(true).withId(1).withSize(diskSizeGi + "Gi").build()).build();

        KafkaResource.kafkaJBOD(CLUSTER_NAME, kafkaReplicas, jbodStorage).done();
        // kafka cluster already deployed
        verifyVolumeNamesAndLabels(kafkaReplicas, 2, diskSizeGi);

        LOGGER.info("Deleting cluster");
        cmdKubeClient().deleteByName("kafka", CLUSTER_NAME);

        LOGGER.info("Waiting for PVC deletion");
        PersistentVolumeClaimUtils.waitForPVCDeletion(kafkaReplicas, jbodStorage, CLUSTER_NAME);

        LOGGER.info("Waiting for Kafka pods deletion");
        verifyPVCDeletion(kafkaReplicas, jbodStorage);
    }

    @Test
    void testKafkaJBODDeleteClaimsFalse() {
        int kafkaReplicas = 2;
        String diskSizeGi = "10";

        JbodStorage jbodStorage = new JbodStorageBuilder().withVolumes(
            new PersistentClaimStorageBuilder().withDeleteClaim(false).withId(0).withSize(diskSizeGi + "Gi").build(),
            new PersistentClaimStorageBuilder().withDeleteClaim(false).withId(1).withSize(diskSizeGi + "Gi").build()).build();

        KafkaResource.kafkaJBOD(CLUSTER_NAME, kafkaReplicas, jbodStorage).done();
        // kafka cluster already deployed
        verifyVolumeNamesAndLabels(kafkaReplicas, 2, diskSizeGi);

        LOGGER.info("Deleting cluster");
        cmdKubeClient().deleteByName("kafka", CLUSTER_NAME);

        LOGGER.info("Waiting for PVC deletion");
        PersistentVolumeClaimUtils.waitForPVCDeletion(kafkaReplicas, jbodStorage, CLUSTER_NAME);

        LOGGER.info("Waiting for Kafka pods deletion");
        verifyPVCDeletion(kafkaReplicas, jbodStorage);
    }

    @Test
    @Tag(INTERNAL_CLIENTS_USED)
    void testPersistentStorageSize() {
        String[] diskSizes = {"70Gi", "20Gi"};
        int kafkaRepl = 2;
        int diskCount = 2;

        JbodStorage jbodStorage =  new JbodStorageBuilder()
                .withVolumes(
                        new PersistentClaimStorageBuilder().withDeleteClaim(false).withId(0).withSize(diskSizes[0]).build(),
                        new PersistentClaimStorageBuilder().withDeleteClaim(false).withId(1).withSize(diskSizes[1]).build()
                ).build();

        KafkaResource.kafkaPersistent(CLUSTER_NAME, kafkaRepl)
            .editSpec()
                .editKafka()
                    .withStorage(jbodStorage)
                .endKafka()
                .editZookeeper().
                    withReplicas(1)
                .endZookeeper()
            .endSpec()
            .done();

        KafkaTopicResource.topic(CLUSTER_NAME, TEST_TOPIC_NAME).done();

        KafkaClientsResource.deployKafkaClients(false, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).done();

        List<PersistentVolumeClaim> volumes = kubeClient().listPersistentVolumeClaims();

        checkStorageSizeForVolumes(volumes, diskSizes, kafkaRepl, diskCount);

        String kafkaClientsPodName = kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(TEST_TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        LOGGER.info("Checking produced and consumed messages to pod:{}", kafkaClientsPodName);

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );
    }

    @Test
    @Tag(LOADBALANCER_SUPPORTED)
    void testRegenerateCertExternalAddressChange() {
        LOGGER.info("Creating kafka without external listener");
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1).done();

        String brokerSecret = "my-cluster-kafka-brokers";

        Secret secretsWithoutExt = kubeClient().getSecret(brokerSecret);

        LOGGER.info("Editing kafka with external listener");
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            ArrayOrObjectKafkaListeners lst = new ArrayOrObjectKafkaListeners(asList(
                    new GenericKafkaListenerBuilder()
                            .withName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
                            .withPort(9092)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(false)
                            .build(),
                    new GenericKafkaListenerBuilder()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9094)
                            .withType(KafkaListenerType.LOADBALANCER)
                            .withTls(true)
                            .build()
            ));
            kafka.getSpec().getKafka().setListeners(lst);
        });

        StatefulSetUtils.waitTillSsHasRolled(kafkaStatefulSetName(CLUSTER_NAME), 3, StatefulSetUtils.ssSnapshot(kafkaStatefulSetName(CLUSTER_NAME)));

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
    @Tag(INTERNAL_CLIENTS_USED)
    void testLabelModificationDoesNotBreakCluster() {
        Map<String, String> labels = new HashMap<>();
        String[] labelKeys = {"label-name-1", "label-name-2", ""};
        String[] labelValues = {"name-of-the-label-1", "name-of-the-label-2", ""};
        String brokerServiceName = "my-cluster-kafka-brokers";
        String configMapName = "my-cluster-kafka-config";

        labels.put(labelKeys[0], labelValues[0]);
        labels.put(labelKeys[1], labelValues[1]);

        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3, 1)
                .editMetadata()
                    .withLabels(labels)
                .endMetadata()
                .done();

        KafkaTopicResource.topic(CLUSTER_NAME, TEST_TOPIC_NAME).done();

        KafkaClientsResource.deployKafkaClients(false, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).done();

        String kafkaClientsPodName = kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(TEST_TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        Map<String, String> kafkaPods = StatefulSetUtils.ssSnapshot(kafkaStatefulSetName(CLUSTER_NAME));

        LOGGER.info("Waiting for kafka stateful set labels changed {}", labels);
        StatefulSetUtils.waitForStatefulSetLabelsChange(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), labels);

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
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, resource -> {
            resource.getMetadata().getLabels().put(labelKeys[0], labelValues[0]);
            resource.getMetadata().getLabels().put(labelKeys[1], labelValues[1]);
            resource.getMetadata().getLabels().put(labelKeys[2], labelValues[2]);
        });

        labels.put(labelKeys[0], labelValues[0]);
        labels.put(labelKeys[1], labelValues[1]);
        labels.put(labelKeys[2], labelValues[2]);

        LOGGER.info("Waiting for kafka service labels changed {}", labels);
        ServiceUtils.waitForServiceLabelsChange(brokerServiceName, labels);

        LOGGER.info("Verifying kafka labels via services");
        Service service = kubeClient().getService(brokerServiceName);

        verifyPresentLabels(labels, service);

        LOGGER.info("Waiting for kafka config map labels changed {}", labels);
        ConfigMapUtils.waitForConfigMapLabelsChange(configMapName, labels);

        LOGGER.info("Verifying kafka labels via config maps");
        ConfigMap configMap = kubeClient().getConfigMap(configMapName);

        verifyPresentLabels(labels, configMap);

        LOGGER.info("Waiting for kafka stateful set labels changed {}", labels);
        StatefulSetUtils.waitForStatefulSetLabelsChange(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), labels);

        LOGGER.info("Verifying kafka labels via stateful set");
        statefulSet = kubeClient().getStatefulSet(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        verifyPresentLabels(labels, statefulSet);

        StatefulSetUtils.waitTillSsHasRolled(kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaPods);

        LOGGER.info("Verifying via kafka pods");
        labels = kubeClient().getPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0)).getMetadata().getLabels();

        assertThat("Label exists in kafka pods", labelValues[0].equals(labels.get(labelKeys[0])));
        assertThat("Label exists in kafka pods", labelValues[1].equals(labels.get(labelKeys[1])));
        assertThat("Label exists in kafka pods", labelValues[2].equals(labels.get(labelKeys[2])));

        LOGGER.info("Removing labels: {} -> {}, {} -> {}, {} -> {}", labelKeys[0], labels.get(labelKeys[0]),
            labelKeys[1], labels.get(labelKeys[1]), labelKeys[2], labels.get(labelKeys[2]));
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, resource -> {
            resource.getMetadata().getLabels().remove(labelKeys[0]);
            resource.getMetadata().getLabels().remove(labelKeys[1]);
            resource.getMetadata().getLabels().remove(labelKeys[2]);
        });

        labels.remove(labelKeys[0]);
        labels.remove(labelKeys[1]);
        labels.remove(labelKeys[2]);

        LOGGER.info("Waiting for kafka service labels deletion {}", labels.toString());
        ServiceUtils.waitForServiceLabelsDeletion(brokerServiceName, labelKeys[0], labelKeys[1], labelKeys[2]);

        LOGGER.info("Verifying kafka labels via services");
        service = kubeClient().getService(brokerServiceName);

        verifyNullLabels(labelKeys, service);

        LOGGER.info("Verifying kafka labels via config maps");
        ConfigMapUtils.waitForConfigMapLabelsDeletion(configMapName, labelKeys[0], labelKeys[1], labelKeys[2]);

        configMap = kubeClient().getConfigMap(configMapName);

        verifyNullLabels(labelKeys, configMap);

        LOGGER.info("Waiting for kafka stateful set labels changed {}", labels);
        String statefulSetName = kubeClient().getStatefulSet(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME)).getMetadata().getName();
        StatefulSetUtils.waitForStatefulSetLabelsDeletion(statefulSetName, labelKeys[0], labelKeys[1], labelKeys[2]);

        statefulSet = kubeClient().getStatefulSet(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        LOGGER.info("Verifying kafka labels via stateful set");
        verifyNullLabels(labelKeys, statefulSet);

        StatefulSetUtils.waitTillSsHasRolled(kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaPods);

        LOGGER.info("Waiting for kafka pod labels deletion {}", labels.toString());
        PodUtils.waitUntilPodLabelsDeletion(KafkaResources.kafkaPodName(CLUSTER_NAME, 0), labelKeys[0], labelKeys[1], labelKeys[2]);

        labels = kubeClient().getPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0)).getMetadata().getLabels();

        LOGGER.info("Verifying via kafka pods");
        verifyNullLabels(labelKeys, labels);

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );
    }

    @Test
    @Tag(INTERNAL_CLIENTS_USED)
    void testAppDomainLabels() {
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1).done();

        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();

        KafkaClientsResource.deployKafkaClients(false, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).done();

        final String kafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        Map<String, String> labels;

        LOGGER.info("---> PODS <---");

        List<Pod> pods = kubeClient().listPods().stream()
                .filter(pod -> pod.getMetadata().getName().startsWith(CLUSTER_NAME))
                .filter(pod -> !pod.getMetadata().getName().startsWith(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS))
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

        List<ConfigMap> configMaps = kubeClient().listConfigMaps(CLUSTER_NAME);

        for (ConfigMap configMap : configMaps) {
            LOGGER.info("Getting labels from {} config map", configMap.getMetadata().getName());
            verifyAppLabelsForSecretsAndConfigMaps(configMap.getMetadata().getLabels());
        }

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );
    }

    @Test
    void testUOListeningOnlyUsersInSameCluster() {
        final String firstClusterName = "my-cluster-1";
        final String secondClusterName = "my-cluster-2";
        final String userName = KafkaUserUtils.generateRandomNameOfKafkaUser();

        KafkaResource.kafkaEphemeral(firstClusterName, 3, 1).done();

        KafkaResource.kafkaEphemeral(secondClusterName, 3, 1).done();

        KafkaUserResource.tlsUser(firstClusterName, userName).done();

        LOGGER.info("Verifying that user {} in cluster {} is created", userName, firstClusterName);
        String entityOperatorPodName = kubeClient().listPods(Labels.STRIMZI_NAME_LABEL, KafkaResources.entityOperatorDeploymentName(firstClusterName)).get(0).getMetadata().getName();
        String uOLogs = kubeClient().logs(entityOperatorPodName, "user-operator");
        assertThat(uOLogs, containsString("User " + userName + " in namespace " + NAMESPACE + " was ADDED"));

        LOGGER.info("Verifying that user {} in cluster {} is not created", userName, secondClusterName);
        entityOperatorPodName = kubeClient().listPods(Labels.STRIMZI_NAME_LABEL, KafkaResources.entityOperatorDeploymentName(secondClusterName)).get(0).getMetadata().getName();
        uOLogs = kubeClient().logs(entityOperatorPodName, "user-operator");
        assertThat(uOLogs, not(containsString("User " + userName + " in namespace " + NAMESPACE + " was ADDED")));

        LOGGER.info("Verifying that user belongs to {} cluster", firstClusterName);
        String kafkaUserResource = cmdKubeClient().getResourceAsYaml("kafkauser", userName);
        assertThat(kafkaUserResource, containsString(Labels.STRIMZI_CLUSTER_LABEL + ": " + firstClusterName));
    }

    @Test
    @Tag(INTERNAL_CLIENTS_USED)
    void testMessagesAreStoredInDisk() {
        String topicName = KafkaTopicUtils.generateRandomNameOfTopic();
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 1, 1).done();

        Map<String, String> kafkaPodsSnapshot = StatefulSetUtils.ssSnapshot(kafkaStatefulSetName(CLUSTER_NAME));

        KafkaTopicResource.topic(CLUSTER_NAME, topicName, 1, 1).done();

        KafkaClientsResource.deployKafkaClients(false, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).done();

        final String kafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(TEST_TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        TestUtils.waitFor("KafkaTopic creation inside kafka pod", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0), "/bin/bash",
                        "-c", "cd /var/lib/kafka/data/kafka-log0; ls -1").out().contains(topicName));

        String topicDirNameInPod = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0), "/bin/bash",
                "-c", "cd /var/lib/kafka/data/kafka-log0; ls -1 | sed -n '/" + topicName + "/p'").out();

        String commandToGetDataFromTopic =
                "cd /var/lib/kafka/data/kafka-log0/" + topicDirNameInPod + "/;cat 00000000000000000000.log";

        LOGGER.info("Executing command {} in {}", commandToGetDataFromTopic, KafkaResources.kafkaPodName(CLUSTER_NAME, 0));
        String topicData = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0),
                "/bin/bash", "-c", commandToGetDataFromTopic).out();

        LOGGER.info("Topic {} is present in kafka broker {} with no data", topicName, KafkaResources.kafkaPodName(CLUSTER_NAME, 0));
        assertThat("Topic contains data", topicData, emptyOrNullString());
        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );

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
        StatefulSetUtils.waitTillSsHasRolled(kafkaStatefulSetName(CLUSTER_NAME), 1, kafkaPodsSnapshot);

        LOGGER.info("Executing command {} in {}", commandToGetDataFromTopic, KafkaResources.kafkaPodName(CLUSTER_NAME, 0));
        topicData = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0), "/bin/bash", "-c",
                commandToGetDataFromTopic).out();

        assertThat("Topic has no data", topicData, notNullValue());
    }

    @Test
    @Tag(INTERNAL_CLIENTS_USED)
    void testConsumerOffsetFiles() {
        Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put("offsets.topic.replication.factor", "3");
        kafkaConfig.put("offsets.topic.num.partitions", "100");

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1)
            .editSpec()
                .editKafka()
                    .withConfig(kafkaConfig)
                .endKafka()
            .endSpec()
            .done();

        KafkaTopicResource.topic(CLUSTER_NAME, TEST_TOPIC_NAME, 3, 1).done();

        KafkaClientsResource.deployKafkaClients(false, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).done();

        final String kafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(TEST_TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        String commandToGetFiles =  "cd /var/lib/kafka/data/kafka-log0/;" +
                "ls -1 | sed -n \"s#__consumer_offsets-\\([0-9]*\\)#\\1#p\" | sort -V";

        LOGGER.info("Executing command {} in {}", commandToGetFiles, KafkaResources.kafkaPodName(CLUSTER_NAME, 0));
        String result = cmdKubeClient().execInPod(KafkaResources.kafkaPodName(CLUSTER_NAME, 0),
                "/bin/bash", "-c", commandToGetFiles).out();

        assertThat("Folder kafka-log0 has data in files", result.equals(""));

        internalKafkaClient.checkProducedAndConsumedMessages(
            internalKafkaClient.sendMessagesPlain(),
            internalKafkaClient.receiveMessagesPlain()
        );

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
    void testLabelsAndAnnotationForPVC() {
        final String labelAnnotationKey = "testKey";

        Map<String, String> pvcLabel = new HashMap<>();
        pvcLabel.put(labelAnnotationKey, "testValue");
        Map<String, String> pvcAnnotation = pvcLabel;

        Map<String, String> statefulSetLabels = new HashMap<>();
        statefulSetLabels.put("app.kubernetes.io/part-of", "some-app");
        statefulSetLabels.put("app.kubernetes.io/managed-by", "some-app");

        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3, 1)
            .editSpec()
                .editKafka()
                    .withNewTemplate()
                        .withNewStatefulset()
                            .withNewMetadata()
                                .withLabels(statefulSetLabels)
                            .endMetadata()
                        .endStatefulset()
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

        LOGGER.info("Check if Kubernetes labels are applied");
        Map<String, String> actualStatefulSetLabels = kubeClient().getStatefulSet(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME)).getMetadata().getLabels();
        assertThat(actualStatefulSetLabels.get("app.kubernetes.io/part-of"), is("some-app"));
        assertThat(actualStatefulSetLabels.get("app.kubernetes.io/managed-by"), is(not("some-app")));
        LOGGER.info("Kubernetes labels are correctly set and present");

        List<PersistentVolumeClaim> pvcs = kubeClient().listPersistentVolumeClaims();

        assertThat(7, is(kubeClient().listPersistentVolumeClaims().size()));

        for (PersistentVolumeClaim pvc : pvcs) {
            LOGGER.info("Verifying that PVC label {} - {} = {}", pvc.getMetadata().getName(), "testValue", pvc.getMetadata().getLabels().get("testKey"));

            assertThat("testValue", is(pvc.getMetadata().getLabels().get("testKey")));
            assertThat("testValue", is(pvc.getMetadata().getAnnotations().get("testKey")));
        }

        pvcLabel.put(labelAnnotationKey, "editedTestValue");
        pvcAnnotation.put(labelAnnotationKey, "editedTestValue");

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            LOGGER.info("Replacing kafka && zookeeper labels and annotaions from {} to {}", "testKey", "editedTestValue");
            kafka.getSpec().getKafka().getTemplate().getPersistentVolumeClaim().getMetadata().setLabels(pvcLabel);
            kafka.getSpec().getKafka().getTemplate().getPersistentVolumeClaim().getMetadata().setAnnotations(pvcAnnotation);

            kafka.getSpec().getZookeeper().getTemplate().getPersistentVolumeClaim().getMetadata().setLabels(pvcLabel);
            kafka.getSpec().getZookeeper().getTemplate().getPersistentVolumeClaim().getMetadata().setAnnotations(pvcAnnotation);
        });

        PersistentVolumeClaimUtils.waitUntilPVCLabelsChange(pvcLabel, labelAnnotationKey);
        PersistentVolumeClaimUtils.waitUntilPVCAnnotationChange(pvcAnnotation, labelAnnotationKey);
        KafkaUtils.waitForKafkaReady(CLUSTER_NAME);

        pvcs = kubeClient().listPersistentVolumeClaims();

        assertThat(7, is(kubeClient().listPersistentVolumeClaims().size()));

        for (PersistentVolumeClaim pvc : pvcs) {
            LOGGER.info("Verifying replaced PVC label {} - {} = {}", pvc.getMetadata().getName(), "testValue", pvc.getMetadata().getLabels().get("testKey"));

            assertThat("editedTestValue", is(pvc.getMetadata().getLabels().get("testKey")));
            assertThat("editedTestValue", is(pvc.getMetadata().getAnnotations().get("testKey")));
        }
    }

    @Test
    void testKafkaOffsetsReplicationFactorHigherThanReplicas() {
        int replicas = 3;
        Kafka kafka = KafkaResource.kafkaWithoutWait(KafkaResource.defaultKafka(CLUSTER_NAME, replicas, 1)
            .editSpec()
                .editKafka()
                    .addToConfig("offsets.topic.replication.factor", 4)
                    .addToConfig("transaction.state.log.min.isr", 4)
                    .addToConfig("transaction.state.log.replication.factor", 4)
                .endKafka()
            .endSpec().build());

        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(CLUSTER_NAME, NAMESPACE,
            "Kafka configuration option .* should be set to " + replicas + " or less because 'spec.kafka.replicas' is " + replicas);
        KafkaResource.kafkaClient().inNamespace(NAMESPACE).delete(kafka);
    }

    @Test
    void testHostAliases() {
        HostAlias hostAlias = new HostAliasBuilder()
            .withIp(aliasIp)
            .withHostnames(aliasHostname)
            .build();

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3)
            .editSpec()
                .editKafka()
                    .withNewTemplate()
                        .withNewPod()
                            .withHostAliases(hostAlias)
                        .endPod()
                    .endTemplate()
                .endKafka()
                .editZookeeper()
                    .withNewTemplate()
                        .withNewPod()
                            .withHostAliases(hostAlias)
                        .endPod()
                    .endTemplate()
                .endZookeeper()
            .endSpec()
            .done();

        List<String> pods = kubeClient().listPodNames(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME);

        for (String podName : pods) {
            if (!podName.contains("entity-operator")) {
                String containerName = podName.contains("kafka") ? "kafka" : "zookeeper";
                LOGGER.info("Checking the /etc/hosts file");
                String output = cmdKubeClient().execInPodContainer(false, podName, containerName, "cat", "/etc/hosts").out();
                assertThat(output, containsString(etcHostsData));
            }
        }
    }

    protected void checkKafkaConfiguration(String podNamePrefix, Map<String, Object> config, String clusterName) {
        LOGGER.info("Checking kafka configuration");
        List<Pod> pods = kubeClient().listPodsByPrefixInName(podNamePrefix);

        Properties properties = configMap2Properties(kubeClient().getConfigMap(clusterName + "-kafka-config"));

        for (Map.Entry<String, Object> property : config.entrySet()) {
            String key = property.getKey();
            Object val = property.getValue();

            assertThat(properties.keySet().contains(key), is(true));
            assertThat(properties.getProperty(key), is(val));
        }

        for (Pod pod: pods) {
            ExecResult result = cmdKubeClient().execInPod(pod.getMetadata().getName(), "/bin/bash", "-c", "cat /tmp/strimzi.properties");
            Properties execProperties = stringToProperties(result.out());

            for (Map.Entry<String, Object> property : config.entrySet()) {
                String key = property.getKey();
                Object val = property.getValue();

                assertThat(execProperties.keySet().contains(key), is(true));
                assertThat(execProperties.getProperty(key), is(val));
            }
        }
    }

    void checkStorageSizeForVolumes(List<PersistentVolumeClaim> volumes, String[] diskSizes, int kafkaRepl, int diskCount) {
        int k = 0;
        for (int i = 0; i < kafkaRepl; i++) {
            for (int j = 0; j < diskCount; j++) {
                LOGGER.info("Checking volume {} and size of storage {}", volumes.get(k).getMetadata().getName(),
                        volumes.get(k).getSpec().getResources().getRequests().get("storage"));
                assertThat(volumes.get(k).getSpec().getResources().getRequests().get("storage"), is(new Quantity(diskSizes[i])));
                k++;
            }
        }
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

    void verifyVolumeNamesAndLabels(int kafkaReplicas, int diskCountPerReplica, String diskSizeGi) {
        ArrayList<String> pvcs = new ArrayList<>();

        kubeClient().listPersistentVolumeClaims().stream()
                .filter(pvc -> pvc.getMetadata().getName().contains("kafka"))
                .forEach(volume -> {
                    String volumeName = volume.getMetadata().getName();
                    pvcs.add(volumeName);
                    LOGGER.info("Checking labels for volume:" + volumeName);
                    assertThat(volume.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is(CLUSTER_NAME));
                    assertThat(volume.getMetadata().getLabels().get(Labels.STRIMZI_KIND_LABEL), is(Kafka.RESOURCE_KIND));
                    assertThat(volume.getMetadata().getLabels().get(Labels.STRIMZI_NAME_LABEL), is(CLUSTER_NAME.concat("-kafka")));
                    assertThat(volume.getSpec().getResources().getRequests().get("storage"), is(new Quantity(diskSizeGi, "Gi")));
                });

        LOGGER.info("Checking PVC names included in JBOD array");
        for (int i = 0; i < kafkaReplicas; i++) {
            for (int j = 0; j < diskCountPerReplica; j++) {
                assertThat(pvcs.contains("data-" + j + "-" + CLUSTER_NAME + "-kafka-" + i), is(true));
            }
        }

        LOGGER.info("Checking PVC on Kafka pods");
        for (int i = 0; i < kafkaReplicas; i++) {
            ArrayList<String> dataSourcesOnPod = new ArrayList<>();
            ArrayList<String> pvcsOnPod = new ArrayList<>();

            LOGGER.info("Getting list of mounted data sources and PVCs on Kafka pod " + i);
            for (int j = 0; j < diskCountPerReplica; j++) {
                dataSourcesOnPod.add(kubeClient().getPod(CLUSTER_NAME.concat("-kafka-" + i))
                        .getSpec().getVolumes().get(j).getName());
                pvcsOnPod.add(kubeClient().getPod(CLUSTER_NAME.concat("-kafka-" + i))
                        .getSpec().getVolumes().get(j).getPersistentVolumeClaim().getClaimName());
            }

            LOGGER.info("Verifying mounted data sources and PVCs on Kafka pod " + i);
            for (int j = 0; j < diskCountPerReplica; j++) {
                assertThat(dataSourcesOnPod.contains("data-" + j), is(true));
                assertThat(pvcsOnPod.contains("data-" + j + "-" + CLUSTER_NAME + "-kafka-" + i), is(true));
            }
        }
    }

    void verifyPresentLabels(Map<String, String> labels, HasMetadata resources) {
        for (Map.Entry<String, String> label : labels.entrySet()) {
            assertThat("Label exists with concrete value in HasMetadata(Services, CM, STS) resources",
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

    void verifyAppLabels(Map<String, String> labels) {
        LOGGER.info("Verifying labels {}", labels);
        assertThat("Label " + Labels.STRIMZI_CLUSTER_LABEL + " is not present", labels.containsKey(Labels.STRIMZI_CLUSTER_LABEL));
        assertThat("Label " + Labels.STRIMZI_KIND_LABEL + " is not present", labels.containsKey(Labels.STRIMZI_KIND_LABEL));
        assertThat("Label " + Labels.STRIMZI_NAME_LABEL + " is not present", labels.containsKey(Labels.STRIMZI_NAME_LABEL));
    }

    void verifyAppLabelsForSecretsAndConfigMaps(Map<String, String> labels) {
        LOGGER.info("Verifying labels {}", labels);
        assertThat("Label " + Labels.STRIMZI_CLUSTER_LABEL + " is not present", labels.containsKey(Labels.STRIMZI_CLUSTER_LABEL));
        assertThat("Label " + Labels.STRIMZI_KIND_LABEL + " is not present", labels.containsKey(Labels.STRIMZI_KIND_LABEL));
    }

    @BeforeAll
    void setup() throws Exception {
        ResourceManager.setClassResources();
        installClusterOperator(NAMESPACE);
    }

    @Override
    protected void tearDownEnvironmentAfterEach() throws Exception {
        super.tearDownEnvironmentAfterEach();
        if (cluster.getListOfDeployedResources().contains(TEMPLATE_PATH)) {
            cluster.deleteCustomResources(TEMPLATE_PATH);
        }

        if (KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(OPENSHIFT_CLUSTER_NAME).get() != null) {
            cmdKubeClient().deleteByName(Kafka.RESOURCE_KIND, OPENSHIFT_CLUSTER_NAME);
        }

        kubeClient().listPods().stream()
            .filter(p -> p.getMetadata().getName().startsWith(OPENSHIFT_CLUSTER_NAME))
            .forEach(p -> PodUtils.deletePodWithWait(p.getMetadata().getName()));

        kubeClient().getClient().customResources(CustomResourceDefinitionContext.fromCrd(Crds.kafkaTopic()), KafkaTopic.class, KafkaTopicList.class, DoneableKafkaTopic.class).inNamespace(NAMESPACE).delete();
        kubeClient().getClient().persistentVolumeClaims().inNamespace(NAMESPACE).delete();
    }
}
