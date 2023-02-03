/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.strimzi.api.kafka.KafkaTopicList;
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
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListener;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.JbodStorageBuilder;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageBuilder;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.annotations.ParallelSuite;
import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.ConfigMapUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PersistentVolumeClaimUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.ServiceUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.executor.ExecResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.time.Duration;
import java.time.Instant;
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
import static io.strimzi.systemtest.Constants.CRUISE_CONTROL;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.LOADBALANCER_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.utils.StUtils.configMap2Properties;
import static io.strimzi.systemtest.utils.StUtils.stringToProperties;
import static io.strimzi.test.TestUtils.fromYamlString;
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
@ParallelSuite
class KafkaST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(KafkaST.class);
    private static final String OPENSHIFT_CLUSTER_NAME = "openshift-my-cluster";

    private final String namespace = testSuiteNamespaceManager.getMapOfAdditionalNamespaces().get(KafkaST.class.getSimpleName()).stream().findFirst().get();

    @ParallelNamespaceTest
    @KRaftNotSupported("EntityOperator is not supported by KRaft mode and is used in this test class")
    void testEODeletion(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());

        // Get pod name to check termination process
        Pod pod = kubeClient(namespaceName).listPods(namespaceName).stream()
                .filter(p -> p.getMetadata().getName().startsWith(KafkaResources.entityOperatorDeploymentName(clusterName)))
                .findAny()
                .orElseThrow();

        assertThat("Entity operator pod does not exist", pod, notNullValue());

        LOGGER.info("Setting entity operator to null");

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> kafka.getSpec().setEntityOperator(null), namespaceName);

        // Wait when EO(UO + TO) will be removed
        DeploymentUtils.waitForDeploymentDeletion(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName));
        PodUtils.deletePodWithWait(namespaceName, pod.getMetadata().getName());

        LOGGER.info("Entity operator was deleted");
    }

    @ParallelNamespaceTest
    @SuppressWarnings({"checkstyle:MethodLength", "checkstyle:JavaNCSS"})
    void testCustomAndUpdatedValues(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaStatefulSetName(clusterName));
        final LabelSelector zkSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.zookeeperStatefulSetName(clusterName));

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
        updatedKafkaConfig.put("offsets.topic.replication.factor", "3");
        updatedKafkaConfig.put("transaction.state.log.replication.factor", "3");
        updatedKafkaConfig.put("default.replication.factor", "3");

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

        final int initialDelaySeconds = 30;
        final int timeoutSeconds = 10;
        final int updatedInitialDelaySeconds = 31;
        final int updatedTimeoutSeconds = 11;
        final int periodSeconds = 10;
        final int successThreshold = 1;
        final int failureThreshold = 3;
        final int updatedPeriodSeconds = 5;
        final int updatedFailureThreshold = 1;

        Kafka kafka = KafkaTemplates.kafkaPersistent(clusterName, 3)
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
                    .withReplicas(3)
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
            .build();

        if (Environment.isKRaftModeEnabled()) {
            kafka.getSpec().getEntityOperator().setTopicOperator(null);
            kafka.getSpec().getEntityOperator().getTemplate().setTopicOperatorContainer(null);
        }

        resourceManager.createResource(extensionContext, kafka);

        final Map<String, String> kafkaSnapshot = PodUtils.podSnapshot(namespaceName, kafkaSelector);
        final Map<String, String> zkSnapshot = PodUtils.podSnapshot(namespaceName, zkSelector);
        final Map<String, String> eoPod = DeploymentUtils.depSnapshot(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName));

        LOGGER.info("Verify values before update");
        checkReadinessLivenessProbe(namespaceName, kafkaStatefulSetName(clusterName), "kafka", initialDelaySeconds, timeoutSeconds,
                periodSeconds, successThreshold, failureThreshold);
        checkKafkaConfiguration(namespaceName, kafkaStatefulSetName(clusterName), kafkaConfig, clusterName);
        checkSpecificVariablesInContainer(namespaceName, kafkaStatefulSetName(clusterName), "kafka", envVarGeneral);

        for (String cmName : StUtils.getKafkaConfigurationConfigMaps(clusterName, 3)) {
            String kafkaConfiguration = kubeClient().getConfigMap(namespaceName, cmName).getData().get("server.config");
            assertThat(kafkaConfiguration, containsString("offsets.topic.replication.factor=1"));
            assertThat(kafkaConfiguration, containsString("transaction.state.log.replication.factor=1"));
            assertThat(kafkaConfiguration, containsString("default.replication.factor=1"));
        }

        String kafkaConfigurationFromPod = cmdKubeClient(namespaceName).execInPod(KafkaResources.kafkaPodName(clusterName, 0), "cat", "/tmp/strimzi.properties").out();
        assertThat(kafkaConfigurationFromPod, containsString("offsets.topic.replication.factor=1"));
        assertThat(kafkaConfigurationFromPod, containsString("transaction.state.log.replication.factor=1"));
        assertThat(kafkaConfigurationFromPod, containsString("default.replication.factor=1"));

        if (!Environment.isKRaftModeEnabled()) {
            LOGGER.info("Testing Zookeepers");
            checkReadinessLivenessProbe(namespaceName, zookeeperStatefulSetName(clusterName), "zookeeper", initialDelaySeconds, timeoutSeconds,
                    periodSeconds, successThreshold, failureThreshold);
            checkComponentConfiguration(namespaceName, zookeeperStatefulSetName(clusterName), "zookeeper", "ZOOKEEPER_CONFIGURATION", zookeeperConfig);
            checkSpecificVariablesInContainer(namespaceName, zookeeperStatefulSetName(clusterName), "zookeeper", envVarGeneral);
        }

        LOGGER.info("Checking configuration of TO and UO");
        if (!Environment.isKRaftModeEnabled()) {
            checkReadinessLivenessProbe(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), "topic-operator", initialDelaySeconds, timeoutSeconds,
                    periodSeconds, successThreshold, failureThreshold);
            checkSpecificVariablesInContainer(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), "topic-operator", envVarGeneral);

            checkReadinessLivenessProbe(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), "tls-sidecar", initialDelaySeconds, timeoutSeconds,
                    periodSeconds, successThreshold, failureThreshold);
            checkSpecificVariablesInContainer(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), "tls-sidecar", envVarGeneral);
        }

        checkReadinessLivenessProbe(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), "user-operator", initialDelaySeconds, timeoutSeconds,
                periodSeconds, successThreshold, failureThreshold);
        checkSpecificVariablesInContainer(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), "user-operator", envVarGeneral);

        LOGGER.info("Updating configuration of Kafka cluster");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, k -> {
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
            entityOperatorSpec.getTemplate().getUserOperatorContainer().setEnv(StUtils.createContainerEnvVarsFromMap(envVarUpdated));
            entityOperatorSpec.getTemplate().getTlsSidecarContainer().setEnv(StUtils.createContainerEnvVarsFromMap(envVarUpdated));
            if (!Environment.isKRaftModeEnabled()) {
                entityOperatorSpec.getTopicOperator().getLivenessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
                entityOperatorSpec.getTopicOperator().getReadinessProbe().setInitialDelaySeconds(updatedInitialDelaySeconds);
                entityOperatorSpec.getTopicOperator().getLivenessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
                entityOperatorSpec.getTopicOperator().getReadinessProbe().setTimeoutSeconds(updatedTimeoutSeconds);
                entityOperatorSpec.getTopicOperator().getLivenessProbe().setPeriodSeconds(updatedPeriodSeconds);
                entityOperatorSpec.getTopicOperator().getReadinessProbe().setPeriodSeconds(updatedPeriodSeconds);
                entityOperatorSpec.getTopicOperator().getLivenessProbe().setFailureThreshold(updatedFailureThreshold);
                entityOperatorSpec.getTopicOperator().getReadinessProbe().setFailureThreshold(updatedFailureThreshold);
                entityOperatorSpec.getTemplate().getTopicOperatorContainer().setEnv(StUtils.createContainerEnvVarsFromMap(envVarUpdated));
            }
        }, namespaceName);

        if (!Environment.isKRaftModeEnabled()) {
            RollingUpdateUtils.waitTillComponentHasRolled(namespaceName, zkSelector, 3, zkSnapshot);
        }
        RollingUpdateUtils.waitTillComponentHasRolled(namespaceName, kafkaSelector, 3, kafkaSnapshot);
        DeploymentUtils.waitTillDepHasRolled(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), 1, eoPod);
        KafkaUtils.waitForKafkaReady(namespaceName, clusterName);

        LOGGER.info("Verify values after update");
        checkReadinessLivenessProbe(namespaceName, kafkaStatefulSetName(clusterName), "kafka", updatedInitialDelaySeconds, updatedTimeoutSeconds,
                updatedPeriodSeconds, successThreshold, updatedFailureThreshold);
        checkKafkaConfiguration(namespaceName, kafkaStatefulSetName(clusterName), updatedKafkaConfig, clusterName);
        checkSpecificVariablesInContainer(namespaceName, kafkaStatefulSetName(clusterName), "kafka", envVarUpdated);

        for (String cmName : StUtils.getKafkaConfigurationConfigMaps(clusterName, 3)) {
            String kafkaConfiguration = kubeClient(namespaceName).getConfigMap(namespaceName, cmName).getData().get("server.config");
            assertThat(kafkaConfiguration, containsString("offsets.topic.replication.factor=3"));
            assertThat(kafkaConfiguration, containsString("transaction.state.log.replication.factor=3"));
            assertThat(kafkaConfiguration, containsString("default.replication.factor=3"));
        }

        kafkaConfigurationFromPod = cmdKubeClient(namespaceName).execInPod(KafkaResources.kafkaPodName(clusterName, 0), "cat", "/tmp/strimzi.properties").out();
        assertThat(kafkaConfigurationFromPod, containsString("offsets.topic.replication.factor=3"));
        assertThat(kafkaConfigurationFromPod, containsString("transaction.state.log.replication.factor=3"));
        assertThat(kafkaConfigurationFromPod, containsString("default.replication.factor=3"));

        if (!Environment.isKRaftModeEnabled()) {
            LOGGER.info("Testing Zookeepers");
            checkReadinessLivenessProbe(namespaceName, zookeeperStatefulSetName(clusterName), "zookeeper", updatedInitialDelaySeconds, updatedTimeoutSeconds,
                    updatedPeriodSeconds, successThreshold, updatedFailureThreshold);
            checkComponentConfiguration(namespaceName, zookeeperStatefulSetName(clusterName), "zookeeper", "ZOOKEEPER_CONFIGURATION", updatedZookeeperConfig);
            checkSpecificVariablesInContainer(namespaceName, zookeeperStatefulSetName(clusterName), "zookeeper", envVarUpdated);

            LOGGER.info("Getting entity operator to check configuration of TO and UO");
            checkReadinessLivenessProbe(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), "topic-operator", updatedInitialDelaySeconds, updatedTimeoutSeconds,
                    updatedPeriodSeconds, successThreshold, updatedFailureThreshold);
            checkSpecificVariablesInContainer(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), "topic-operator", envVarUpdated);

            checkReadinessLivenessProbe(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), "tls-sidecar", updatedInitialDelaySeconds, updatedTimeoutSeconds,
                    updatedPeriodSeconds, successThreshold, updatedFailureThreshold);
            checkSpecificVariablesInContainer(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), "tls-sidecar", envVarUpdated);
        }

        checkReadinessLivenessProbe(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), "user-operator", updatedInitialDelaySeconds, updatedTimeoutSeconds,
                updatedPeriodSeconds, successThreshold, updatedFailureThreshold);
        checkSpecificVariablesInContainer(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), "user-operator", envVarUpdated);
    }

    @ParallelNamespaceTest
    @KRaftNotSupported("EntityOperator is not supported by KRaft mode and is used in this test class")
    void testJvmAndResources(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaStatefulSetName(clusterName));
        final LabelSelector zkSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.zookeeperStatefulSetName(clusterName));

        ArrayList<SystemProperty> javaSystemProps = new ArrayList<>();
        javaSystemProps.add(new SystemPropertyBuilder().withName("javax.net.debug")
                .withValue("verbose").build());

        Map<String, String> jvmOptionsXX = new HashMap<>();
        jvmOptionsXX.put("UseG1GC", "true");

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 1, 1)
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
            .endSpec()
            .build());

        // Make snapshots for Kafka cluster to meke sure that there is no rolling update after CO reconciliation
        final String zkStsName = KafkaResources.zookeeperStatefulSetName(clusterName);
        final String kafkaStsName = kafkaStatefulSetName(clusterName);
        final String eoDepName = KafkaResources.entityOperatorDeploymentName(clusterName);
        final Map<String, String> zkPods = PodUtils.podSnapshot(namespaceName, zkSelector);
        final Map<String, String> kafkaPods = PodUtils.podSnapshot(namespaceName, kafkaSelector);
        final Map<String, String> eoPods = DeploymentUtils.depSnapshot(namespaceName, eoDepName);

        assertResources(namespaceName, KafkaResources.kafkaPodName(clusterName, 0), "kafka",
                "1536Mi", "1", "1Gi", "50m");
        assertExpectedJavaOpts(namespaceName, KafkaResources.kafkaPodName(clusterName, 0), "kafka",
                "-Xmx1g", "-Xms512m", "-XX:+UseG1GC");

        assertResources(namespaceName, KafkaResources.zookeeperPodName(clusterName, 0), "zookeeper",
                "1G", "500m", "500M", "25m");
        assertExpectedJavaOpts(namespaceName, KafkaResources.zookeeperPodName(clusterName, 0), "zookeeper",
                "-Xmx1G", "-Xms512M", "-XX:+UseG1GC");

        Optional<Pod> pod = kubeClient(namespaceName).listPods(namespaceName)
                .stream().filter(p -> p.getMetadata().getName().startsWith(KafkaResources.entityOperatorDeploymentName(clusterName)))
                .findFirst();
        assertThat("EO pod does not exist", pod.isPresent(), is(true));

        assertResources(namespaceName, pod.get().getMetadata().getName(), "topic-operator",
                "1Gi", "500m", "384Mi", "25m");
        assertResources(namespaceName, pod.get().getMetadata().getName(), "user-operator",
                "512M", "300m", "256M", "30m");
        assertExpectedJavaOpts(namespaceName, pod.get().getMetadata().getName(), "topic-operator",
                "-Xmx2G", "-Xms1024M", null);
        assertExpectedJavaOpts(namespaceName, pod.get().getMetadata().getName(), "user-operator",
                "-Xmx1G", "-Xms512M", null);

        String eoPod = eoPods.keySet().toArray()[0].toString();
        kubeClient(namespaceName).getPod(namespaceName, eoPod).getSpec().getContainers().forEach(container -> {
            if (!container.getName().equals("tls-sidecar")) {
                LOGGER.info("Check if -D java options are present in {}", container.getName());

                String javaSystemProp = container.getEnv().stream().filter(envVar ->
                    envVar.getName().equals("STRIMZI_JAVA_SYSTEM_PROPERTIES")).findFirst().orElseThrow().getValue();
                String javaOpts = container.getEnv().stream().filter(envVar ->
                    envVar.getName().equals("STRIMZI_JAVA_OPTS")).findFirst().orElseThrow().getValue();

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
        RollingUpdateUtils.waitForNoRollingUpdate(namespaceName, zkSelector, zkPods);
        RollingUpdateUtils.waitForNoRollingUpdate(namespaceName, kafkaSelector, kafkaPods);
        DeploymentUtils.waitForNoRollingUpdate(namespaceName, eoDepName, eoPods);
    }

    @ParallelNamespaceTest
    @KRaftNotSupported("TopicOperator is not supported by KRaft mode and is used in this test class")
    void testForTopicOperator(ExtensionContext extensionContext) throws InterruptedException {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String scraperName = mapWithScraperNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext,
            KafkaTemplates.kafkaEphemeral(clusterName, 3).build(),
            ScraperTemplates.scraperPod(namespaceName, scraperName).build()
        );

        final String scraperPodName =  kubeClient().listPodsByPrefixInName(namespaceName, scraperName).get(0).getMetadata().getName();
        final String topicName = KafkaTopicUtils.generateRandomNameOfTopic();
        final String cliTopicName = "topic-from-cli";

        //Creating topics for testing
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());

        KafkaTopicUtils.waitForKafkaTopicReady(namespaceName, topicName);

        assertThat(KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName).withName(topicName).get().getMetadata().getName(), is(topicName));
        assertThat(KafkaCmdClient.listTopicsUsingPodCli(namespaceName, scraperPodName, KafkaResources.plainBootstrapAddress(clusterName)), hasItem(topicName));

        KafkaCmdClient.createTopicUsingPodCli(namespaceName, scraperPodName, KafkaResources.plainBootstrapAddress(clusterName), cliTopicName, 1, 1);
        assertThat(KafkaCmdClient.listTopicsUsingPodCli(namespaceName, scraperPodName, KafkaResources.plainBootstrapAddress(clusterName)), hasItems(topicName, cliTopicName));
        assertThat(cmdKubeClient(namespaceName).list(KafkaTopic.RESOURCE_KIND), hasItems(cliTopicName, topicName));

        //Updating first topic using pod CLI
        KafkaCmdClient.updateTopicPartitionsCountUsingPodCli(namespaceName, scraperPodName, KafkaResources.plainBootstrapAddress(clusterName), topicName, 2);

        KafkaUtils.waitForKafkaReady(namespaceName, clusterName);

        assertThat(KafkaCmdClient.describeTopicUsingPodCli(namespaceName, scraperPodName, KafkaResources.plainBootstrapAddress(clusterName), topicName),
                containsString("PartitionCount: 2"));
        KafkaTopic testTopic = fromYamlString(cmdKubeClient(namespaceName).get(KafkaTopic.RESOURCE_KIND, topicName), KafkaTopic.class);
        assertThat(testTopic, is(CoreMatchers.notNullValue()));
        assertThat(testTopic.getSpec(), is(CoreMatchers.notNullValue()));
        assertThat(testTopic.getSpec().getPartitions(), is(Integer.valueOf(2)));

        //Updating second topic via KafkaTopic update
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(cliTopicName, topic -> topic.getSpec().setPartitions(2), namespaceName);

        KafkaUtils.waitForKafkaReady(namespaceName, clusterName);

        assertThat(KafkaCmdClient.describeTopicUsingPodCli(namespaceName, scraperPodName, KafkaResources.plainBootstrapAddress(clusterName), cliTopicName),
            containsString("PartitionCount: 2"));
        testTopic = fromYamlString(cmdKubeClient(namespaceName).get(KafkaTopic.RESOURCE_KIND, cliTopicName), KafkaTopic.class);
        assertThat(testTopic, is(CoreMatchers.notNullValue()));
        assertThat(testTopic.getSpec(), is(CoreMatchers.notNullValue()));
        assertThat(testTopic.getSpec().getPartitions(), is(Integer.valueOf(2)));

        //Deleting first topic by deletion of CM
        cmdKubeClient(namespaceName).deleteByName(KafkaTopic.RESOURCE_KIND, cliTopicName);

        //Deleting another topic using pod CLI
        KafkaCmdClient.deleteTopicUsingPodCli(namespaceName, scraperPodName, KafkaResources.plainBootstrapAddress(clusterName), topicName);
        KafkaTopicUtils.waitForKafkaTopicDeletion(namespaceName, topicName);

        //Checking all topics were deleted
        Thread.sleep(Constants.TIMEOUT_TEARDOWN);
        List<String> topics = KafkaCmdClient.listTopicsUsingPodCli(namespaceName, scraperPodName, KafkaResources.plainBootstrapAddress(clusterName));
        assertThat(topics, not(hasItems(topicName)));
        assertThat(topics, not(hasItems(cliTopicName)));
    }

    @ParallelNamespaceTest
    @KRaftNotSupported("TopicOperator is not supported by KRaft mode and is used in this test class")
    void testRemoveTopicOperatorFromEntityOperator(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        LOGGER.info("Deploying Kafka cluster {}", clusterName);
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());
        String eoPodName = kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName))
            .get(0).getMetadata().getName();

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, k -> k.getSpec().getEntityOperator().setTopicOperator(null), namespaceName);
        //Waiting when EO pod will be recreated without TO
        PodUtils.deletePodWithWait(namespaceName, eoPodName);
        DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), 1);
        PodUtils.waitUntilPodContainersCount(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), 1);

        //Checking that TO was removed
        kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName)).forEach(pod -> {
            pod.getSpec().getContainers().forEach(container -> {
                assertThat(container.getName(), not(containsString("topic-operator")));
            });
        });

        eoPodName = kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName))
                .get(0).getMetadata().getName();

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, k -> k.getSpec().getEntityOperator().setTopicOperator(new EntityTopicOperatorSpec()), namespaceName);
        //Waiting when EO pod will be recreated with TO
        PodUtils.deletePodWithWait(namespaceName, eoPodName);
        DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), 1);

        //Checking that TO was created
        kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName)).forEach(pod -> {
            pod.getSpec().getContainers().forEach(container -> {
                assertThat(container.getName(), anyOf(
                    containsString("topic-operator"),
                    containsString("user-operator"),
                    containsString("tls-sidecar"))
                );
            });
        });
    }

    @ParallelNamespaceTest
    void testRemoveUserOperatorFromEntityOperator(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        Instant startTime = Instant.now();

        LOGGER.info("Deploying Kafka cluster {}", clusterName);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, k -> k.getSpec().getEntityOperator().setUserOperator(null), namespaceName);

        if (!Environment.isKRaftModeEnabled()) {
            //Waiting when EO pod will be recreated without UO
            DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), 1);
            PodUtils.waitUntilPodContainersCount(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), 2);

            //Checking that UO was removed
            kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName)).forEach(pod -> {
                pod.getSpec().getContainers().forEach(container -> {
                    assertThat(container.getName(), not(containsString("user-operator")));
                });
            });
        } else {
            DeploymentUtils.waitForDeploymentDeletion(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName));
        }

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, k -> k.getSpec().getEntityOperator().setUserOperator(new EntityUserOperatorSpec()), namespaceName);
        //Waiting when EO pod will be recreated with UO
        DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), 1);

        //Checking that UO was created
        kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName)).forEach(pod -> {
            pod.getSpec().getContainers().forEach(container -> {
                assertThat(container.getName(), anyOf(
                        containsString("topic-operator"),
                        containsString("user-operator"),
                        containsString("tls-sidecar"))
                );
            });
        });

        Instant endTime = Instant.now();
        long duration = Duration.between(startTime, endTime).toSeconds();
        assertNoCoErrorsLogged(namespaceName, duration);
    }

    @ParallelNamespaceTest
    @KRaftNotSupported("EntityOperator is not supported by KRaft mode and is used in this test class")
    void testRemoveUserAndTopicOperatorsFromEntityOperator(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        Instant startTime = Instant.now();
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());

        String eoDeploymentName = KafkaResources.entityOperatorDeploymentName(clusterName);

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, k -> {
            k.getSpec().getEntityOperator().setTopicOperator(null);
            k.getSpec().getEntityOperator().setUserOperator(null);
        }, namespaceName);

        PodUtils.waitUntilPodStabilityReplicasCount(namespaceName, eoDeploymentName, 0);

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, k -> {
            k.getSpec().getEntityOperator().setTopicOperator(new EntityTopicOperatorSpec());
            k.getSpec().getEntityOperator().setUserOperator(new EntityUserOperatorSpec());
        }, namespaceName);
        DeploymentUtils.waitForDeploymentReady(namespaceName, eoDeploymentName);

        //Checking that EO was created
        kubeClient().listPodsByPrefixInName(namespaceName, eoDeploymentName).forEach(pod -> {
            pod.getSpec().getContainers().forEach(container -> {
                assertThat(container.getName(), anyOf(
                    containsString("topic-operator"),
                    containsString("user-operator"),
                    containsString("tls-sidecar"))
                );
            });
        });

        Instant endTime = Instant.now();
        long duration = Duration.between(startTime, endTime).toSeconds();
        assertNoCoErrorsLogged(namespaceName, duration);
    }

    @ParallelNamespaceTest
    void testEntityOperatorWithoutTopicOperator(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        LOGGER.info("Deploying Kafka cluster without TO in EO");

        Instant startTime = Instant.now();

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editSpec()
                .withNewEntityOperator()
                    .withNewUserOperator()
                    .endUserOperator()
                .endEntityOperator()
            .endSpec()
            .build());

        Instant endTime = Instant.now();
        long duration = Duration.between(startTime, endTime).toSeconds();
        assertNoCoErrorsLogged(namespaceName, duration);

        //Checking that TO was not deployed
        kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName)).forEach(pod -> {
            pod.getSpec().getContainers().forEach(container -> {
                assertThat(container.getName(), not(containsString("topic-operator")));
            });
        });
    }

    @ParallelNamespaceTest
    void testEntityOperatorWithoutUserOperator(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        LOGGER.info("Deploying Kafka cluster without UO in EO");
        Instant startTime = Instant.now();
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editSpec()
                .withNewEntityOperator()
                    .withNewTopicOperator()
                    .endTopicOperator()
                .endEntityOperator()
            .endSpec()
            .build());

        Instant endTime = Instant.now();
        long duration = Duration.between(startTime, endTime).toSeconds();
        assertNoCoErrorsLogged(namespaceName, duration);

        //Checking that UO was not deployed
        kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName)).forEach(pod -> {
            pod.getSpec().getContainers().forEach(container -> {
                assertThat(container.getName(), not(containsString("user-operator")));
            });
        });
    }

    @ParallelNamespaceTest
    void testEntityOperatorWithoutUserAndTopicOperators(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        LOGGER.info("Deploying Kafka cluster without UO and TO in EO");
        Instant startTime = Instant.now();
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editSpec()
                .withNewEntityOperator()
                .endEntityOperator()
            .endSpec()
            .build());

        Instant endTime = Instant.now();
        long duration = Duration.between(startTime, endTime).toSeconds();
        assertNoCoErrorsLogged(clusterOperator.getDeploymentNamespace(), duration);

        //Checking that EO was not deployed
        assertThat("EO should not be deployed", kubeClient().listPodsByPrefixInName(KafkaResources.entityOperatorDeploymentName(clusterName)).size(), is(0));
    }

    @ParallelNamespaceTest
    @KRaftNotSupported("TopicOperator is not supported by KRaft mode and is used in this test class")
    void testTopicWithoutLabels(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String scraperName = mapWithScraperNames.get(extensionContext.getDisplayName());

        // Negative scenario: creating topic without any labels and make sure that TO can't handle this topic
        resourceManager.createResource(extensionContext,
            KafkaTemplates.kafkaEphemeral(clusterName, 3).build(),
            ScraperTemplates.scraperPod(namespaceName, scraperName).build()
        );

        final String scraperPodName =  kubeClient().listPodsByPrefixInName(namespaceName, scraperName).get(0).getMetadata().getName();

        // Creating topic without any label
        resourceManager.createResource(extensionContext, false, KafkaTopicTemplates.topic(clusterName, "topic-without-labels", 1, 1, 1)
            .editMetadata()
                .withLabels(null)
            .endMetadata()
            .build()
        );

        // Checking that resource was created
        assertThat(cmdKubeClient(namespaceName).list("kafkatopic"), hasItems("topic-without-labels"));
        // Checking that TO didn't handle new topic and zk pods don't contain new topic
        assertThat(KafkaCmdClient.listTopicsUsingPodCli(namespaceName, scraperPodName, KafkaResources.plainBootstrapAddress(clusterName)), not(hasItems("topic-without-labels")));

        // Checking TO logs
        String tOPodName = cmdKubeClient(namespaceName).listResourcesByLabel("pod", Labels.STRIMZI_NAME_LABEL + "=" + clusterName + "-entity-operator").get(0);
        String tOlogs = kubeClient(namespaceName).logsInSpecificNamespace(namespaceName, tOPodName, "topic-operator");
        assertThat(tOlogs, not(containsString("Created topic 'topic-without-labels'")));

        //Deleting topic
        cmdKubeClient(namespaceName).deleteByName("kafkatopic", "topic-without-labels");
        KafkaTopicUtils.waitForKafkaTopicDeletion(namespaceName,  "topic-without-labels");

        //Checking all topics were deleted
        List<String> topics = KafkaCmdClient.listTopicsUsingPodCli(namespaceName, scraperPodName, KafkaResources.plainBootstrapAddress(clusterName));
        assertThat(topics, not(hasItems("topic-without-labels")));
    }

    @ParallelNamespaceTest
    @KRaftNotSupported("JBOD is not supported by KRaft mode and is used in this test case.")
    void testKafkaJBODDeleteClaimsTrueFalse(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final int kafkaReplicas = 2;
        final String diskSizeGi = "10";

        JbodStorage jbodStorage = new JbodStorageBuilder().withVolumes(
            new PersistentClaimStorageBuilder().withDeleteClaim(false).withId(0).withSize(diskSizeGi + "Gi").build(),
            new PersistentClaimStorageBuilder().withDeleteClaim(true).withId(1).withSize(diskSizeGi + "Gi").build()).build();

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaJBOD(clusterName, kafkaReplicas, jbodStorage).build());
        // kafka cluster already deployed
        verifyVolumeNamesAndLabels(namespaceName, clusterName, kafkaReplicas, 2, diskSizeGi);

        final int volumesCount = kubeClient(namespaceName).listPersistentVolumeClaims(namespaceName, clusterName).size();

        LOGGER.info("Deleting cluster");
        cmdKubeClient(namespaceName).deleteByName("kafka", clusterName);

        LOGGER.info("Waiting for PVC deletion");
        PersistentVolumeClaimUtils.waitForJbodStorageDeletion(namespaceName, volumesCount, jbodStorage, clusterName);
    }

    @ParallelNamespaceTest
    @KRaftNotSupported("JBOD is not supported by KRaft mode and is used in this test case.")
    void testKafkaJBODDeleteClaimsTrue(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final int kafkaReplicas = 2;
        final String diskSizeGi = "10";

        JbodStorage jbodStorage = new JbodStorageBuilder().withVolumes(
            new PersistentClaimStorageBuilder().withDeleteClaim(true).withId(0).withSize(diskSizeGi + "Gi").build(),
            new PersistentClaimStorageBuilder().withDeleteClaim(true).withId(1).withSize(diskSizeGi + "Gi").build()).build();

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaJBOD(clusterName, kafkaReplicas, jbodStorage).build());
        // kafka cluster already deployed
        verifyVolumeNamesAndLabels(namespaceName, clusterName, kafkaReplicas, 2, diskSizeGi);

        final int volumesCount = kubeClient(namespaceName).listPersistentVolumeClaims(namespaceName, clusterName).size();

        LOGGER.info("Deleting cluster");
        cmdKubeClient(namespaceName).deleteByName("kafka", clusterName);

        LOGGER.info("Waiting for PVC deletion");
        PersistentVolumeClaimUtils.waitForJbodStorageDeletion(namespaceName, volumesCount, jbodStorage, clusterName);
    }

    @ParallelNamespaceTest
    @KRaftNotSupported("JBOD is not supported by KRaft mode and is used in this test case.")
    void testKafkaJBODDeleteClaimsFalse(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final int kafkaReplicas = 2;
        final String diskSizeGi = "10";

        JbodStorage jbodStorage = new JbodStorageBuilder().withVolumes(
            new PersistentClaimStorageBuilder().withDeleteClaim(false).withId(0).withSize(diskSizeGi + "Gi").build(),
            new PersistentClaimStorageBuilder().withDeleteClaim(false).withId(1).withSize(diskSizeGi + "Gi").build()).build();

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaJBOD(clusterName, kafkaReplicas, jbodStorage).build());
        // kafka cluster already deployed
        verifyVolumeNamesAndLabels(namespaceName, clusterName, kafkaReplicas, 2, diskSizeGi);

        int volumesCount = kubeClient(namespaceName).listPersistentVolumeClaims(namespaceName, clusterName).size();

        LOGGER.info("Deleting cluster");
        cmdKubeClient(namespaceName).deleteByName("kafka", clusterName);

        LOGGER.info("Waiting for PVC deletion");
        PersistentVolumeClaimUtils.waitForJbodStorageDeletion(namespaceName, volumesCount, jbodStorage, clusterName);
    }

    @ParallelNamespaceTest
    @Tag(INTERNAL_CLIENTS_USED)
    @KRaftNotSupported("JBOD is not supported by KRaft mode and is used in this test case.")
    void testPersistentStorageSize(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);

        final String[] diskSizes = {"70Gi", "20Gi"};
        final int kafkaRepl = 2;
        final int diskCount = 2;

        JbodStorage jbodStorage =  new JbodStorageBuilder()
                .withVolumes(
                        new PersistentClaimStorageBuilder().withDeleteClaim(false).withId(0).withSize(diskSizes[0]).build(),
                        new PersistentClaimStorageBuilder().withDeleteClaim(false).withId(1).withSize(diskSizes[1]).build()
                ).build();

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), kafkaRepl)
            .editSpec()
                .editKafka()
                    .withStorage(jbodStorage)
                .endKafka()
                .editZookeeper().
                    withReplicas(1)
                .endZookeeper()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName()).build());

        List<PersistentVolumeClaim> volumes = kubeClient(testStorage.getNamespaceName()).listPersistentVolumeClaims(testStorage.getNamespaceName(), testStorage.getClusterName()).stream().filter(
            persistentVolumeClaim -> persistentVolumeClaim.getMetadata().getName().contains(testStorage.getClusterName())).collect(Collectors.toList());

        checkStorageSizeForVolumes(volumes, diskSizes, kafkaRepl, diskCount);

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withNamespaceName(testStorage.getNamespaceName())
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withMessageCount(testStorage.getMessageCount())
            .build();

        resourceManager.createResource(extensionContext,
            kafkaClients.producerStrimzi(),
            kafkaClients.consumerStrimzi()
        );

        ClientUtils.waitForClientsSuccess(testStorage);
    }

    @ParallelNamespaceTest
    @Tag(LOADBALANCER_SUPPORTED)
    void testRegenerateCertExternalAddressChange(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaStatefulSetName(clusterName));

        LOGGER.info("Creating kafka without external listener");
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3, 1).build());

        final String brokerSecret = clusterName + "-kafka-brokers";

        Secret secretsWithoutExt = kubeClient(namespaceName).getSecret(namespaceName, brokerSecret);

        LOGGER.info("Editing kafka with external listener");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {
            List<GenericKafkaListener> lst = asList(
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
                            .withNewConfiguration()
                                .withFinalizers(LB_FINALIZERS)
                            .endConfiguration()
                            .build()
            );
            kafka.getSpec().getKafka().setListeners(lst);
        }, namespaceName);

        RollingUpdateUtils.waitTillComponentHasRolled(namespaceName, kafkaSelector, 3, PodUtils.podSnapshot(namespaceName, kafkaSelector));

        Secret secretsWithExt = kubeClient(namespaceName).getSecret(namespaceName, brokerSecret);

        LOGGER.info("Checking secrets");
        kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, KafkaResources.kafkaStatefulSetName(clusterName)).forEach(kafkaPod -> {
            String kafkaPodName = kafkaPod.getMetadata().getName();
            assertThat(secretsWithExt.getData().get(kafkaPodName + ".crt"), is(not(secretsWithoutExt.getData().get(kafkaPodName + ".crt"))));
            assertThat(secretsWithExt.getData().get(kafkaPodName + ".key"), is(not(secretsWithoutExt.getData().get(kafkaPodName + ".key"))));
        });
    }

    @ParallelNamespaceTest
    @SuppressWarnings({"checkstyle:MethodLength"})
    @Tag(INTERNAL_CLIENTS_USED)
    void testLabelModificationDoesNotBreakCluster(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        final int kafkaReplicas = 3;

        Map<String, String> labels = new HashMap<>();
        final String[] labelKeys = {"label-name-1", "label-name-2", ""};
        final String[] labelValues = {"name-of-the-label-1", "name-of-the-label-2", ""};

        labels.put(labelKeys[0], labelValues[0]);
        labels.put(labelKeys[1], labelValues[1]);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), kafkaReplicas, 1)
            .editMetadata()
                .withLabels(labels)
            .endMetadata()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName()).build());

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withNamespaceName(testStorage.getNamespaceName())
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withMessageCount(testStorage.getMessageCount())
            .build();

        resourceManager.createResource(extensionContext,
            kafkaClients.producerStrimzi(),
            kafkaClients.consumerStrimzi()
        );
        ClientUtils.waitForClientsSuccess(testStorage);

        Map<String, String> zkPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getZookeeperSelector());
        Map<String, String> kafkaPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getKafkaSelector());

        LOGGER.info("Waiting for kafka stateful set labels changed {}", labels);
        StUtils.waitForStatefulSetOrStrimziPodSetLabelsChange(testStorage.getNamespaceName(), testStorage.getKafkaStatefulSetName(), labels);

        LOGGER.info("Getting labels from stateful set resource");
        Map<String, String> kafkaLabels = StUtils.getLabelsOfStatefulSetOrStrimziPodSet(testStorage.getNamespaceName(), testStorage.getKafkaStatefulSetName());

        LOGGER.info("Verifying default labels in the Kafka CR");

        assertThat("Label exists in stateful set with concrete value",
                labelValues[0].equals(kafkaLabels.get(labelKeys[0])));
        assertThat("Label exists in stateful set with concrete value",
                labelValues[1].equals(kafkaLabels.get(labelKeys[1])));

        labelValues[0] = "new-name-of-the-label-1";
        labelValues[1] = "new-name-of-the-label-2";
        labelKeys[2] = "label-name-3";
        labelValues[2] = "name-of-the-label-3";
        LOGGER.info("Setting new values of labels from {} to {} | from {} to {} and adding one {} with value {}",
                "name-of-the-label-1", labelValues[0], "name-of-the-label-2", labelValues[1], labelKeys[2], labelValues[2]);

        LOGGER.info("Edit kafka labels in Kafka CR");
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), resource -> {
            resource.getMetadata().getLabels().put(labelKeys[0], labelValues[0]);
            resource.getMetadata().getLabels().put(labelKeys[1], labelValues[1]);
            resource.getMetadata().getLabels().put(labelKeys[2], labelValues[2]);
        }, testStorage.getNamespaceName());

        if (!Environment.isKRaftModeEnabled()) {
            zkPods = RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getZookeeperSelector(), 1, zkPods);
        }
        kafkaPods = RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getKafkaSelector(), 3, kafkaPods);

        labels.put(labelKeys[0], labelValues[0]);
        labels.put(labelKeys[1], labelValues[1]);
        labels.put(labelKeys[2], labelValues[2]);

        LOGGER.info("Waiting for kafka service labels changed {}", labels);
        ServiceUtils.waitForServiceLabelsChange(testStorage.getNamespaceName(), KafkaResources.brokersServiceName(testStorage.getClusterName()), labels);

        LOGGER.info("Verifying kafka labels via services");
        Service service = kubeClient(testStorage.getNamespaceName()).getService(testStorage.getNamespaceName(), KafkaResources.brokersServiceName(testStorage.getClusterName()));

        verifyPresentLabels(labels, service.getMetadata().getLabels());

        for (String cmName : StUtils.getKafkaConfigurationConfigMaps(testStorage.getClusterName(), kafkaReplicas)) {
            LOGGER.info("Waiting for Kafka ConfigMap {}/{} to have new labels: {}", testStorage.getNamespaceName(), cmName, labels);
            ConfigMapUtils.waitForConfigMapLabelsChange(testStorage.getNamespaceName(), cmName, labels);

            LOGGER.info("Verifying Kafka labels on ConfigMap {}/{}", testStorage.getNamespaceName(), cmName);
            ConfigMap configMap = kubeClient(testStorage.getNamespaceName()).getConfigMap(testStorage.getNamespaceName(), cmName);

            verifyPresentLabels(labels, configMap.getMetadata().getLabels());
        }

        LOGGER.info("Waiting for kafka stateful set labels changed {}", labels);
        StUtils.waitForStatefulSetOrStrimziPodSetLabelsChange(testStorage.getNamespaceName(), testStorage.getKafkaStatefulSetName(), labels);

        LOGGER.info("Verifying kafka labels via stateful set");

        verifyPresentLabels(labels, StUtils.getLabelsOfStatefulSetOrStrimziPodSet(testStorage.getNamespaceName(), testStorage.getKafkaStatefulSetName()));

        LOGGER.info("Verifying via kafka pods");
        labels = kubeClient(testStorage.getNamespaceName()).getPod(testStorage.getNamespaceName(), KafkaResources.kafkaPodName(testStorage.getClusterName(), 0)).getMetadata().getLabels();

        assertThat("Label exists in kafka pods", labelValues[0].equals(labels.get(labelKeys[0])));
        assertThat("Label exists in kafka pods", labelValues[1].equals(labels.get(labelKeys[1])));
        assertThat("Label exists in kafka pods", labelValues[2].equals(labels.get(labelKeys[2])));

        LOGGER.info("Removing labels: {} -> {}, {} -> {}, {} -> {}", labelKeys[0], labels.get(labelKeys[0]),
            labelKeys[1], labels.get(labelKeys[1]), labelKeys[2], labels.get(labelKeys[2]));

        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getClusterName(), resource -> {
            resource.getMetadata().getLabels().remove(labelKeys[0]);
            resource.getMetadata().getLabels().remove(labelKeys[1]);
            resource.getMetadata().getLabels().remove(labelKeys[2]);
        }, testStorage.getNamespaceName());

        if (!Environment.isKRaftModeEnabled()) {
            RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getZookeeperSelector(), 1, zkPods);
        }
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getKafkaSelector(), 3, kafkaPods);

        labels.remove(labelKeys[0]);
        labels.remove(labelKeys[1]);
        labels.remove(labelKeys[2]);

        LOGGER.info("Waiting for kafka service labels deletion {}", labels.toString());
        ServiceUtils.waitForServiceLabelsDeletion(testStorage.getNamespaceName(), KafkaResources.brokersServiceName(testStorage.getClusterName()), labelKeys[0], labelKeys[1], labelKeys[2]);

        LOGGER.info("Verifying kafka labels via services");
        service = kubeClient(testStorage.getNamespaceName()).getService(testStorage.getNamespaceName(), KafkaResources.brokersServiceName(testStorage.getClusterName()));

        verifyNullLabels(labelKeys, service);

        for (String cmName : StUtils.getKafkaConfigurationConfigMaps(testStorage.getClusterName(), kafkaReplicas)) {
            LOGGER.info("Waiting for Kafka ConfigMap {}/{} to have labels removed: {}", testStorage.getNamespaceName(), cmName, labelKeys);
            ConfigMapUtils.waitForConfigMapLabelsDeletion(testStorage.getNamespaceName(), cmName, labelKeys[0], labelKeys[1], labelKeys[2]);

            LOGGER.info("Verifying Kafka labels on ConfigMap {}/{}", testStorage.getNamespaceName(), cmName);
            ConfigMap configMap = kubeClient(testStorage.getNamespaceName()).getConfigMap(testStorage.getNamespaceName(), cmName);

            verifyNullLabels(labelKeys, configMap);
        }

        LOGGER.info("Waiting for kafka stateful set labels changed {}", labels);
        StUtils.waitForStatefulSetOrStrimziPodSetLabelsDeletion(testStorage.getNamespaceName(), testStorage.getKafkaStatefulSetName(), labelKeys[0], labelKeys[1], labelKeys[2]);

        LOGGER.info("Verifying kafka labels via stateful set");
        verifyNullLabels(labelKeys, StUtils.getLabelsOfStatefulSetOrStrimziPodSet(testStorage.getNamespaceName(), testStorage.getKafkaStatefulSetName()));

        LOGGER.info("Waiting for kafka pod labels deletion {}", labels.toString());
        PodUtils.waitUntilPodLabelsDeletion(testStorage.getNamespaceName(), KafkaResources.kafkaPodName(testStorage.getClusterName(), 0), labelKeys[0], labelKeys[1], labelKeys[2]);

        labels = kubeClient(testStorage.getNamespaceName()).getPod(testStorage.getNamespaceName(), KafkaResources.kafkaPodName(testStorage.getClusterName(), 0)).getMetadata().getLabels();

        LOGGER.info("Verifying via kafka pods");
        verifyNullLabels(labelKeys, labels);

        resourceManager.createResource(extensionContext,
            kafkaClients.producerStrimzi(),
            kafkaClients.consumerStrimzi()
        );
        ClientUtils.waitForClientsSuccess(testStorage);
    }

    @ParallelNamespaceTest
    @Tag(INTERNAL_CLIENTS_USED)
    @KRaftNotSupported("TopicOperator is not supported by KRaft mode and is used in this test class")
    void testAppDomainLabels(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3, 1).build());
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName()).build());

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withNamespaceName(testStorage.getNamespaceName())
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withMessageCount(testStorage.getMessageCount())
            .build();

        LOGGER.info("---> PODS <---");

        List<Pod> pods = kubeClient(testStorage.getNamespaceName()).listPods(testStorage.getNamespaceName(), testStorage.getClusterName()).stream()
                .filter(pod -> pod.getMetadata().getName().startsWith(testStorage.getClusterName()))
                .collect(Collectors.toList());

        for (Pod pod : pods) {
            LOGGER.info("Getting labels from {} pod", pod.getMetadata().getName());
            verifyAppLabels(pod.getMetadata().getLabels());
        }

        LOGGER.info("---> STATEFUL SETS <---");

        Map<String, String> kafkaLabels = StUtils.getLabelsOfStatefulSetOrStrimziPodSet(testStorage.getNamespaceName(), testStorage.getKafkaStatefulSetName());

        LOGGER.info("Getting labels from stateful set of kafka resource");
        verifyAppLabels(kafkaLabels);

        Map<String, String> zooLabels = StUtils.getLabelsOfStatefulSetOrStrimziPodSet(testStorage.getNamespaceName(), testStorage.getZookeeperStatefulSetName());

        LOGGER.info("Getting labels from stateful set of zookeeper resource");
        verifyAppLabels(zooLabels);

        LOGGER.info("---> SERVICES <---");

        List<Service> services = kubeClient(testStorage.getNamespaceName()).listServices(testStorage.getNamespaceName()).stream()
                .filter(service -> service.getMetadata().getName().startsWith(testStorage.getClusterName()))
                .collect(Collectors.toList());

        for (Service service : services) {
            LOGGER.info("Getting labels from {} service", service.getMetadata().getName());
            verifyAppLabels(service.getMetadata().getLabels());
        }

        LOGGER.info("---> SECRETS <---");

        List<Secret> secrets = kubeClient(testStorage.getNamespaceName()).listSecrets(testStorage.getNamespaceName()).stream()
                .filter(secret -> secret.getMetadata().getName().startsWith(testStorage.getClusterName()) && secret.getType().equals("Opaque"))
                .collect(Collectors.toList());

        for (Secret secret : secrets) {
            LOGGER.info("Getting labels from {} secret", secret.getMetadata().getName());
            verifyAppLabelsForSecretsAndConfigMaps(secret.getMetadata().getLabels());
        }

        LOGGER.info("---> CONFIG MAPS <---");

        List<ConfigMap> configMaps = kubeClient(testStorage.getNamespaceName()).listConfigMapsInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName());

        for (ConfigMap configMap : configMaps) {
            LOGGER.info("Getting labels from {} config map", configMap.getMetadata().getName());
            verifyAppLabelsForSecretsAndConfigMaps(configMap.getMetadata().getLabels());
        }

        resourceManager.createResource(extensionContext,
            kafkaClients.producerStrimzi(),
            kafkaClients.consumerStrimzi()
        );
        ClientUtils.waitForClientsSuccess(testStorage);
    }

    @ParallelNamespaceTest
    void testUOListeningOnlyUsersInSameCluster(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String userName = mapWithTestUsers.get(extensionContext.getDisplayName());
        final String firstClusterName = "my-cluster-1";
        final String secondClusterName = "my-cluster-2";

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(firstClusterName, 3, 1).build());
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(secondClusterName, 3, 1).build());
        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(namespaceName, firstClusterName, userName).build());

        LOGGER.info("Verifying that KafkaUser {} in cluster {} is created", userName, firstClusterName);
        String entityOperatorPodName = kubeClient(namespaceName).listPodNamesInSpecificNamespace(namespaceName, Labels.STRIMZI_NAME_LABEL, KafkaResources.entityOperatorDeploymentName(firstClusterName)).get(0);
        String uOLogs = kubeClient(namespaceName).logsInSpecificNamespace(namespaceName, entityOperatorPodName, "user-operator");
        assertThat(uOLogs, containsString("KafkaUser " + userName + " in namespace " + namespaceName + " was ADDED"));

        LOGGER.info("Verifying that KafkaUser {} in cluster {} is not created", userName, secondClusterName);
        entityOperatorPodName = kubeClient(namespaceName).listPodNamesInSpecificNamespace(namespaceName, Labels.STRIMZI_NAME_LABEL, KafkaResources.entityOperatorDeploymentName(secondClusterName)).get(0);
        uOLogs = kubeClient(namespaceName).logsInSpecificNamespace(namespaceName, entityOperatorPodName, "user-operator");
        assertThat(uOLogs, not(containsString("KafkaUser " + userName + " in namespace " + namespaceName + " was ADDED")));

        LOGGER.info("Verifying that KafkaUser belongs to {} cluster", firstClusterName);
        String kafkaUserResource = cmdKubeClient(namespaceName).getResourceAsYaml("kafkauser", userName);
        assertThat(kafkaUserResource, containsString(Labels.STRIMZI_CLUSTER_LABEL + ": " + firstClusterName));
    }

    @ParallelNamespaceTest
    @Tag(INTERNAL_CLIENTS_USED)
    @KRaftNotSupported("TopicOperator is not supported by KRaft mode and is used in this test class")
    void testMessagesAreStoredInDisk(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(testStorage.getClusterName(), testStorage.getKafkaStatefulSetName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 1, 1).build());

        Map<String, String> kafkaPodsSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), kafkaSelector);

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName(), 1, 1).build());

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withNamespaceName(testStorage.getNamespaceName())
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withMessageCount(testStorage.getMessageCount())
            .build();

        TestUtils.waitFor("KafkaTopic creation inside kafka pod", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient(testStorage.getNamespaceName()).execInPod(KafkaResources.kafkaPodName(testStorage.getClusterName(), 0), "/bin/bash",
                        "-c", "cd /var/lib/kafka/data/kafka-log0; ls -1").out().contains(testStorage.getTopicName()));

        String topicDirNameInPod = cmdKubeClient(testStorage.getNamespaceName()).execInPod(KafkaResources.kafkaPodName(testStorage.getClusterName(), 0), "/bin/bash",
                "-c", "cd /var/lib/kafka/data/kafka-log0; ls -1 | sed -n '/" + testStorage.getTopicName() + "/p'").out();

        String commandToGetDataFromTopic =
                "cd /var/lib/kafka/data/kafka-log0/" + topicDirNameInPod + "/;cat 00000000000000000000.log";

        LOGGER.info("Executing command {} in {}", commandToGetDataFromTopic, KafkaResources.kafkaPodName(testStorage.getClusterName(), 0));
        String topicData = cmdKubeClient(testStorage.getNamespaceName()).execInPod(KafkaResources.kafkaPodName(testStorage.getClusterName(), 0),
                "/bin/bash", "-c", commandToGetDataFromTopic).out();

        LOGGER.info("Topic {} is present in kafka broker {} with no data", testStorage.getTopicName(), KafkaResources.kafkaPodName(testStorage.getClusterName(), 0));
        assertThat("Topic contains data", topicData, emptyOrNullString());

        resourceManager.createResource(extensionContext,
            kafkaClients.producerStrimzi(),
            kafkaClients.consumerStrimzi()
        );
        ClientUtils.waitForClientsSuccess(testStorage);

        LOGGER.info("Executing command {} in {}", commandToGetDataFromTopic, KafkaResources.kafkaPodName(testStorage.getClusterName(), 0));
        topicData = cmdKubeClient(testStorage.getNamespaceName()).execInPod(KafkaResources.kafkaPodName(testStorage.getClusterName(), 0), "/bin/bash", "-c",
                commandToGetDataFromTopic).out();

        assertThat("Topic has no data", topicData, notNullValue());

        List<Pod> kafkaPods = kubeClient(testStorage.getNamespaceName()).listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getKafkaStatefulSetName());

        for (Pod kafkaPod : kafkaPods) {
            LOGGER.info("Deleting kafka pod {}", kafkaPod.getMetadata().getName());
            kubeClient(testStorage.getNamespaceName()).deletePod(testStorage.getNamespaceName(), kafkaPod);
        }

        LOGGER.info("Wait for kafka to rolling restart ...");
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), kafkaSelector, 1, kafkaPodsSnapshot);

        LOGGER.info("Executing command {} in {}", commandToGetDataFromTopic, KafkaResources.kafkaPodName(testStorage.getClusterName(), 0));
        topicData = cmdKubeClient(testStorage.getNamespaceName()).execInPod(KafkaResources.kafkaPodName(testStorage.getClusterName(), 0), "/bin/bash", "-c",
                commandToGetDataFromTopic).out();

        assertThat("Topic has no data", topicData, notNullValue());
    }

    @ParallelNamespaceTest
    @Tag(INTERNAL_CLIENTS_USED)
    void testConsumerOffsetFiles(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        final Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put("offsets.topic.replication.factor", "3");
        kafkaConfig.put("offsets.topic.num.partitions", "100");

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3, 1)
            .editSpec()
                .editKafka()
                    .withConfig(kafkaConfig)
                .endKafka()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName(), 3, 1).build());

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withNamespaceName(testStorage.getNamespaceName())
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withMessageCount(testStorage.getMessageCount())
            .build();

        String commandToGetFiles =  "cd /var/lib/kafka/data/kafka-log0/;" +
                "ls -1 | sed -n \"s#__consumer_offsets-\\([0-9]*\\)#\\1#p\" | sort -V";

        LOGGER.info("Executing command {} in {}", commandToGetFiles, KafkaResources.kafkaPodName(testStorage.getClusterName(), 0));
        String result = cmdKubeClient(testStorage.getNamespaceName()).execInPod(KafkaResources.kafkaPodName(testStorage.getClusterName(), 0),
                "/bin/bash", "-c", commandToGetFiles).out();

        // TODO / FIXME
        //assertThat("Folder kafka-log0 has data in files:\n" + result, result.equals(""));

        LOGGER.info("Result: \n" + result);

        resourceManager.createResource(extensionContext,
            kafkaClients.producerStrimzi(),
            kafkaClients.consumerStrimzi()
        );
        ClientUtils.waitForClientsSuccess(testStorage);

        LOGGER.info("Executing command {} in {}", commandToGetFiles, KafkaResources.kafkaPodName(testStorage.getClusterName(), 0));
        result = cmdKubeClient(testStorage.getNamespaceName()).execInPod(KafkaResources.kafkaPodName(testStorage.getClusterName(), 0),
                "/bin/bash", "-c", commandToGetFiles).out();

        StringBuilder stringToMatch = new StringBuilder();

        for (int i = 0; i < 100; i++) {
            stringToMatch.append(i).append("\n");
        }

        assertThat("Folder kafka-log0 doesn't contain 100 files", result, containsString(stringToMatch.toString()));
    }


    @ParallelNamespaceTest
    @KRaftNotSupported("JBOD is not supported by KRaft mode and is used in this test case.")
    void testLabelsAndAnnotationForPVC(ExtensionContext extensionContext) {
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String labelAnnotationKey = "testKey";
        final String firstValue = "testValue";
        final String changedValue = "editedTestValue";

        Map<String, String> pvcLabel = new HashMap<>();
        pvcLabel.put(labelAnnotationKey, firstValue);
        Map<String, String> pvcAnnotation = pvcLabel;

        Map<String, String> statefulSetLabels = new HashMap<>();
        statefulSetLabels.put("app.kubernetes.io/part-of", "some-app");
        statefulSetLabels.put("app.kubernetes.io/managed-by", "some-app");

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3, 1)
            .editSpec()
                .editKafka()
                    .withNewTemplate()
                        .withNewStatefulset()
                            .withNewMetadata()
                                .withLabels(statefulSetLabels)
                            .endMetadata()
                        .endStatefulset()
                        .withNewPodSet()
                            .withNewMetadata()
                                .withLabels(statefulSetLabels)
                            .endMetadata()
                        .endPodSet()
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
                        .withSize("3Gi")
                    .endPersistentClaimStorage()
                .endZookeeper()
            .endSpec()
            .build());

        LOGGER.info("Check if Kubernetes labels are applied");

        Map<String, String> actualStatefulSetLabels = StUtils.getLabelsOfStatefulSetOrStrimziPodSet(namespaceName, KafkaResources.kafkaStatefulSetName(clusterName));

        assertThat(actualStatefulSetLabels.get("app.kubernetes.io/part-of"), is("some-app"));
        assertThat(actualStatefulSetLabels.get("app.kubernetes.io/managed-by"), is("some-app"));

        LOGGER.info("Kubernetes labels are correctly set and present");

        List<PersistentVolumeClaim> pvcs = kubeClient(namespaceName).listPersistentVolumeClaims(namespaceName, clusterName).stream().filter(
            persistentVolumeClaim -> persistentVolumeClaim.getMetadata().getName().contains(clusterName)).collect(Collectors.toList());

        assertThat(pvcs.size(), is(7));

        for (PersistentVolumeClaim pvc : pvcs) {
            LOGGER.info("Verifying that PVC label {} - {} = {}", pvc.getMetadata().getName(), firstValue, pvc.getMetadata().getLabels().get(labelAnnotationKey));

            assertThat(firstValue, is(pvc.getMetadata().getLabels().get(labelAnnotationKey)));
            assertThat(firstValue, is(pvc.getMetadata().getAnnotations().get(labelAnnotationKey)));
        }

        pvcLabel.put(labelAnnotationKey, changedValue);
        pvcAnnotation.put(labelAnnotationKey, changedValue);

        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, kafka -> {
            LOGGER.info("Replacing kafka && zookeeper labels and annotations from {} to {}", labelAnnotationKey, changedValue);
            kafka.getSpec().getKafka().getTemplate().getPersistentVolumeClaim().getMetadata().setLabels(pvcLabel);
            kafka.getSpec().getKafka().getTemplate().getPersistentVolumeClaim().getMetadata().setAnnotations(pvcAnnotation);
            kafka.getSpec().getZookeeper().getTemplate().getPersistentVolumeClaim().getMetadata().setLabels(pvcLabel);
            kafka.getSpec().getZookeeper().getTemplate().getPersistentVolumeClaim().getMetadata().setAnnotations(pvcAnnotation);
        }, namespaceName);

        PersistentVolumeClaimUtils.waitUntilPVCLabelsChange(namespaceName, clusterName, pvcLabel, labelAnnotationKey);
        PersistentVolumeClaimUtils.waitUntilPVCAnnotationChange(namespaceName, clusterName, pvcAnnotation, labelAnnotationKey);
        KafkaUtils.waitForKafkaReady(namespaceName, clusterName);

        pvcs = kubeClient(namespaceName).listPersistentVolumeClaims(namespaceName, clusterName).stream().filter(
            persistentVolumeClaim -> persistentVolumeClaim.getMetadata().getName().contains(clusterName)).collect(Collectors.toList());

        LOGGER.info(pvcs.toString());

        assertThat(pvcs.size(), is(7));

        for (PersistentVolumeClaim pvc : pvcs) {
            LOGGER.info("Verifying replaced PVC label {} - {} = {}", pvc.getMetadata().getName(), firstValue, pvc.getMetadata().getLabels().get(labelAnnotationKey));

            assertThat(pvc.getMetadata().getLabels().get(labelAnnotationKey), is(changedValue));
            assertThat(pvc.getMetadata().getAnnotations().get(labelAnnotationKey), is(changedValue));
        }
    }

    @ParallelNamespaceTest
    @Tag(INTERNAL_CLIENTS_USED)
    @Tag(CRUISE_CONTROL)
    void testReadOnlyRootFileSystem(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);

        Kafka kafka = KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3, 3)
                .editSpec()
                    .editKafka()
                        .withNewTemplate()
                            .withNewKafkaContainer()
                                .withSecurityContext(new SecurityContextBuilder().withReadOnlyRootFilesystem(true).build())
                            .endKafkaContainer()
                        .endTemplate()
                    .endKafka()
                    .editZookeeper()
                        .withNewTemplate()
                            .withNewZookeeperContainer()
                                .withSecurityContext(new SecurityContextBuilder().withReadOnlyRootFilesystem(true).build())
                            .endZookeeperContainer()
                        .endTemplate()
                    .endZookeeper()
                    .editEntityOperator()
                        .withNewTemplate()
                            .withNewTlsSidecarContainer()
                                .withSecurityContext(new SecurityContextBuilder().withReadOnlyRootFilesystem(true).build())
                            .endTlsSidecarContainer()
                            .withNewTopicOperatorContainer()
                                .withSecurityContext(new SecurityContextBuilder().withReadOnlyRootFilesystem(true).build())
                            .endTopicOperatorContainer()
                            .withNewUserOperatorContainer()
                                .withSecurityContext(new SecurityContextBuilder().withReadOnlyRootFilesystem(true).build())
                            .endUserOperatorContainer()
                        .endTemplate()
                    .endEntityOperator()
                    .editOrNewKafkaExporter()
                        .withNewTemplate()
                            .withNewContainer()
                                .withSecurityContext(new SecurityContextBuilder().withReadOnlyRootFilesystem(true).build())
                            .endContainer()
                        .endTemplate()
                    .endKafkaExporter()
                    .editOrNewCruiseControl()
                        .withNewTemplate()
                            .withNewTlsSidecarContainer()
                                .withSecurityContext(new SecurityContextBuilder().withReadOnlyRootFilesystem(true).build())
                            .endTlsSidecarContainer()
                            .withNewCruiseControlContainer()
                                .withSecurityContext(new SecurityContextBuilder().withReadOnlyRootFilesystem(true).build())
                            .endCruiseControlContainer()
                        .endTemplate()
                    .endCruiseControl()
                .endSpec()
                .build();

        kafka.getSpec().getEntityOperator().getTemplate().setTopicOperatorContainer(null);

        resourceManager.createResource(extensionContext, kafka);

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName()).build());

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(testStorage.getTopicName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withNamespaceName(testStorage.getNamespaceName())
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withMessageCount(testStorage.getMessageCount())
            .build();

        resourceManager.createResource(extensionContext,
            kafkaClients.producerStrimzi(),
            kafkaClients.consumerStrimzi()
        );
        ClientUtils.waitForClientsSuccess(testStorage);
    }

    protected void checkKafkaConfiguration(String namespaceName, String podNamePrefix, Map<String, Object> config, String clusterName) {
        LOGGER.info("Checking kafka configuration");
        List<Pod> pods = kubeClient(namespaceName).listPodsByPrefixInName(namespaceName, podNamePrefix);

        for (String cmName : StUtils.getKafkaConfigurationConfigMaps(clusterName, pods.size())) {
            Properties properties = configMap2Properties(kubeClient(namespaceName).getConfigMap(namespaceName, cmName));

            for (Map.Entry<String, Object> property : config.entrySet()) {
                String key = property.getKey();
                Object val = property.getValue();

                assertThat(properties.keySet().contains(key), is(true));
                assertThat(properties.getProperty(key), is(val));
            }
        }

        for (Pod pod: pods) {
            ExecResult result = cmdKubeClient(namespaceName).execInPod(pod.getMetadata().getName(), "/bin/bash", "-c", "cat /tmp/strimzi.properties");
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
                LOGGER.info("Checking volume {} and size of testStorage {}", volumes.get(k).getMetadata().getName(),
                        volumes.get(k).getSpec().getResources().getRequests().get("storage"));
                assertThat(volumes.get(k).getSpec().getResources().getRequests().get("storage"), is(new Quantity(diskSizes[i])));
                k++;
            }
        }
    }

    void verifyVolumeNamesAndLabels(String namespaceName, String clusterName, int kafkaReplicas, int diskCountPerReplica, String diskSizeGi) {
        ArrayList<String> pvcs = new ArrayList<>();

        kubeClient(namespaceName).listPersistentVolumeClaims(namespaceName, clusterName).stream()
            .filter(pvc -> pvc.getMetadata().getName().contains(clusterName + "-kafka"))
            .forEach(volume -> {
                String volumeName = volume.getMetadata().getName();
                pvcs.add(volumeName);
                LOGGER.info("Checking labels for volume:" + volumeName);
                assertThat(volume.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is(clusterName));
                assertThat(volume.getMetadata().getLabels().get(Labels.STRIMZI_KIND_LABEL), is(Kafka.RESOURCE_KIND));
                assertThat(volume.getMetadata().getLabels().get(Labels.STRIMZI_NAME_LABEL), is(clusterName.concat("-kafka")));
                assertThat(volume.getSpec().getResources().getRequests().get("storage"), is(new Quantity(diskSizeGi, "Gi")));
            });

        LOGGER.info("Checking PVC names included in JBOD array");
        for (int i = 0; i < kafkaReplicas; i++) {
            for (int j = 0; j < diskCountPerReplica; j++) {
                assertThat(pvcs.contains("data-" + j + "-" + clusterName + "-kafka-" + i), is(true));
            }
        }

        LOGGER.info("Checking PVC on Kafka pods");
        for (int i = 0; i < kafkaReplicas; i++) {
            ArrayList<String> dataSourcesOnPod = new ArrayList<>();
            ArrayList<String> pvcsOnPod = new ArrayList<>();

            LOGGER.info("Getting list of mounted data sources and PVCs on Kafka pod " + i);
            for (int j = 0; j < diskCountPerReplica; j++) {
                dataSourcesOnPod.add(kubeClient(namespaceName).getPod(namespaceName, clusterName.concat("-kafka-" + i))
                        .getSpec().getVolumes().get(j).getName());
                pvcsOnPod.add(kubeClient(namespaceName).getPod(namespaceName, clusterName.concat("-kafka-" + i))
                        .getSpec().getVolumes().get(j).getPersistentVolumeClaim().getClaimName());
            }

            LOGGER.info("Verifying mounted data sources and PVCs on Kafka pod " + i);
            for (int j = 0; j < diskCountPerReplica; j++) {
                assertThat(dataSourcesOnPod.contains("data-" + j), is(true));
                assertThat(pvcsOnPod.contains("data-" + j + "-" + clusterName + "-kafka-" + i), is(true));
            }
        }
    }

    void verifyPresentLabels(Map<String, String> labels, Map<String, String> resourceLabels) {
        for (Map.Entry<String, String> label : labels.entrySet()) {
            assertThat("Label exists with concrete value in HasMetadata(Services, CM, STS) resources",
                    label.getValue().equals(resourceLabels.get(label.getKey())));
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

    protected void afterEachMayOverride(ExtensionContext extensionContext) throws Exception {
        resourceManager.deleteResources(extensionContext);

        final String namespaceName = StUtils.getNamespaceBasedOnRbac(namespace, extensionContext);

        if (KafkaResource.kafkaClient().inNamespace(namespaceName).withName(OPENSHIFT_CLUSTER_NAME).get() != null) {
            cmdKubeClient(namespaceName).deleteByName(Kafka.RESOURCE_KIND, OPENSHIFT_CLUSTER_NAME);
        }

        kubeClient(namespaceName).listPods(namespaceName).stream()
            .filter(p -> p.getMetadata().getName().startsWith(OPENSHIFT_CLUSTER_NAME))
            .forEach(p -> PodUtils.deletePodWithWait(p.getMetadata().getNamespace(), p.getMetadata().getName()));

        kubeClient(namespaceName).getClient().resources(KafkaTopic.class, KafkaTopicList.class).inNamespace(namespaceName).delete();
        kubeClient(namespaceName).getClient().persistentVolumeClaims().inNamespace(namespaceName).delete();

        testSuiteNamespaceManager.deleteParallelNamespace(extensionContext);
    }
}
