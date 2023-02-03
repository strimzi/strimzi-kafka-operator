/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.BeforeAllOnce;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.IsolatedSuite;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.JobUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.annotations.IsolatedTest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static io.strimzi.systemtest.Constants.INFRA_NAMESPACE;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * Feature Gates should give us additional options on
 * how to control and mature different behaviors in the operators.
 * https://github.com/strimzi/proposals/blob/main/022-feature-gates.md
 */
@Tag(REGRESSION)
@IsolatedSuite
public class FeatureGatesIsolatedST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(FeatureGatesIsolatedST.class);

    /**
     * UseStrimziPodSets feature gate
     * https://github.com/strimzi/proposals/blob/main/031-statefulset-removal.md
     */
    @IsolatedTest("Feature Gates test for enabled UseStrimziPodSets gate")
    @Tag(INTERNAL_CLIENTS_USED)
    public void testDisabledStrimziPodSetsFeatureGate(ExtensionContext extensionContext) {
        assumeFalse(Environment.isOlmInstall() || Environment.isHelmInstall());

        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String producerName = "producer-test-" + new Random().nextInt(Integer.MAX_VALUE);
        final String consumerName = "consumer-test-" + new Random().nextInt(Integer.MAX_VALUE);
        final String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

        final LabelSelector zooSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.zookeeperStatefulSetName(clusterName));
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaStatefulSetName(clusterName));

        int messageCount = 600;
        List<EnvVar> testEnvVars = new ArrayList<>();
        int zooReplicas = 3;
        int kafkaReplicas = 3;

        testEnvVars.add(new EnvVar(Environment.STRIMZI_FEATURE_GATES_ENV, "-UseStrimziPodSets", null));

        clusterOperator.unInstall();
        clusterOperator = new SetupClusterOperator.SetupClusterOperatorBuilder()
                .withExtensionContext(BeforeAllOnce.getSharedExtensionContext())
                .withNamespace(INFRA_NAMESPACE)
                .withWatchingNamespaces(Constants.WATCH_ALL_NAMESPACES)
                .withExtraEnvVars(testEnvVars)
                .createInstallation()
                .runInstallation();

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, kafkaReplicas).build());

        LOGGER.info("Try to send some messages to Kafka over next few minutes.");
        KafkaTopic kafkaTopic = KafkaTopicTemplates.topic(clusterName, topicName)
                .editSpec()
                    .withReplicas(kafkaReplicas)
                    .withPartitions(kafkaReplicas)
                .endSpec()
                .build();
        resourceManager.createResource(extensionContext, kafkaTopic);

        KafkaClients kafkaBasicClientJob = new KafkaClientsBuilder()
                .withProducerName(producerName)
                .withConsumerName(consumerName)
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(clusterName))
                .withTopicName(topicName)
                .withMessageCount(messageCount)
                .withDelayMs(500)
                .withNamespaceName(clusterOperator.getDeploymentNamespace())
                .build();

        resourceManager.createResource(extensionContext, kafkaBasicClientJob.producerStrimzi());
        resourceManager.createResource(extensionContext, kafkaBasicClientJob.consumerStrimzi());
        JobUtils.waitForJobRunning(consumerName, clusterOperator.getDeploymentNamespace());

        // Delete one Zoo Pod
        Pod zooPod = PodUtils.getPodsByPrefixInNameWithDynamicWait(clusterOperator.getDeploymentNamespace(), KafkaResources.zookeeperStatefulSetName(clusterName) + "-").get(0);
        LOGGER.info("Delete first found ZooKeeper pod {}", zooPod.getMetadata().getName());
        kubeClient().deletePod(clusterOperator.getDeploymentNamespace(), zooPod);
        RollingUpdateUtils.waitForComponentAndPodsReady(clusterOperator.getDeploymentNamespace(), zooSelector, zooReplicas);

        // Delete one Kafka Pod
        Pod kafkaPod = PodUtils.getPodsByPrefixInNameWithDynamicWait(clusterOperator.getDeploymentNamespace(), KafkaResources.kafkaStatefulSetName(clusterName) + "-").get(0);
        LOGGER.info("Delete first found Kafka broker pod {}", kafkaPod.getMetadata().getName());
        kubeClient().deletePod(clusterOperator.getDeploymentNamespace(), kafkaPod);
        RollingUpdateUtils.waitForComponentAndPodsReady(clusterOperator.getDeploymentNamespace(), kafkaSelector, kafkaReplicas);

        // Roll Zoo
        LOGGER.info("Force Rolling Update of ZooKeeper via annotation.");
        Map<String, String> zooPods = PodUtils.podSnapshot(clusterOperator.getDeploymentNamespace(), zooSelector);
        zooPods.keySet().forEach(podName -> {
            kubeClient(clusterOperator.getDeploymentNamespace()).editPod(podName).edit(pod -> new PodBuilder(pod)
                    .editMetadata()
                        .addToAnnotations(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")
                    .endMetadata()
                    .build());
        });

        LOGGER.info("Wait for next reconciliation to happen.");
        RollingUpdateUtils.waitTillComponentHasRolled(clusterOperator.getDeploymentNamespace(), zooSelector, zooReplicas, zooPods);

        // Roll Kafka
        LOGGER.info("Force Rolling Update of Kafka via annotation.");
        Map<String, String> kafkaPods = PodUtils.podSnapshot(clusterOperator.getDeploymentNamespace(), kafkaSelector);
        kafkaPods.keySet().forEach(podName -> {
            kubeClient(clusterOperator.getDeploymentNamespace()).editPod(podName).edit(pod -> new PodBuilder(pod)
                    .editMetadata()
                        .addToAnnotations(Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true")
                    .endMetadata()
                    .build());
        });

        LOGGER.info("Wait for next reconciliation to happen.");
        RollingUpdateUtils.waitTillComponentHasRolled(clusterOperator.getDeploymentNamespace(), kafkaSelector, kafkaReplicas, kafkaPods);

        LOGGER.info("Waiting for clients to finish sending/receiving messages.");
        ClientUtils.waitForClientSuccess(producerName, clusterOperator.getDeploymentNamespace(), MESSAGE_COUNT);
        ClientUtils.waitForClientSuccess(consumerName, clusterOperator.getDeploymentNamespace(), MESSAGE_COUNT);
    }

    @IsolatedTest
    void testSwitchingStrimziPodSetFeatureGateOnAndOff(ExtensionContext extensionContext) {
        assumeFalse(Environment.isOlmInstall() || Environment.isHelmInstall());

        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String producerName = "producer-test-" + new Random().nextInt(Integer.MAX_VALUE);
        final String consumerName = "consumer-test-" + new Random().nextInt(Integer.MAX_VALUE);
        final String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

        int zkReplicas = 1;
        int kafkaReplicas = 3;

        final LabelSelector zkSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.zookeeperStatefulSetName(clusterName));
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaStatefulSetName(clusterName));

        int messageCount = 500;

        List<EnvVar> coEnvVars = new ArrayList<>();
        coEnvVars.add(new EnvVar(Environment.STRIMZI_FEATURE_GATES_ENV, "+UseStrimziPodSets", null));

        LOGGER.info("Deploying CO with SPS - STS is disabled");

        clusterOperator.unInstall();
        clusterOperator = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(BeforeAllOnce.getSharedExtensionContext())
            .withNamespace(INFRA_NAMESPACE)
            .withWatchingNamespaces(Constants.WATCH_ALL_NAMESPACES)
            .withExtraEnvVars(coEnvVars)
            .createInstallation()
            .runInstallation();

        LOGGER.info("Deploying Kafka");
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, kafkaReplicas, zkReplicas).build());
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());

        Map<String, String> kafkaPods = PodUtils.podSnapshot(clusterOperator.getDeploymentNamespace(), kafkaSelector);
        Map<String, String> zkPods = PodUtils.podSnapshot(clusterOperator.getDeploymentNamespace(), zkSelector);
        Map<String, String> coPod = DeploymentUtils.depSnapshot(clusterOperator.getDeploymentNamespace(), ResourceManager.getCoDeploymentName());

        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(clusterName))
            .withTopicName(topicName)
            .withMessageCount(messageCount)
            .withDelayMs(1000)
            .withNamespaceName(clusterOperator.getDeploymentNamespace())
            .build();

        resourceManager.createResource(extensionContext,
            clients.producerStrimzi(),
            clients.consumerStrimzi()
        );

        LOGGER.info("Changing FG env variable to enable STS");
        coEnvVars = kubeClient().getDeployment(clusterOperator.getDeploymentNamespace(), Constants.STRIMZI_DEPLOYMENT_NAME).getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        coEnvVars.stream().filter(env -> env.getName().equals(Environment.STRIMZI_FEATURE_GATES_ENV)).findFirst().get().setValue("-UseStrimziPodSets");

        Deployment coDep = kubeClient().getDeployment(clusterOperator.getDeploymentNamespace(), Constants.STRIMZI_DEPLOYMENT_NAME);
        coDep.getSpec().getTemplate().getSpec().getContainers().get(0).setEnv(coEnvVars);
        kubeClient().getClient().apps().deployments().inNamespace(clusterOperator.getDeploymentNamespace()).resource(coDep).replace();

        coPod = DeploymentUtils.waitTillDepHasRolled(clusterOperator.getDeploymentNamespace(), Constants.STRIMZI_DEPLOYMENT_NAME, 1, coPod);

        zkPods = RollingUpdateUtils.waitTillComponentHasRolled(clusterOperator.getDeploymentNamespace(), zkSelector, zkReplicas, zkPods);
        kafkaPods = RollingUpdateUtils.waitTillComponentHasRolled(clusterOperator.getDeploymentNamespace(), kafkaSelector, kafkaReplicas, kafkaPods);

        KafkaUtils.waitForKafkaReady(clusterOperator.getDeploymentNamespace(), clusterName);

        LOGGER.info("Changing FG env variable to disable again STS");
        coEnvVars.stream().filter(env -> env.getName().equals(Environment.STRIMZI_FEATURE_GATES_ENV)).findFirst().get().setValue("");

        coDep = kubeClient().getDeployment(clusterOperator.getDeploymentNamespace(), Constants.STRIMZI_DEPLOYMENT_NAME);
        coDep.getSpec().getTemplate().getSpec().getContainers().get(0).setEnv(coEnvVars);
        kubeClient().getClient().apps().deployments().inNamespace(clusterOperator.getDeploymentNamespace()).resource(coDep).replace();

        DeploymentUtils.waitTillDepHasRolled(clusterOperator.getDeploymentNamespace(), Constants.STRIMZI_DEPLOYMENT_NAME, 1, coPod);

        RollingUpdateUtils.waitTillComponentHasRolled(clusterOperator.getDeploymentNamespace(), zkSelector, zkReplicas, zkPods);
        RollingUpdateUtils.waitTillComponentHasRolled(clusterOperator.getDeploymentNamespace(), kafkaSelector, kafkaReplicas, kafkaPods);

        ClientUtils.waitForClientsSuccess(producerName, consumerName, clusterOperator.getDeploymentNamespace(), messageCount);
    }

    /**
     * UseKRaft feature gate
     */
    @IsolatedTest("Feature Gates test for enabled UseKRaft gate")
    @Tag(INTERNAL_CLIENTS_USED)
    public void testKRaftMode(ExtensionContext extensionContext) {
        assumeFalse(Environment.isOlmInstall() || Environment.isHelmInstall());
        final TestStorage testStorage = new TestStorage(extensionContext);

        final String clusterName = testStorage.getClusterName();
        final String producerName = testStorage.getProducerName();
        final String consumerName = testStorage.getConsumerName();
        final String topicName = testStorage.getTopicName();

        final LabelSelector zkSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.zookeeperStatefulSetName(clusterName));
        final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaStatefulSetName(clusterName));

        int messageCount = 180;
        List<EnvVar> testEnvVars = new ArrayList<>();
        int kafkaReplicas = 3;

        testEnvVars.add(new EnvVar(Environment.STRIMZI_FEATURE_GATES_ENV, "+UseStrimziPodSets,+UseKRaft", null));

        clusterOperator.unInstall();
        clusterOperator = new SetupClusterOperator.SetupClusterOperatorBuilder()
                .withExtensionContext(BeforeAllOnce.getSharedExtensionContext())
                .withNamespace(INFRA_NAMESPACE)
                .withWatchingNamespaces(Constants.WATCH_ALL_NAMESPACES)
                .withExtraEnvVars(testEnvVars)
                .createInstallation()
                .runInstallation();

        Kafka kafka = KafkaTemplates.kafkaPersistent(clusterName, kafkaReplicas)
            .editSpec()
                .editKafka()
                .withListeners(
                        new GenericKafkaListenerBuilder()
                                .withType(KafkaListenerType.INTERNAL)
                                .withName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
                                .withPort(9092)
                                .withTls(false)
                                .build(),
                        new GenericKafkaListenerBuilder()
                                .withType(KafkaListenerType.INTERNAL)
                                .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                                .withPort(9093)
                                .withTls(true)
                                .withNewKafkaListenerAuthenticationTlsAuth()
                                .endKafkaListenerAuthenticationTlsAuth()
                                .build()
                )
                .endKafka()
            .endSpec()
            .build();
        kafka.getSpec().getEntityOperator().setTopicOperator(null); // The builder cannot disable the EO. It has to be done this way.
        resourceManager.createResource(extensionContext, kafka);

        resourceManager.createResource(extensionContext,
            KafkaUserTemplates.tlsUser(testStorage).build()
        );

        LOGGER.info("Try to send some messages to Kafka over next few minutes.");

        KafkaClients kafkaClients = new KafkaClientsBuilder()
                .withProducerName(producerName)
                .withConsumerName(consumerName)
                .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
                .withUserName(testStorage.getUserName())
                .withTopicName(topicName)
                .withMessageCount(messageCount)
                .withDelayMs(500)
                .withNamespaceName(INFRA_NAMESPACE)
                .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerTlsStrimzi(clusterName));
        resourceManager.createResource(extensionContext, kafkaClients.consumerTlsStrimzi(clusterName));

        // Check that there is no ZooKeeper
        Map<String, String> zkPods = PodUtils.podSnapshot(INFRA_NAMESPACE, zkSelector);
        assertThat("No ZooKeeper pods should exist", zkPods.size(), is(0));

        // Roll Kafka
        LOGGER.info("Force Rolling Update of Kafka via read-only configuration change.");
        Map<String, String> kafkaPods = PodUtils.podSnapshot(INFRA_NAMESPACE, kafkaSelector);
        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, k -> k.getSpec().getKafka().getConfig().put("log.retention.hours", 72), INFRA_NAMESPACE);

        LOGGER.info("Wait for next reconciliation to happen.");
        RollingUpdateUtils.waitTillComponentHasRolled(INFRA_NAMESPACE, kafkaSelector, kafkaReplicas, kafkaPods);

        LOGGER.info("Waiting for clients to finish sending/receiving messages.");
        ClientUtils.waitForClientsSuccess(producerName, consumerName, INFRA_NAMESPACE, MESSAGE_COUNT);
    }
}
