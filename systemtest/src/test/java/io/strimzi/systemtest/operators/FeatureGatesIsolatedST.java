/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.annotations.IsolatedTest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
public class FeatureGatesIsolatedST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(FeatureGatesIsolatedST.class);

    /**
     * UseKRaft feature gate
     */
    @IsolatedTest("Feature Gates test for enabled UseKRaft gate")
    @Tag(INTERNAL_CLIENTS_USED)
    @Disabled("Does not use KafkaNodePools for KRaft. Needs to be updated. This is tracked in https://github.com/strimzi/strimzi-kafka-operator/issues/8827")
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

        testEnvVars.add(new EnvVar(Environment.STRIMZI_FEATURE_GATES_ENV, "+UseKRaft", null));

        clusterOperator = new SetupClusterOperator.SetupClusterOperatorBuilder()
                .withExtensionContext(extensionContext)
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

        LOGGER.info("Trying to send some messages to Kafka in next few minutes");

        KafkaClients kafkaClients = new KafkaClientsBuilder()
                .withProducerName(producerName)
                .withConsumerName(consumerName)
                .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(testStorage.getClusterName()))
                .withUsername(testStorage.getUsername())
                .withTopicName(topicName)
                .withMessageCount(messageCount)
                .withDelayMs(500)
                .withNamespaceName(INFRA_NAMESPACE)
                .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerTlsStrimzi(clusterName));
        resourceManager.createResource(extensionContext, kafkaClients.consumerTlsStrimzi(clusterName));

        // Check that there is no ZooKeeper
        Map<String, String> zkPods = PodUtils.podSnapshot(INFRA_NAMESPACE, zkSelector);
        assertThat("No ZooKeeper Pods should exist", zkPods.size(), is(0));

        // Roll Kafka
        LOGGER.info("Forcing rolling update of Kafka via read-only configuration change");
        Map<String, String> kafkaPods = PodUtils.podSnapshot(INFRA_NAMESPACE, kafkaSelector);
        KafkaResource.replaceKafkaResourceInSpecificNamespace(clusterName, k -> k.getSpec().getKafka().getConfig().put("log.retention.hours", 72), INFRA_NAMESPACE);

        LOGGER.info("Waiting for the next reconciliation to happen");
        RollingUpdateUtils.waitTillComponentHasRolled(INFRA_NAMESPACE, kafkaSelector, kafkaReplicas, kafkaPods);

        LOGGER.info("Waiting for clients to finish sending/receiving messages");
        ClientUtils.waitForClientsSuccess(producerName, consumerName, INFRA_NAMESPACE, MESSAGE_COUNT);
    }
    @IsolatedTest
    void testSwitchingConnectStabilityIdentifiesFeatureGateOnAndOff(ExtensionContext extensionContext) {
        assumeFalse(Environment.isOlmInstall() || Environment.isHelmInstall());

        final TestStorage testStorage = new TestStorage(extensionContext);
        final int connectReplicas = 1;
        // sending a lot of messages throughout the test, so we will not hit race condition when there will not be any
        // messages left to send at the end of the test scenario (for the last `waitForMessagesInKafkaConnectFileSink` check)
        final int messageCount = 1000;
        List<EnvVar> coEnvVars = new ArrayList<>();

        coEnvVars.add(new EnvVar(Environment.STRIMZI_FEATURE_GATES_ENV, "-StableConnectIdentities", null));

        LOGGER.info("Deploying CO without Stable Connect Identities");

        clusterOperator = new SetupClusterOperator.SetupClusterOperatorBuilder()
                .withExtensionContext(extensionContext)
                .withNamespace(testStorage.getNamespaceName())
                .withWatchingNamespaces(Constants.WATCH_ALL_NAMESPACES)
                .withExtraEnvVars(coEnvVars)
                .createInstallation()
                .runInstallation();

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 3, 1).build());
        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(testStorage).build());
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnectWithFilePlugin(testStorage.getClusterName(), clusterOperator.getDeploymentNamespace(), connectReplicas)
                .editMetadata()
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .editSpec()
                    .addToConfig("key.converter.schemas.enable", false)
                    .addToConfig("value.converter.schemas.enable", false)
                    .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .endSpec()
                .build());
        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(testStorage.getClusterName())
                .editMetadata()
                    .withNamespace(testStorage.getNamespaceName())
                .endMetadata()
                .editSpec()
                    .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                    .addToConfig("topics", testStorage.getTopicName())
                    .addToConfig("file", Constants.DEFAULT_SINK_FILE_PATH)
                .endSpec()
                .build());

        Map<String, String> coPod = DeploymentUtils.depSnapshot(clusterOperator.getDeploymentNamespace(), ResourceManager.getCoDeploymentName());

        final LabelSelector connectLabelSelector = KafkaConnectResource.getLabelSelector(testStorage.getClusterName(), KafkaConnectResources.deploymentName(testStorage.getClusterName()));
        Map<String, String> connectPods = PodUtils.podSnapshot(clusterOperator.getDeploymentNamespace(), connectLabelSelector);

        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(messageCount)
            .withDelayMs(500)
            .withNamespaceName(clusterOperator.getDeploymentNamespace())
            .build();

        String connectorPodName = kubeClient().listPods(testStorage.getNamespaceName(), connectLabelSelector).get(0).getMetadata().getName();

        // we are sending messages continuously throughout the test to check that connector is working
        resourceManager.createResource(extensionContext, clients.producerStrimzi());

        LOGGER.info("Verifying that KafkaConnector is able to sink the messages to the file-sink file");
        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(testStorage.getNamespaceName(), connectorPodName, Constants.DEFAULT_SINK_FILE_PATH, "Hello-world");

        LOGGER.info("Changing FG env variable to enable Stable Connect Identities");
        coEnvVars = kubeClient().getDeployment(clusterOperator.getDeploymentNamespace(), Constants.STRIMZI_DEPLOYMENT_NAME).getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        coEnvVars.stream().filter(env -> env.getName().equals(Environment.STRIMZI_FEATURE_GATES_ENV)).findFirst().get().setValue("+StableConnectIdentities");

        Deployment coDep = kubeClient().getDeployment(clusterOperator.getDeploymentNamespace(), Constants.STRIMZI_DEPLOYMENT_NAME);
        coDep.getSpec().getTemplate().getSpec().getContainers().get(0).setEnv(coEnvVars);
        kubeClient().getClient().apps().deployments().inNamespace(clusterOperator.getDeploymentNamespace()).resource(coDep).update();

        coPod = DeploymentUtils.waitTillDepHasRolled(clusterOperator.getDeploymentNamespace(), Constants.STRIMZI_DEPLOYMENT_NAME, 1, coPod);
        connectPods = RollingUpdateUtils.waitTillComponentHasRolled(clusterOperator.getDeploymentNamespace(), connectLabelSelector, connectReplicas, connectPods);
        KafkaConnectorUtils.waitForConnectorReady(testStorage.getNamespaceName(), testStorage.getClusterName());

        connectorPodName = kubeClient().listPods(testStorage.getNamespaceName(), connectLabelSelector).get(0).getMetadata().getName();

        LOGGER.info("Verifying that KafkaConnector is able to sink the messages to the file-sink file");
        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(testStorage.getNamespaceName(), connectorPodName, Constants.DEFAULT_SINK_FILE_PATH, "Hello-world");

        LOGGER.info("Changing FG env variable to disable again Stable Connect Identities");
        coEnvVars.stream().filter(env -> env.getName().equals(Environment.STRIMZI_FEATURE_GATES_ENV)).findFirst().get().setValue("-StableConnectIdentities");

        coDep = kubeClient().getDeployment(clusterOperator.getDeploymentNamespace(), Constants.STRIMZI_DEPLOYMENT_NAME);
        coDep.getSpec().getTemplate().getSpec().getContainers().get(0).setEnv(coEnvVars);
        kubeClient().getClient().apps().deployments().inNamespace(clusterOperator.getDeploymentNamespace()).resource(coDep).update();

        DeploymentUtils.waitTillDepHasRolled(clusterOperator.getDeploymentNamespace(), Constants.STRIMZI_DEPLOYMENT_NAME, 1, coPod);
        RollingUpdateUtils.waitTillComponentHasRolled(clusterOperator.getDeploymentNamespace(), connectLabelSelector, connectReplicas, connectPods);
        KafkaConnectorUtils.waitForConnectorReady(testStorage.getNamespaceName(), testStorage.getClusterName());

        connectorPodName = kubeClient().listPods(testStorage.getNamespaceName(), connectLabelSelector).get(0).getMetadata().getName();

        LOGGER.info("Verifying that KafkaConnector is able to sink the messages to the file-sink file");
        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(testStorage.getNamespaceName(), connectorPodName, Constants.DEFAULT_SINK_FILE_PATH, "Hello-world");
    }
}
