/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.connect.ConnectorPlugin;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListener;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.status.KafkaBridgeStatus;
import io.strimzi.api.kafka.model.status.KafkaConnectStatus;
import io.strimzi.api.kafka.model.status.KafkaConnectorStatus;
import io.strimzi.api.kafka.model.status.KafkaMirrorMaker2Status;
import io.strimzi.api.kafka.model.status.KafkaMirrorMakerStatus;
import io.strimzi.api.kafka.model.status.KafkaStatus;
import io.strimzi.api.kafka.model.status.ListenerStatus;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.kafkaclients.externalClients.ExternalKafkaClient;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectorResource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMakerResource;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMakerTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaBridgeUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaMirrorMaker2Utils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaMirrorMakerUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.api.kafka.model.KafkaResources.externalBootstrapServiceName;
import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CONNECTOR_OPERATOR;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER2;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils.getKafkaSecretCertificates;
import static io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils.getKafkaStatusCertificates;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(REGRESSION)
class CustomResourceStatusST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(CustomResourceStatusST.class);

    private static final String CUSTOM_RESOURCE_STATUS_CLUSTER_NAME = "custom-resource-status-cluster-name";
    private static final String EXAMPLE_TOPIC_NAME = "example-topic-name";

    @ParallelTest
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    void testKafkaStatus(ExtensionContext extensionContext) {
        LOGGER.info("Checking status of deployed Kafka cluster");
        KafkaUtils.waitForKafkaReady(Constants.TEST_SUITE_NAMESPACE, CUSTOM_RESOURCE_STATUS_CLUSTER_NAME);

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(Constants.TEST_SUITE_NAMESPACE)
            .withClusterName(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesPlain(),
            externalKafkaClient.receiveMessagesPlain()
        );

        assertKafkaStatus(1, KafkaResources.bootstrapServiceName(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME) + "." + Constants.TEST_SUITE_NAMESPACE + ".svc");

        ResourceRequirements resources = new ResourceRequirementsBuilder()
            .addToRequests("cpu", new Quantity("100000m"))
            .build();

        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(KafkaResource.getNodePoolName(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME), knp ->
                knp.getSpec().setResources(resources), Constants.TEST_SUITE_NAMESPACE);
        } else {
            KafkaResource.replaceKafkaResourceInSpecificNamespace(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, k -> {
                k.getSpec().getKafka().setResources(resources);
            }, Constants.TEST_SUITE_NAMESPACE);
        }

        LOGGER.info("Waiting for cluster to be in NotReady state");
        KafkaUtils.waitForKafkaNotReady(Constants.TEST_SUITE_NAMESPACE, CUSTOM_RESOURCE_STATUS_CLUSTER_NAME);

        LOGGER.info("Recover cluster to Ready state");
        resources.setRequests(Collections.singletonMap("cpu", new Quantity("100m")));

        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(KafkaResource.getNodePoolName(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME), knp ->
                knp.getSpec().setResources(resources), Constants.TEST_SUITE_NAMESPACE);
        } else {
            KafkaResource.replaceKafkaResourceInSpecificNamespace(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, k -> {
                k.getSpec().getKafka().setResources(resources);
            }, Constants.TEST_SUITE_NAMESPACE);
        }

        KafkaUtils.waitForKafkaReady(Constants.TEST_SUITE_NAMESPACE, CUSTOM_RESOURCE_STATUS_CLUSTER_NAME);
        assertKafkaStatus(3, KafkaResources.bootstrapServiceName(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME) + "." + Constants.TEST_SUITE_NAMESPACE + ".svc");
    }

    @ParallelTest
    void testKafkaUserStatus(ExtensionContext extensionContext) {
        String userName = mapWithTestUsers.get(extensionContext.getDisplayName());

        resourceManager.createResourceWithWait(extensionContext, KafkaUserTemplates.tlsUser(Constants.TEST_SUITE_NAMESPACE, CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, userName).build());

        LOGGER.info("Checking status of deployed KafkaUser");
        Condition kafkaCondition = KafkaUserResource.kafkaUserClient().inNamespace(Constants.TEST_SUITE_NAMESPACE).withName(userName).get().getStatus().getConditions().get(0);
        LOGGER.info("KafkaUser Status: {}", kafkaCondition.getStatus());
        LOGGER.info("KafkaUser Type: {}", kafkaCondition.getType());
        assertThat("KafkaUser is in wrong state!", kafkaCondition.getType(), is(Ready.toString()));
        LOGGER.info("KafkaUser is in desired state: Ready");
    }

    @ParallelTest
    void testKafkaUserStatusNotReady(ExtensionContext extensionContext) {
        // Simulate NotReady state with userName longer than 64 characters
        String userName = "sasl-use-rabcdefghijklmnopqrstuvxyzabcdefghijklmnopqrstuvxyzabcdef";

        resourceManager.createResourceWithoutWait(extensionContext, KafkaUserTemplates.defaultUser(Constants.TEST_SUITE_NAMESPACE, CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, userName).build());

        KafkaUserUtils.waitForKafkaUserNotReady(Constants.TEST_SUITE_NAMESPACE, userName);

        LOGGER.info("Checking status of deployed KafkaUser {}", userName);
        Condition kafkaCondition = KafkaUserResource.kafkaUserClient().inNamespace(Constants.TEST_SUITE_NAMESPACE).withName(userName).get().getStatus().getConditions().get(0);
        LOGGER.info("KafkaUser Status: {}", kafkaCondition.getStatus());
        LOGGER.info("KafkaUser Type: {}", kafkaCondition.getType());
        LOGGER.info("KafkaUser Message: {}", kafkaCondition.getMessage());
        LOGGER.info("KafkaUser Reason: {}", kafkaCondition.getReason());
        assertThat("KafkaUser is in wrong state!", kafkaCondition.getType(), is(NotReady.toString()));
        LOGGER.info("KafkaUser {} is in desired state: {}", userName, kafkaCondition.getType());

        KafkaUserResource.kafkaUserClient().inNamespace(Constants.TEST_SUITE_NAMESPACE).withName(userName).delete();
        KafkaUserUtils.waitForKafkaUserDeletion(Constants.TEST_SUITE_NAMESPACE, userName);
    }

    @ParallelTest
    @Tag(MIRROR_MAKER)
    void testKafkaMirrorMakerStatus(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String mirrorMakerName = clusterName + "-mirror-maker";

        // Deploy MirrorMaker
        resourceManager.createResourceWithWait(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(mirrorMakerName, CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, ClientUtils.generateRandomConsumerGroup(), 1, false)
            .editMetadata()
                .withNamespace(Constants.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .build());
        KafkaMirrorMakerUtils.waitForKafkaMirrorMakerReady(Constants.TEST_SUITE_NAMESPACE, mirrorMakerName);
        assertKafkaMirrorMakerStatus(1, mirrorMakerName);
        // Corrupt MirrorMaker Pods
        KafkaMirrorMakerResource.replaceMirrorMakerResourceInSpecificNamespace(mirrorMakerName, mm -> mm.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100000000m"))
                .build()), Constants.TEST_SUITE_NAMESPACE);
        KafkaMirrorMakerUtils.waitForKafkaMirrorMakerNotReady(Constants.TEST_SUITE_NAMESPACE, mirrorMakerName);
        // Restore MirrorMaker Pod
        KafkaMirrorMakerResource.replaceMirrorMakerResourceInSpecificNamespace(mirrorMakerName, mm -> mm.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100m"))
                .build()), Constants.TEST_SUITE_NAMESPACE);
        KafkaMirrorMakerUtils.waitForKafkaMirrorMakerReady(Constants.TEST_SUITE_NAMESPACE, mirrorMakerName);
        assertKafkaMirrorMakerStatus(3, mirrorMakerName);
    }

    @ParallelTest
    @Tag(MIRROR_MAKER)
    void testKafkaMirrorMakerStatusWrongBootstrap(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String mirrorMakerName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResourceWithWait(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(mirrorMakerName, CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, ClientUtils.generateRandomConsumerGroup(), 1, false)
            .editMetadata()
                .withNamespace(Constants.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .build());
        KafkaMirrorMakerUtils.waitForKafkaMirrorMakerReady(Constants.TEST_SUITE_NAMESPACE, mirrorMakerName);
        assertKafkaMirrorMakerStatus(1, mirrorMakerName);
        // Corrupt MirrorMaker Pods
        KafkaMirrorMakerResource.replaceMirrorMakerResourceInSpecificNamespace(mirrorMakerName, mm -> mm.getSpec().getConsumer().setBootstrapServers("non-exists-bootstrap"), Constants.TEST_SUITE_NAMESPACE);
        KafkaMirrorMakerUtils.waitForKafkaMirrorMakerNotReady(Constants.TEST_SUITE_NAMESPACE, mirrorMakerName);
        // Restore MirrorMaker Pods
        KafkaMirrorMakerResource.replaceMirrorMakerResourceInSpecificNamespace(mirrorMakerName, mm -> mm.getSpec().getConsumer().setBootstrapServers(KafkaResources.plainBootstrapAddress(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME)), Constants.TEST_SUITE_NAMESPACE);
        KafkaMirrorMakerUtils.waitForKafkaMirrorMakerReady(Constants.TEST_SUITE_NAMESPACE, mirrorMakerName);
        assertKafkaMirrorMakerStatus(3, mirrorMakerName);
    }

    @ParallelTest
    @Tag(BRIDGE)
    void testKafkaBridgeStatus(ExtensionContext extensionContext) {
        String bridgeUrl = KafkaBridgeResources.url(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, Constants.TEST_SUITE_NAMESPACE, 8080);

        resourceManager.createResourceWithWait(extensionContext, KafkaBridgeTemplates.kafkaBridge(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, KafkaResources.plainBootstrapAddress(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME), 1)
            .editMetadata()
                .withNamespace(Constants.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .build());
        KafkaBridgeUtils.waitForKafkaBridgeReady(Constants.TEST_SUITE_NAMESPACE, CUSTOM_RESOURCE_STATUS_CLUSTER_NAME);
        assertKafkaBridgeStatus(1, bridgeUrl);

        KafkaBridgeResource.replaceBridgeResourceInSpecificNamespace(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, kb -> kb.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100000000m"))
                .build()),
            Constants.TEST_SUITE_NAMESPACE);
        KafkaBridgeUtils.waitForKafkaBridgeNotReady(Constants.TEST_SUITE_NAMESPACE, CUSTOM_RESOURCE_STATUS_CLUSTER_NAME);

        KafkaBridgeResource.replaceBridgeResourceInSpecificNamespace(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, kb -> kb.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("10m"))
                .build()),
            Constants.TEST_SUITE_NAMESPACE);
        KafkaBridgeUtils.waitForKafkaBridgeReady(Constants.TEST_SUITE_NAMESPACE, CUSTOM_RESOURCE_STATUS_CLUSTER_NAME);
        assertKafkaBridgeStatus(3, bridgeUrl);
    }

    @ParallelTest
    @Tag(CONNECT)
    @Tag(CONNECTOR_OPERATOR)
    @Tag(CONNECT_COMPONENTS)
    void testKafkaConnectAndConnectorStatus(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        String connectUrl = KafkaConnectResources.url(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, testStorage.getNamespaceName(), 8083);

        resourceManager.createResourceWithWait(extensionContext, KafkaConnectTemplates.kafkaConnectWithFilePlugin(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, testStorage.getNamespaceName(), 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .build());

        resourceManager.createResourceWithWait(extensionContext, KafkaConnectorTemplates.kafkaConnector(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME)
            .editMetadata()
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .editSpec()
                .addToConfig("topic", EXAMPLE_TOPIC_NAME)
            .endSpec()
            .build());

        assertKafkaConnectStatus(1, connectUrl);
        assertKafkaConnectorStatus(1, "RUNNING|UNASSIGNED", "source", List.of(EXAMPLE_TOPIC_NAME));

        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, kb -> kb.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100000000m"))
                .build()),
            testStorage.getNamespaceName());
        KafkaConnectUtils.waitForConnectNotReady(testStorage.getNamespaceName(), CUSTOM_RESOURCE_STATUS_CLUSTER_NAME);

        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, kb -> kb.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100m"))
                .build()),
            testStorage.getNamespaceName());
        KafkaConnectUtils.waitForConnectReady(testStorage.getNamespaceName(), CUSTOM_RESOURCE_STATUS_CLUSTER_NAME);
        assertKafkaConnectStatus(3, connectUrl);

        KafkaConnectorResource.replaceKafkaConnectorResourceInSpecificNamespace(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME,
            kc -> kc.getMetadata().setLabels(Collections.singletonMap(Labels.STRIMZI_CLUSTER_LABEL, "non-existing-connect-cluster")), testStorage.getNamespaceName());
        KafkaConnectorUtils.waitForConnectorNotReady(testStorage.getNamespaceName(), CUSTOM_RESOURCE_STATUS_CLUSTER_NAME);
        assertThat(KafkaConnectorResource.kafkaConnectorClient().inNamespace(testStorage.getNamespaceName()).withName(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME).get().getStatus().getConnectorStatus(), is(nullValue()));

        KafkaConnectorResource.replaceKafkaConnectorResourceInSpecificNamespace(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME,
            kc -> kc.getMetadata().setLabels(Collections.singletonMap(Labels.STRIMZI_CLUSTER_LABEL, CUSTOM_RESOURCE_STATUS_CLUSTER_NAME)), testStorage.getNamespaceName());

        KafkaConnectorUtils.waitForConnectorReady(testStorage.getNamespaceName(), CUSTOM_RESOURCE_STATUS_CLUSTER_NAME);
        KafkaConnectUtils.waitForConnectReady(testStorage.getNamespaceName(), CUSTOM_RESOURCE_STATUS_CLUSTER_NAME);

        assertKafkaConnectorStatus(1, "RUNNING|UNASSIGNED", "source", List.of(EXAMPLE_TOPIC_NAME));

        String defaultClass = KafkaConnectorResource.kafkaConnectorClient().inNamespace(testStorage.getNamespaceName()).withName(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME).get().getSpec().getClassName();

        KafkaConnectorResource.replaceKafkaConnectorResourceInSpecificNamespace(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME,
            kc -> kc.getSpec().setClassName("non-existing-class"), testStorage.getNamespaceName());
        KafkaConnectorUtils.waitForConnectorNotReady(testStorage.getNamespaceName(), CUSTOM_RESOURCE_STATUS_CLUSTER_NAME);
        assertThat(KafkaConnectorResource.kafkaConnectorClient().inNamespace(testStorage.getNamespaceName()).withName(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME).get().getStatus().getConnectorStatus(), is(nullValue()));

        KafkaConnectorResource.replaceKafkaConnectorResourceInSpecificNamespace(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME,
            kc -> {
                kc.getMetadata().setLabels(Collections.singletonMap(Labels.STRIMZI_CLUSTER_LABEL, CUSTOM_RESOURCE_STATUS_CLUSTER_NAME));
                kc.getSpec().setClassName(defaultClass);
            }, testStorage.getNamespaceName());

        KafkaConnectorUtils.waitForConnectorReady(testStorage.getNamespaceName(), CUSTOM_RESOURCE_STATUS_CLUSTER_NAME);
        assertKafkaConnectorStatus(3, "RUNNING|UNASSIGNED", "source", List.of(EXAMPLE_TOPIC_NAME));
    }

    @ParallelTest
    @Tag(CONNECTOR_OPERATOR)
    void testKafkaConnectorWithoutClusterConfig(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        // This test check NPE when connect cluster is not specified in labels
        // Check for NPE in CO logs is performed after every test in BaseST
        resourceManager.createResourceWithoutWait(extensionContext, KafkaConnectorTemplates.kafkaConnector(clusterName, CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, 2)
            .withNewMetadata()
                .withName(clusterName)
                .withNamespace(Constants.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .build());

        KafkaConnectorUtils.waitForConnectorNotReady(Constants.TEST_SUITE_NAMESPACE, clusterName);

        KafkaConnectorResource.kafkaConnectorClient().inNamespace(Constants.TEST_SUITE_NAMESPACE).withName(clusterName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
        KafkaConnectorUtils.waitForConnectorDeletion(Constants.TEST_SUITE_NAMESPACE, clusterName);
    }

    @ParallelTest
    void testKafkaStatusCertificate(ExtensionContext extensionContext) {
        String certs = getKafkaStatusCertificates(Constants.TLS_LISTENER_DEFAULT_NAME, Constants.TEST_SUITE_NAMESPACE, CUSTOM_RESOURCE_STATUS_CLUSTER_NAME);
        String secretCerts = getKafkaSecretCertificates(Constants.TEST_SUITE_NAMESPACE, CUSTOM_RESOURCE_STATUS_CLUSTER_NAME + "-cluster-ca-cert", "ca.crt");

        LOGGER.info("Check if KafkaStatus certificates are the same as Secret certificates");
        assertThat(secretCerts, is(certs));
    }

    @ParallelTest
    @Tag(MIRROR_MAKER2)
    @Tag(CONNECT_COMPONENTS)
    void testKafkaMirrorMaker2Status(ExtensionContext extensionContext) {
        String targetClusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String mirrorMaker2Name = targetClusterName + "-mirror-maker-2";
        String mm2Url = KafkaMirrorMaker2Resources.url(mirrorMaker2Name, Constants.TEST_SUITE_NAMESPACE, 8083);

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(targetClusterName, 1, 1)
            .editMetadata()
                .withNamespace(Constants.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .build());
        resourceManager.createResourceWithWait(extensionContext, KafkaMirrorMaker2Templates.kafkaMirrorMaker2(mirrorMaker2Name, CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, targetClusterName, 1, false)
                .editMetadata()
                    .withNamespace(Constants.TEST_SUITE_NAMESPACE)
                .endMetadata()
                .editSpec()
                    .editFirstMirror()
                        .editOrNewHeartbeatConnector()
                        .withConfig(Map.of("heartbeats.topic.replication.factor", "1"))
                        .endHeartbeatConnector()
                    .endMirror()
                .endSpec()
                .build());
        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2Ready(Constants.TEST_SUITE_NAMESPACE, mirrorMaker2Name);
        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2ConnectorReadiness(Constants.TEST_SUITE_NAMESPACE, mirrorMaker2Name);
        assertKafkaMirrorMaker2Status(1, mm2Url, mirrorMaker2Name);

        // Corrupt MirrorMaker Pods
        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2ResourceInSpecificNamespace(mirrorMaker2Name, mm2 -> mm2.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100000000m"))
                .build()),
            Constants.TEST_SUITE_NAMESPACE);
        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2NotReady(Constants.TEST_SUITE_NAMESPACE, mirrorMaker2Name);
        // Restore MirrorMaker Pod
        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2ResourceInSpecificNamespace(mirrorMaker2Name, mm2 -> mm2.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100m"))
                .build()),
            Constants.TEST_SUITE_NAMESPACE);
        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2Ready(Constants.TEST_SUITE_NAMESPACE, mirrorMaker2Name);
        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2ConnectorReadiness(Constants.TEST_SUITE_NAMESPACE, mirrorMaker2Name);
        assertKafkaMirrorMaker2Status(3, mm2Url, mirrorMaker2Name);
        // Wait for pods stability and check that pods weren't rolled
        PodUtils.verifyThatRunningPodsAreStable(Constants.TEST_SUITE_NAMESPACE, KafkaMirrorMaker2Resources.deploymentName(mirrorMaker2Name));
        assertKafkaMirrorMaker2Status(3, mm2Url, mirrorMaker2Name);
        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2ConnectorReadiness(Constants.TEST_SUITE_NAMESPACE, mirrorMaker2Name);
    }

    @ParallelTest
    @Tag(MIRROR_MAKER2)
    void testKafkaMirrorMaker2WrongBootstrap(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String mirrorMaker2Name = clusterName + "-mirror-maker-2";

        KafkaMirrorMaker2 kafkaMirrorMaker2 = KafkaMirrorMaker2Templates.kafkaMirrorMaker2(mirrorMaker2Name,
            "non-existing-source", "non-existing-target", 1, false)
            .editMetadata()
                .withNamespace(Constants.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .build();

        resourceManager.createResourceWithoutWait(extensionContext, kafkaMirrorMaker2);

        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2NotReady(Constants.TEST_SUITE_NAMESPACE, mirrorMaker2Name);

        // delete
        KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(Constants.TEST_SUITE_NAMESPACE).withName(mirrorMaker2Name).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();

        DeploymentUtils.waitForDeploymentDeletion(Constants.TEST_SUITE_NAMESPACE, KafkaMirrorMaker2Resources.deploymentName(mirrorMaker2Name));
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        this.clusterOperator = this.clusterOperator.defaultInstallation(extensionContext)
            .withOperationTimeout(Constants.CO_OPERATION_TIMEOUT_SHORT)
            .createInstallation()
            .runInstallation();

        GenericKafkaListener plain = new GenericKafkaListenerBuilder()
                .withName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .build();
        GenericKafkaListener tls = new GenericKafkaListenerBuilder()
                .withName(Constants.TLS_LISTENER_DEFAULT_NAME)
                .withPort(9093)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(true)
                .build();
        GenericKafkaListener nodePort = new GenericKafkaListenerBuilder()
                .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                .withPort(9094)
                .withType(KafkaListenerType.NODEPORT)
                .withTls(false)
                .build();

        List<GenericKafkaListener> listeners;
        if (Environment.isNamespaceRbacScope()) {
            listeners = asList(plain, tls);
        } else {
            listeners = asList(plain, tls, nodePort);
        }

        KafkaBuilder kafkaBuilder = KafkaTemplates.kafkaEphemeral(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, 1, 1)
            .editMetadata()
                .withNamespace(Constants.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .editSpec()
                .editKafka()
                    .withListeners(listeners)
                .endKafka()
            .endSpec();

        resourceManager.createResourceWithWait(extensionContext, kafkaBuilder.build());
        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME, TOPIC_NAME, Constants.TEST_SUITE_NAMESPACE).build());
    }

    void assertKafkaStatus(long expectedObservedGeneration, String internalAddress) {
        long observedGeneration = 0;

        KafkaStatus kafkaStatus = KafkaResource.kafkaClient().inNamespace(Constants.TEST_SUITE_NAMESPACE).withName(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME).get().getStatus();

        if (Environment.isKafkaNodePoolsEnabled()) {
            String nodePoolName = KafkaResource.getNodePoolName(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME);
            observedGeneration = KafkaNodePoolResource.kafkaNodePoolClient().inNamespace(Constants.TEST_SUITE_NAMESPACE).withName(nodePoolName).get().getStatus().getObservedGeneration();
        } else {
            observedGeneration = kafkaStatus.getObservedGeneration();
        }

        assertThat("Kafka cluster status has incorrect Observed Generation", observedGeneration, is(expectedObservedGeneration));

        for (ListenerStatus listener : kafkaStatus.getListeners()) {
            switch (listener.getType()) {
                case Constants.TLS_LISTENER_DEFAULT_NAME:
                    assertThat("TLS bootstrap has incorrect port", listener.getAddresses().get(0).getPort(), is(9093));
                    assertThat("TLS bootstrap has incorrect host", listener.getAddresses().get(0).getHost(), is(internalAddress));
                    break;
                case Constants.PLAIN_LISTENER_DEFAULT_NAME:
                    assertThat("Plain bootstrap has incorrect port", listener.getAddresses().get(0).getPort(), is(9092));
                    assertThat("Plain bootstrap has incorrect host", listener.getAddresses().get(0).getHost(), is(internalAddress));
                    break;
                case Constants.EXTERNAL_LISTENER_DEFAULT_NAME:
                    Service extBootstrapService = kubeClient(Constants.TEST_SUITE_NAMESPACE).getClient().services()
                            .inNamespace(Constants.TEST_SUITE_NAMESPACE)
                            .withName(externalBootstrapServiceName(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME))
                            .get();
                    assertThat("External bootstrap has incorrect port", listener.getAddresses().get(0).getPort(), is(extBootstrapService.getSpec().getPorts().get(0).getNodePort()));
                    assertThat("External bootstrap has incorrect host", listener.getAddresses().get(0).getHost() != null);
                    break;
            }
        }
    }

    void assertKafkaMirrorMakerStatus(long expectedObservedGeneration, String mirrorMakerName) {
        KafkaMirrorMakerStatus kafkaMirrorMakerStatus = KafkaMirrorMakerResource.kafkaMirrorMakerClient().inNamespace(Constants.TEST_SUITE_NAMESPACE).withName(mirrorMakerName).get().getStatus();
        assertThat("MirrorMaker cluster status has incorrect Observed Generation", kafkaMirrorMakerStatus.getObservedGeneration(), is(expectedObservedGeneration));
    }

    void assertKafkaMirrorMaker2Status(long expectedObservedGeneration, String apiUrl, String mirrorMaker2Name) {
        KafkaMirrorMaker2Status kafkaMirrorMaker2Status = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(Constants.TEST_SUITE_NAMESPACE).withName(mirrorMaker2Name).get().getStatus();
        assertThat("MirrorMaker2 cluster status has incorrect Observed Generation", kafkaMirrorMaker2Status.getObservedGeneration(), is(expectedObservedGeneration));
        assertThat("MirrorMaker2 cluster status has incorrect URL", kafkaMirrorMaker2Status.getUrl(), is(apiUrl));
        for (Map<String, Object> connector : kafkaMirrorMaker2Status.getConnectors()) {
            assertThat("One of the connectors is not RUNNING:\n" + connector.toString(), ((Map<String, String>) connector.get("connector")).get("state"), is("RUNNING"));
        }
    }

    void assertKafkaBridgeStatus(long expectedObservedGeneration, String bridgeAddress) {
        KafkaBridgeStatus kafkaBridgeStatus = KafkaBridgeResource.kafkaBridgeClient().inNamespace(Constants.TEST_SUITE_NAMESPACE).withName(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME).get().getStatus();
        assertThat("KafkaBridge cluster status has incorrect Observed Generation", kafkaBridgeStatus.getObservedGeneration(), is(expectedObservedGeneration));
        assertThat("KafkaBridge cluster status has incorrect URL", kafkaBridgeStatus.getUrl(), is(bridgeAddress));
    }

    void assertKafkaConnectStatus(long expectedObservedGeneration, String expectedUrl) {
        KafkaConnectStatus kafkaConnectStatus = KafkaConnectResource.kafkaConnectClient().inNamespace(Constants.TEST_SUITE_NAMESPACE).withName(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME).get().getStatus();
        assertThat("KafkaConnect cluster status has incorrect Observed Generation", kafkaConnectStatus.getObservedGeneration(), is(expectedObservedGeneration));
        assertThat("KafkaConnect cluster status has incorrect URL", kafkaConnectStatus.getUrl(), is(expectedUrl));

        validateConnectPlugins(kafkaConnectStatus.getConnectorPlugins());
    }

    void validateConnectPlugins(List<ConnectorPlugin> pluginsList) {
        assertThat(pluginsList, notNullValue());
        List<String> pluginsClasses = pluginsList.stream().map(p -> p.getConnectorClass()).collect(Collectors.toList());
        assertThat(pluginsClasses, hasItems("org.apache.kafka.connect.file.FileStreamSinkConnector",
                "org.apache.kafka.connect.file.FileStreamSourceConnector",
                "org.apache.kafka.connect.mirror.MirrorCheckpointConnector",
                "org.apache.kafka.connect.mirror.MirrorHeartbeatConnector",
                "org.apache.kafka.connect.mirror.MirrorSourceConnector"));
    }

    @SuppressWarnings("unchecked")
    void assertKafkaConnectorStatus(long expectedObservedGeneration, String connectorStates, String type, List<String> topics) {
        TestUtils.waitFor("KafkaConnector status to except observed generation", Constants.GLOBAL_POLL_INTERVAL,
            Constants.GLOBAL_TIMEOUT, () -> {
                KafkaConnectorStatus kafkaConnectorStatus = KafkaConnectorResource.kafkaConnectorClient().inNamespace(Constants.TEST_SUITE_NAMESPACE).withName(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME).get().getStatus();
                boolean formulaResult = kafkaConnectorStatus.getObservedGeneration() == expectedObservedGeneration;

                final Map<String, Object> connectorStatus = kafkaConnectorStatus.getConnectorStatus();
                final String currentState = ((LinkedHashMap<String, String>) connectorStatus.get("connector")).get("state");

                formulaResult = formulaResult && connectorStates.contains(currentState);
                formulaResult = formulaResult && connectorStatus.get("name").equals(CUSTOM_RESOURCE_STATUS_CLUSTER_NAME);
                formulaResult = formulaResult && connectorStatus.get("type").equals(type);
                formulaResult = formulaResult && connectorStatus.get("tasks") != null;
                formulaResult = formulaResult && kafkaConnectorStatus.getTopics().equals(topics);
                LOGGER.info("KafkaConnectorStatus Topic: {}, and expected Topic: {}", kafkaConnectorStatus.getTopics().toString(), topics);

                return formulaResult;
            });
    }
}
