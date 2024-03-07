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
import io.strimzi.api.kafka.model.bridge.KafkaBridgeResources;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeStatus;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.connect.ConnectorPlugin;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.connect.KafkaConnectStatus;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.kafka.listener.ListenerStatus;
import io.strimzi.api.kafka.model.mirrormaker.KafkaMirrorMakerStatus;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Status;
import io.strimzi.api.kafka.model.user.acl.AclOperation;
import io.strimzi.api.kafka.model.user.acl.AclRule;
import io.strimzi.api.kafka.model.user.acl.AclRuleBuilder;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.kafkaclients.externalClients.ExternalKafkaClient;
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectorResource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMakerResource;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMakerTemplates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.api.kafka.model.kafka.KafkaResources.externalBootstrapServiceName;
import static io.strimzi.systemtest.TestConstants.BRIDGE;
import static io.strimzi.systemtest.TestConstants.CONNECT;
import static io.strimzi.systemtest.TestConstants.CONNECTOR_OPERATOR;
import static io.strimzi.systemtest.TestConstants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.TestConstants.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.TestConstants.MIRROR_MAKER;
import static io.strimzi.systemtest.TestConstants.MIRROR_MAKER2;
import static io.strimzi.systemtest.TestConstants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.TestConstants.REGRESSION;
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
    private TestStorage sharedTestStorage;

    @ParallelTest
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    void testKafkaStatus() {
        LOGGER.info("Checking status of deployed Kafka cluster");
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        KafkaUtils.waitForKafkaReady(Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getClusterName());

        ExternalKafkaClient externalKafkaClient = new ExternalKafkaClient.Builder()
            .withTopicName(testStorage.getTopicName())
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withClusterName(sharedTestStorage.getClusterName())
            .withMessageCount(testStorage.getMessageCount())
            .withListenerName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
            .build();

        externalKafkaClient.verifyProducedAndConsumedMessages(
            externalKafkaClient.sendMessagesPlain(),
            externalKafkaClient.receiveMessagesPlain()
        );

        assertKafkaStatus(1, KafkaResources.bootstrapServiceName(sharedTestStorage.getClusterName()) + "." + Environment.TEST_SUITE_NAMESPACE + ".svc");

        ResourceRequirements resources = new ResourceRequirementsBuilder()
            .addToRequests("cpu", new Quantity("100000m"))
            .build();

        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(sharedTestStorage.getBrokerPoolName(), knp ->
                knp.getSpec().setResources(resources), Environment.TEST_SUITE_NAMESPACE);
        } else {
            KafkaResource.replaceKafkaResourceInSpecificNamespace(sharedTestStorage.getClusterName(), k -> {
                k.getSpec().getKafka().setResources(resources);
            }, Environment.TEST_SUITE_NAMESPACE);
        }

        LOGGER.info("Waiting for cluster to be in NotReady state");
        KafkaUtils.waitForKafkaNotReady(Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getClusterName());

        LOGGER.info("Recover cluster to Ready state");
        resources.setRequests(Collections.singletonMap("cpu", new Quantity("100m")));

        if (Environment.isKafkaNodePoolsEnabled()) {
            KafkaNodePoolResource.replaceKafkaNodePoolResourceInSpecificNamespace(sharedTestStorage.getBrokerPoolName(), knp ->
                knp.getSpec().setResources(resources), Environment.TEST_SUITE_NAMESPACE);
        } else {
            KafkaResource.replaceKafkaResourceInSpecificNamespace(sharedTestStorage.getClusterName(), k -> {
                k.getSpec().getKafka().setResources(resources);
            }, Environment.TEST_SUITE_NAMESPACE);
        }

        KafkaUtils.waitForKafkaReady(Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getClusterName());
        assertKafkaStatus(3, KafkaResources.bootstrapServiceName(sharedTestStorage.getClusterName()) + "." + Environment.TEST_SUITE_NAMESPACE + ".svc");
    }

    @ParallelTest
    void testKafkaUserStatus() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        final AclRule aclRule = new AclRuleBuilder()
            .withNewAclRuleTopicResource()
                .withName(testStorage.getTopicName())
            .endAclRuleTopicResource()
            .withOperations(AclOperation.WRITE, AclOperation.DESCRIBE)
            .build();

        resourceManager.createResourceWithoutWait(KafkaUserTemplates.tlsUser(Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getClusterName(), testStorage.getKafkaUsername())
            .editSpec()
                .withNewKafkaUserAuthorizationSimple()
                    .withAcls(aclRule)
                .endKafkaUserAuthorizationSimple()
            .endSpec()
            .build());

        KafkaUserUtils.waitForKafkaUserNotReady(Environment.TEST_SUITE_NAMESPACE, testStorage.getKafkaUsername());

        KafkaUserResource.replaceUserResourceInSpecificNamespace(testStorage.getKafkaUsername(), user -> user.getSpec().setAuthorization(null), Environment.TEST_SUITE_NAMESPACE);
        KafkaUserUtils.waitForKafkaUserReady(Environment.TEST_SUITE_NAMESPACE, testStorage.getKafkaUsername());

        LOGGER.info("Checking status of deployed KafkaUser");
        Condition kafkaCondition = KafkaUserResource.kafkaUserClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(testStorage.getKafkaUsername()).get().getStatus().getConditions().get(0);
        LOGGER.info("KafkaUser Status: {}", kafkaCondition.getStatus());
        LOGGER.info("KafkaUser Type: {}", kafkaCondition.getType());
        assertThat("KafkaUser is in wrong state!", kafkaCondition.getType(), is(Ready.toString()));
        LOGGER.info("KafkaUser is in desired state: Ready");
    }

    @ParallelTest
    @Tag(MIRROR_MAKER)
    void testKafkaMirrorMakerStatusWrongBootstrap() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        String mirrorMakerName = testStorage.getClusterName() + "-mirror-maker-2";

        resourceManager.createResourceWithWait(KafkaMirrorMakerTemplates.kafkaMirrorMaker(mirrorMakerName, sharedTestStorage.getClusterName(), sharedTestStorage.getClusterName(), ClientUtils.generateRandomConsumerGroup(), 1, false)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .build());
        KafkaMirrorMakerUtils.waitForKafkaMirrorMakerReady(Environment.TEST_SUITE_NAMESPACE, mirrorMakerName);
        assertKafkaMirrorMakerStatus(1, mirrorMakerName);
        // Corrupt MirrorMaker Pods
        KafkaMirrorMakerResource.replaceMirrorMakerResourceInSpecificNamespace(mirrorMakerName, mm -> mm.getSpec().getConsumer().setBootstrapServers("non-exists-bootstrap"), Environment.TEST_SUITE_NAMESPACE);
        KafkaMirrorMakerUtils.waitForKafkaMirrorMakerNotReady(Environment.TEST_SUITE_NAMESPACE, mirrorMakerName);
        // Restore MirrorMaker Pods
        KafkaMirrorMakerResource.replaceMirrorMakerResourceInSpecificNamespace(mirrorMakerName, mm -> mm.getSpec().getConsumer().setBootstrapServers(KafkaResources.plainBootstrapAddress(sharedTestStorage.getClusterName())), Environment.TEST_SUITE_NAMESPACE);
        KafkaMirrorMakerUtils.waitForKafkaMirrorMakerReady(Environment.TEST_SUITE_NAMESPACE, mirrorMakerName);
        assertKafkaMirrorMakerStatus(3, mirrorMakerName);
    }

    @ParallelTest
    @Tag(BRIDGE)
    void testKafkaBridgeStatus() {
        String bridgeUrl = KafkaBridgeResources.url(sharedTestStorage.getClusterName(), Environment.TEST_SUITE_NAMESPACE, 8080);

        resourceManager.createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(sharedTestStorage.getClusterName(), KafkaResources.plainBootstrapAddress(sharedTestStorage.getClusterName()), 1)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .build());
        KafkaBridgeUtils.waitForKafkaBridgeReady(Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getClusterName());
        assertKafkaBridgeStatus(1, bridgeUrl);

        KafkaBridgeResource.replaceBridgeResourceInSpecificNamespace(sharedTestStorage.getClusterName(), kb -> kb.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100000000m"))
                .build()),
            Environment.TEST_SUITE_NAMESPACE);
        KafkaBridgeUtils.waitForKafkaBridgeNotReady(Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getClusterName());

        KafkaBridgeResource.replaceBridgeResourceInSpecificNamespace(sharedTestStorage.getClusterName(), kb -> kb.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("10m"))
                .build()),
            Environment.TEST_SUITE_NAMESPACE);
        KafkaBridgeUtils.waitForKafkaBridgeReady(Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getClusterName());
        assertKafkaBridgeStatus(3, bridgeUrl);
    }

    @ParallelTest
    @Tag(CONNECT)
    @Tag(CONNECTOR_OPERATOR)
    @Tag(CONNECT_COMPONENTS)
    void testKafkaConnectAndConnectorStatus() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        String connectUrl = KafkaConnectResources.url(sharedTestStorage.getClusterName(), testStorage.getNamespaceName(), 8083);

        resourceManager.createResourceWithWait(KafkaConnectTemplates.kafkaConnectWithFilePlugin(sharedTestStorage.getClusterName(), testStorage.getNamespaceName(), 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .build());

        resourceManager.createResourceWithWait(KafkaConnectorTemplates.kafkaConnector(sharedTestStorage.getClusterName())
            .editMetadata()
                .withNamespace(testStorage.getNamespaceName())
            .endMetadata()
            .editSpec()
                .addToConfig("topic", testStorage.getTopicName())
            .endSpec()
            .build());

        assertKafkaConnectStatus(1, connectUrl);

        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(sharedTestStorage.getClusterName(),
            kb -> kb.getSpec().setBootstrapServers("non-existing-bootstrap"), testStorage.getNamespaceName());

        KafkaConnectUtils.waitForConnectNotReady(testStorage.getNamespaceName(), sharedTestStorage.getClusterName());

        KafkaConnectResource.replaceKafkaConnectResourceInSpecificNamespace(sharedTestStorage.getClusterName(),
            kb -> kb.getSpec().setBootstrapServers(KafkaResources.tlsBootstrapAddress(sharedTestStorage.getClusterName())), testStorage.getNamespaceName());

        KafkaConnectUtils.waitForConnectReady(testStorage.getNamespaceName(), sharedTestStorage.getClusterName());
        assertKafkaConnectStatus(3, connectUrl);

        KafkaConnectorResource.replaceKafkaConnectorResourceInSpecificNamespace(sharedTestStorage.getClusterName(),
            kc -> kc.getMetadata().setLabels(Collections.singletonMap(Labels.STRIMZI_CLUSTER_LABEL, "non-existing-connect-cluster")), testStorage.getNamespaceName());
        KafkaConnectorUtils.waitForConnectorNotReady(testStorage.getNamespaceName(), sharedTestStorage.getClusterName());
        assertThat(KafkaConnectorResource.kafkaConnectorClient().inNamespace(testStorage.getNamespaceName()).withName(sharedTestStorage.getClusterName()).get().getStatus().getConnectorStatus(), is(nullValue()));

        KafkaConnectorResource.replaceKafkaConnectorResourceInSpecificNamespace(sharedTestStorage.getClusterName(),
            kc -> kc.getMetadata().setLabels(Collections.singletonMap(Labels.STRIMZI_CLUSTER_LABEL, sharedTestStorage.getClusterName())), testStorage.getNamespaceName());

        KafkaConnectorUtils.waitForConnectorReady(testStorage.getNamespaceName(), sharedTestStorage.getClusterName());
        KafkaConnectUtils.waitForConnectReady(testStorage.getNamespaceName(), sharedTestStorage.getClusterName());

        String defaultClass = KafkaConnectorResource.kafkaConnectorClient().inNamespace(testStorage.getNamespaceName()).withName(sharedTestStorage.getClusterName()).get().getSpec().getClassName();

        KafkaConnectorResource.replaceKafkaConnectorResourceInSpecificNamespace(sharedTestStorage.getClusterName(),
            kc -> kc.getSpec().setClassName("non-existing-class"), testStorage.getNamespaceName());
        KafkaConnectorUtils.waitForConnectorNotReady(testStorage.getNamespaceName(), sharedTestStorage.getClusterName());
        assertThat(KafkaConnectorResource.kafkaConnectorClient().inNamespace(testStorage.getNamespaceName()).withName(sharedTestStorage.getClusterName()).get().getStatus().getConnectorStatus(), is(nullValue()));

        KafkaConnectorResource.replaceKafkaConnectorResourceInSpecificNamespace(sharedTestStorage.getClusterName(),
            kc -> {
                kc.getMetadata().setLabels(Collections.singletonMap(Labels.STRIMZI_CLUSTER_LABEL, sharedTestStorage.getClusterName()));
                kc.getSpec().setClassName(defaultClass);
            }, testStorage.getNamespaceName());

        KafkaConnectorUtils.waitForConnectorReady(testStorage.getNamespaceName(), sharedTestStorage.getClusterName());
    }

    @ParallelTest
    @Tag(CONNECTOR_OPERATOR)
    void testKafkaConnectorWithoutClusterConfig() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        // This test check NPE when connect cluster is not specified in labels
        // Check for NPE in CO logs is performed after every test in BaseST
        resourceManager.createResourceWithoutWait(KafkaConnectorTemplates.kafkaConnector(testStorage.getClusterName(), sharedTestStorage.getClusterName(), 2)
            .withNewMetadata()
                .withName(testStorage.getClusterName())
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .build());

        KafkaConnectorUtils.waitForConnectorNotReady(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName());

        KafkaConnectorResource.kafkaConnectorClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(testStorage.getClusterName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
        KafkaConnectorUtils.waitForConnectorDeletion(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName());
    }

    @ParallelTest
    void testKafkaStatusCertificate() {
        String certs = getKafkaStatusCertificates(TestConstants.TLS_LISTENER_DEFAULT_NAME, Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getClusterName());
        String secretCerts = getKafkaSecretCertificates(Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getClusterName() + "-cluster-ca-cert", "ca.crt");

        LOGGER.info("Check if KafkaStatus certificates are the same as Secret certificates");
        assertThat(secretCerts, is(certs));
    }

    @ParallelTest
    @Tag(MIRROR_MAKER2)
    @Tag(CONNECT_COMPONENTS)
    void testKafkaMirrorMaker2Status() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        String mm2Url = KafkaMirrorMaker2Resources.url(testStorage.getClusterName(), Environment.TEST_SUITE_NAMESPACE, 8083);

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), 1, 1)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .build());
        resourceManager.createResourceWithWait(KafkaMirrorMaker2Templates.kafkaMirrorMaker2(testStorage.getClusterName(), sharedTestStorage.getClusterName(), testStorage.getClusterName(), 1, false)
                .editMetadata()
                    .withNamespace(Environment.TEST_SUITE_NAMESPACE)
                .endMetadata()
                .editSpec()
                    .editFirstMirror()
                        .editOrNewHeartbeatConnector()
                        .withConfig(Map.of("heartbeats.topic.replication.factor", "1"))
                        .endHeartbeatConnector()
                    .endMirror()
                .endSpec()
                .build());
        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2Ready(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName());
        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2ConnectorReadiness(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName());
        assertKafkaMirrorMaker2Status(1, mm2Url, testStorage.getClusterName());

        // Corrupt MirrorMaker Pods
        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2ResourceInSpecificNamespace(testStorage.getClusterName(), mm2 -> mm2.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100000000m"))
                .build()),
            Environment.TEST_SUITE_NAMESPACE);
        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2NotReady(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName());
        // Restore MirrorMaker Pod
        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2ResourceInSpecificNamespace(testStorage.getClusterName(), mm2 -> mm2.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100m"))
                .build()),
            Environment.TEST_SUITE_NAMESPACE);
        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2Ready(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName());
        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2ConnectorReadiness(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName());
        assertKafkaMirrorMaker2Status(3, mm2Url, testStorage.getClusterName());
        // Wait for pods stability and check that pods weren't rolled
        PodUtils.verifyThatRunningPodsAreStable(Environment.TEST_SUITE_NAMESPACE, KafkaMirrorMaker2Resources.componentName(testStorage.getClusterName()));
        assertKafkaMirrorMaker2Status(3, mm2Url, testStorage.getClusterName());
        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2ConnectorReadiness(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName());
    }

    @ParallelTest
    @Tag(MIRROR_MAKER2)
    void testKafkaMirrorMaker2WrongBootstrap() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        String mirrorMaker2Name = testStorage.getClusterName() + "-mirror-maker-2";

        KafkaMirrorMaker2 kafkaMirrorMaker2 = KafkaMirrorMaker2Templates.kafkaMirrorMaker2(mirrorMaker2Name,
            "non-existing-source", "non-existing-target", 1, false)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .build();

        resourceManager.createResourceWithoutWait(kafkaMirrorMaker2);

        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2NotReady(Environment.TEST_SUITE_NAMESPACE, mirrorMaker2Name);

        // delete
        KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(mirrorMaker2Name).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();

        DeploymentUtils.waitForDeploymentDeletion(Environment.TEST_SUITE_NAMESPACE, KafkaMirrorMaker2Resources.componentName(mirrorMaker2Name));
    }

    @BeforeAll
    void setup() {
        sharedTestStorage = new TestStorage(ResourceManager.getTestContext());

        this.clusterOperator = this.clusterOperator.defaultInstallation()
            .withOperationTimeout(TestConstants.CO_OPERATION_TIMEOUT_SHORT)
            .createInstallation()
            .runInstallation();

        GenericKafkaListener plain = new GenericKafkaListenerBuilder()
                .withName(TestConstants.PLAIN_LISTENER_DEFAULT_NAME)
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .build();
        GenericKafkaListener tls = new GenericKafkaListenerBuilder()
                .withName(TestConstants.TLS_LISTENER_DEFAULT_NAME)
                .withPort(9093)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(true)
                .build();
        GenericKafkaListener nodePort = new GenericKafkaListenerBuilder()
                .withName(TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME)
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

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(sharedTestStorage.getNamespaceName(), sharedTestStorage.getBrokerPoolName(), sharedTestStorage.getClusterName(), 1).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(sharedTestStorage.getNamespaceName(), sharedTestStorage.getControllerPoolName(), sharedTestStorage.getClusterName(), 1).build()
            )
        );
        KafkaBuilder kafkaBuilder = KafkaTemplates.kafkaPersistent(sharedTestStorage.getClusterName(), 1, 1)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .editSpec()
                .editKafka()
                    .withListeners(listeners)
                .endKafka()
            .endSpec();

        resourceManager.createResourceWithWait(kafkaBuilder.build());
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(sharedTestStorage.getClusterName(), sharedTestStorage.getTopicName(), Environment.TEST_SUITE_NAMESPACE).build());
    }

    void assertKafkaStatus(long expectedObservedGeneration, String internalAddress) {
        long observedGeneration = 0;

        KafkaStatus kafkaStatus = KafkaResource.kafkaClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(sharedTestStorage.getClusterName()).get().getStatus();

        if (Environment.isKafkaNodePoolsEnabled()) {
            observedGeneration = KafkaNodePoolResource.kafkaNodePoolClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(sharedTestStorage.getBrokerPoolName()).get().getStatus().getObservedGeneration();
        } else {
            observedGeneration = kafkaStatus.getObservedGeneration();
        }

        assertThat("Kafka cluster status has incorrect Observed Generation", observedGeneration, is(expectedObservedGeneration));

        for (ListenerStatus listener : kafkaStatus.getListeners()) {
            switch (listener.getName()) {
                case TestConstants.TLS_LISTENER_DEFAULT_NAME:
                    assertThat("TLS bootstrap has incorrect port", listener.getAddresses().get(0).getPort(), is(9093));
                    assertThat("TLS bootstrap has incorrect host", listener.getAddresses().get(0).getHost(), is(internalAddress));
                    break;
                case TestConstants.PLAIN_LISTENER_DEFAULT_NAME:
                    assertThat("Plain bootstrap has incorrect port", listener.getAddresses().get(0).getPort(), is(9092));
                    assertThat("Plain bootstrap has incorrect host", listener.getAddresses().get(0).getHost(), is(internalAddress));
                    break;
                case TestConstants.EXTERNAL_LISTENER_DEFAULT_NAME:
                    Service extBootstrapService = kubeClient(Environment.TEST_SUITE_NAMESPACE).getClient().services()
                            .inNamespace(Environment.TEST_SUITE_NAMESPACE)
                            .withName(externalBootstrapServiceName(sharedTestStorage.getClusterName()))
                            .get();
                    assertThat("External bootstrap has incorrect port", listener.getAddresses().get(0).getPort(), is(extBootstrapService.getSpec().getPorts().get(0).getNodePort()));
                    assertThat("External bootstrap has incorrect host", listener.getAddresses().get(0).getHost() != null);
                    break;
            }
        }
    }

    void assertKafkaMirrorMakerStatus(long expectedObservedGeneration, String mirrorMakerName) {
        KafkaMirrorMakerStatus kafkaMirrorMakerStatus = KafkaMirrorMakerResource.kafkaMirrorMakerClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(mirrorMakerName).get().getStatus();
        assertThat("MirrorMaker cluster status has incorrect Observed Generation", kafkaMirrorMakerStatus.getObservedGeneration(), is(expectedObservedGeneration));
    }

    void assertKafkaMirrorMaker2Status(long expectedObservedGeneration, String apiUrl, String mirrorMaker2Name) {
        KafkaMirrorMaker2Status kafkaMirrorMaker2Status = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(mirrorMaker2Name).get().getStatus();
        assertThat("MirrorMaker2 cluster status has incorrect Observed Generation", kafkaMirrorMaker2Status.getObservedGeneration(), is(expectedObservedGeneration));
        assertThat("MirrorMaker2 cluster status has incorrect URL", kafkaMirrorMaker2Status.getUrl(), is(apiUrl));
        for (Map<String, Object> connector : kafkaMirrorMaker2Status.getConnectors()) {
            assertThat("One of the connectors is not RUNNING:\n" + connector.toString(), ((Map<String, String>) connector.get("connector")).get("state"), is("RUNNING"));
        }
    }

    void assertKafkaBridgeStatus(long expectedObservedGeneration, String bridgeAddress) {
        KafkaBridgeStatus kafkaBridgeStatus = KafkaBridgeResource.kafkaBridgeClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(sharedTestStorage.getClusterName()).get().getStatus();
        assertThat("KafkaBridge cluster status has incorrect Observed Generation", kafkaBridgeStatus.getObservedGeneration(), is(expectedObservedGeneration));
        assertThat("KafkaBridge cluster status has incorrect URL", kafkaBridgeStatus.getUrl(), is(bridgeAddress));
    }

    void assertKafkaConnectStatus(long expectedObservedGeneration, String expectedUrl) {
        KafkaConnectStatus kafkaConnectStatus = KafkaConnectResource.kafkaConnectClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(sharedTestStorage.getClusterName()).get().getStatus();
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
}
