/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaConnectS2IResources;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.KafkaMirrorMakerResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.connect.ConnectorPlugin;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.status.KafkaBridgeStatus;
import io.strimzi.api.kafka.model.status.KafkaConnectS2IStatus;
import io.strimzi.api.kafka.model.status.KafkaConnectStatus;
import io.strimzi.api.kafka.model.status.KafkaConnectorStatus;
import io.strimzi.api.kafka.model.status.KafkaMirrorMaker2Status;
import io.strimzi.api.kafka.model.status.KafkaMirrorMakerStatus;
import io.strimzi.api.kafka.model.status.KafkaStatus;
import io.strimzi.api.kafka.model.status.KafkaTopicStatus;
import io.strimzi.api.kafka.model.status.ListenerStatus;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.kafkaclients.externalClients.BasicExternalKafkaClient;
import io.strimzi.systemtest.annotations.OpenShiftOnly;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectS2IResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectorResource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMaker2Resource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMakerResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaBridgeUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectS2IUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaMirrorMaker2Utils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaMirrorMakerUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

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
import static io.strimzi.systemtest.Constants.CONNECT_S2I;
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
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;

@Tag(REGRESSION)
class CustomResourceStatusST extends AbstractST {
    static final String NAMESPACE = "status-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(CustomResourceStatusST.class);
    private static final String CONNECTS2I_CLUSTER_NAME = CLUSTER_NAME + "-s2i";
    private static int topicOperatorReconciliationInterval;

    @Test
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    void testKafkaStatus() {
        LOGGER.info("Checking status of deployed kafka cluster");
        KafkaUtils.waitForKafkaReady(CLUSTER_NAME);

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .build();

        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesPlain(),
            basicExternalKafkaClient.receiveMessagesPlain()
        );

        assertKafkaStatus(1, "my-cluster-kafka-bootstrap.status-cluster-test.svc");

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec().getEntityOperator().getTopicOperator().setResources(new ResourceRequirementsBuilder()
                    .addToRequests("cpu", new Quantity("100000m"))
                    .build());
        });

        LOGGER.info("Wait until cluster will be in NotReady state ...");
        KafkaUtils.waitForKafkaNotReady(CLUSTER_NAME);

        LOGGER.info("Recovery cluster to Ready state ...");
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec().getEntityOperator().getTopicOperator().setResources(new ResourceRequirementsBuilder()
                    .addToRequests("cpu", new Quantity("100m"))
                    .build());
        });
        KafkaUtils.waitForKafkaReady(CLUSTER_NAME);
        assertKafkaStatus(3, "my-cluster-kafka-bootstrap.status-cluster-test.svc");
    }

    @Test
    void testKafkaUserStatus() {
        String userName = "status-user-test";
        KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();

        LOGGER.info("Checking status of deployed KafkaUser");
        Condition kafkaCondition = KafkaUserResource.kafkaUserClient().inNamespace(NAMESPACE).withName(userName).get().getStatus().getConditions().get(0);
        LOGGER.info("KafkaUser Status: {}", kafkaCondition.getStatus());
        LOGGER.info("KafkaUser Type: {}", kafkaCondition.getType());
        assertThat("KafkaUser is in wrong state!", kafkaCondition.getType(), is(Ready.toString()));
        LOGGER.info("KafkaUser is in desired state: Ready");
    }

    @Test
    void testKafkaUserStatusNotReady() {
        // Simulate NotReady state with userName longer than 64 characters
        String userName = "sasl-use-rabcdefghijklmnopqrstuvxyzabcdefghijklmnopqrstuvxyzabcdef";
        KafkaUserResource.kafkaUserWithoutWait(KafkaUserResource.defaultUser(CLUSTER_NAME, userName).build());

        KafkaUserUtils.waitForKafkaUserNotReady(userName);

        LOGGER.info("Checking status of deployed KafkaUser {}", userName);
        Condition kafkaCondition = KafkaUserResource.kafkaUserClient().inNamespace(NAMESPACE).withName(userName).get().getStatus().getConditions().get(0);
        LOGGER.info("KafkaUser Status: {}", kafkaCondition.getStatus());
        LOGGER.info("KafkaUser Type: {}", kafkaCondition.getType());
        LOGGER.info("KafkaUser Message: {}", kafkaCondition.getMessage());
        LOGGER.info("KafkaUser Reason: {}", kafkaCondition.getReason());
        assertThat("KafkaUser is in wrong state!", kafkaCondition.getType(), is(NotReady.toString()));
        LOGGER.info("KafkaUser {} is in desired state: {}", userName, kafkaCondition.getType());

        KafkaUserResource.kafkaUserClient().inNamespace(NAMESPACE).withName(userName).delete();
    }

    @Test
    @Tag(MIRROR_MAKER)
    void testKafkaMirrorMakerStatus() {
        // Deploy Mirror Maker
        KafkaMirrorMakerResource.kafkaMirrorMaker(CLUSTER_NAME, CLUSTER_NAME, CLUSTER_NAME, ClientUtils.generateRandomConsumerGroup(), 1, false).done();
        KafkaMirrorMakerUtils.waitForKafkaMirrorMakerReady(CLUSTER_NAME);
        assertKafkaMirrorMakerStatus(1);
        // Corrupt Mirror Maker pods
        KafkaMirrorMakerResource.replaceMirrorMakerResource(CLUSTER_NAME, mm -> mm.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100000000m"))
                .build()));
        KafkaMirrorMakerUtils.waitForKafkaMirrorMakerNotReady(CLUSTER_NAME);
        // Restore Mirror Maker pod
        KafkaMirrorMakerResource.replaceMirrorMakerResource(CLUSTER_NAME, mm -> mm.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100m"))
                .build()));
        KafkaMirrorMakerUtils.waitForKafkaMirrorMakerReady(CLUSTER_NAME);
        assertKafkaMirrorMakerStatus(3);
    }

    @Test
    @Tag(MIRROR_MAKER)
    void testKafkaMirrorMakerStatusWrongBootstrap() {
        KafkaMirrorMakerResource.kafkaMirrorMaker(CLUSTER_NAME, CLUSTER_NAME, CLUSTER_NAME, ClientUtils.generateRandomConsumerGroup(), 1, false).done();
        KafkaMirrorMakerUtils.waitForKafkaMirrorMakerReady(CLUSTER_NAME);
        assertKafkaMirrorMakerStatus(1);
        // Corrupt Mirror Maker pods
        KafkaMirrorMakerResource.replaceMirrorMakerResource(CLUSTER_NAME, mm -> mm.getSpec().getConsumer().setBootstrapServers("non-exists-bootstrap"));
        KafkaMirrorMakerUtils.waitForKafkaMirrorMakerNotReady(CLUSTER_NAME);
        // Restore Mirror Maker pods
        KafkaMirrorMakerResource.replaceMirrorMakerResource(CLUSTER_NAME, mm -> mm.getSpec().getConsumer().setBootstrapServers(KafkaResources.plainBootstrapAddress(CLUSTER_NAME)));
        KafkaMirrorMakerUtils.waitForKafkaMirrorMakerReady(CLUSTER_NAME);
        assertKafkaMirrorMakerStatus(3);
    }

    @Test
    @Tag(BRIDGE)
    void testKafkaBridgeStatus() {
        String bridgeUrl = KafkaBridgeResources.url(CLUSTER_NAME, NAMESPACE, 8080);
        KafkaBridgeResource.kafkaBridge(CLUSTER_NAME, KafkaResources.plainBootstrapAddress(CLUSTER_NAME), 1).done();
        KafkaBridgeUtils.waitForKafkaBridgeReady(CLUSTER_NAME);
        assertKafkaBridgeStatus(1, bridgeUrl);

        KafkaBridgeResource.replaceBridgeResource(CLUSTER_NAME, kb -> kb.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100000000m"))
                .build()));
        KafkaBridgeUtils.waitForKafkaBridgeNotReady(CLUSTER_NAME);

        KafkaBridgeResource.replaceBridgeResource(CLUSTER_NAME, kb -> kb.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("10m"))
                .build()));
        KafkaBridgeUtils.waitForKafkaBridgeReady(CLUSTER_NAME);
        assertKafkaBridgeStatus(3, bridgeUrl);
    }

    @Test
    @Tag(CONNECT)
    @Tag(CONNECTOR_OPERATOR)
    @Tag(CONNECT_COMPONENTS)
    void testKafkaConnectAndConnectorStatus() {
        String connectUrl = KafkaConnectResources.url(CLUSTER_NAME, NAMESPACE, 8083);
        KafkaConnectResource.kafkaConnect(CLUSTER_NAME, 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata().done();

        KafkaConnectorResource.kafkaConnector(CLUSTER_NAME).done();

        assertKafkaConnectStatus(1, connectUrl);
        assertKafkaConnectorStatus(CLUSTER_NAME, 1, "RUNNING|UNASSIGNED", 0, "RUNNING", "source");

        KafkaConnectResource.replaceKafkaConnectResource(CLUSTER_NAME, kb -> kb.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100000000m"))
                .build()));
        KafkaConnectUtils.waitForConnectNotReady(CLUSTER_NAME);

        KafkaConnectResource.replaceKafkaConnectResource(CLUSTER_NAME, kb -> kb.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100m"))
                .build()));
        KafkaConnectUtils.waitForConnectReady(CLUSTER_NAME);
        assertKafkaConnectStatus(3, connectUrl);

        KafkaConnectorResource.replaceKafkaConnectorResource(CLUSTER_NAME,
            kc -> kc.getMetadata().setLabels(Collections.singletonMap(Labels.STRIMZI_CLUSTER_LABEL, "non-existing-connect-cluster")));
        KafkaConnectorUtils.waitForConnectorNotReady(CLUSTER_NAME);
        assertThat(KafkaConnectorResource.kafkaConnectorClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus().getConnectorStatus(), is(nullValue()));

        KafkaConnectorResource.replaceKafkaConnectorResource(CLUSTER_NAME,
            kc -> kc.getMetadata().setLabels(Collections.singletonMap(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME)));
        KafkaConnectorUtils.waitForConnectorReady(CLUSTER_NAME);
        assertKafkaConnectorStatus(CLUSTER_NAME, 1, "RUNNING|UNASSIGNED", 0, "RUNNING", "source");

        String defaultClass = KafkaConnectorResource.kafkaConnectorClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getSpec().getClassName();

        KafkaConnectorResource.replaceKafkaConnectorResource(CLUSTER_NAME,
            kc -> kc.getSpec().setClassName("non-existing-class"));
        KafkaConnectorUtils.waitForConnectorNotReady(CLUSTER_NAME);
        assertThat(KafkaConnectorResource.kafkaConnectorClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus().getConnectorStatus(), is(nullValue()));

        KafkaConnectorResource.replaceKafkaConnectorResource(CLUSTER_NAME,
            kc -> {
                kc.getMetadata().setLabels(Collections.singletonMap(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME));
                kc.getSpec().setClassName(defaultClass);
            });

        KafkaConnectorUtils.waitForConnectorReady(CLUSTER_NAME);
        assertKafkaConnectorStatus(CLUSTER_NAME, 3, "RUNNING|UNASSIGNED", 0, "RUNNING", "source");
    }

    @Test
    @OpenShiftOnly
    @Tag(CONNECT_S2I)
    @Tag(CONNECT_COMPONENTS)
    void testKafkaConnectS2IStatus() {
        String connectS2IDeploymentConfigName = KafkaConnectS2IResources.deploymentName(CONNECTS2I_CLUSTER_NAME);
        String connectS2IUrl = KafkaConnectS2IResources.url(CONNECTS2I_CLUSTER_NAME, NAMESPACE, 8083);

        KafkaConnectS2IResource.kafkaConnectS2I(CONNECTS2I_CLUSTER_NAME, CLUSTER_NAME, 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata().done();

        assertKafkaConnectS2IStatus(1, connectS2IUrl, connectS2IDeploymentConfigName);

        KafkaConnectS2IResource.replaceConnectS2IResource(CONNECTS2I_CLUSTER_NAME, kb -> kb.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100000000m"))
                .build()));
        KafkaConnectS2IUtils.waitForConnectS2INotReady(CONNECTS2I_CLUSTER_NAME);

        KafkaConnectS2IResource.replaceConnectS2IResource(CONNECTS2I_CLUSTER_NAME, kb -> kb.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100m"))
                .build()));
        KafkaConnectS2IUtils.waitForConnectS2IReady(CONNECTS2I_CLUSTER_NAME);
        assertKafkaConnectS2IStatus(3, connectS2IUrl, connectS2IDeploymentConfigName);
    }

    @Test
    @Tag(CONNECTOR_OPERATOR)
    void testKafkaConnectorWithoutClusterConfig() {
        // This test check NPE when connect cluster is not specified in labels
        // Check for NPE in CO logs is performed after every test in BaseST
        KafkaConnectorResource.kafkaConnectorWithoutWait(KafkaConnectorResource.defaultKafkaConnector(CLUSTER_NAME, CLUSTER_NAME, 2)
            .withNewMetadata()
                .withName(CLUSTER_NAME)
                .withNamespace(ResourceManager.kubeClient().getNamespace())
            .endMetadata()
            .build());

        KafkaConnectorUtils.waitForConnectorNotReady(CLUSTER_NAME);

        KafkaConnectorResource.kafkaConnectorClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    @Test
    void testKafkaTopicStatus() {
        KafkaTopicUtils.waitForKafkaTopicReady(TOPIC_NAME);
        assertKafkaTopicStatus(1, TOPIC_NAME);
    }

    @Test
    void testKafkaTopicStatusNotReady() {
        String topicName = "my-topic";
        KafkaTopicResource.topicWithoutWait(KafkaTopicResource.defaultTopic(CLUSTER_NAME, topicName, 1, 10, 10).build());
        KafkaTopicUtils.waitForKafkaTopicNotReady(topicName);
        assertKafkaTopicStatus(1, topicName);
    }

    @Test
    void testKafkaStatusCertificate() {
        String certs = getKafkaStatusCertificates("tls", NAMESPACE, CLUSTER_NAME);
        String secretCerts = getKafkaSecretCertificates(CLUSTER_NAME + "-cluster-ca-cert", "ca.crt");

        LOGGER.info("Check if KafkaStatus certificates are the same as secret certificates");
        assertThat(secretCerts, is(certs));
    }

    @Test
    @Tag(MIRROR_MAKER2)
    @Tag(CONNECT_COMPONENTS)
    void testKafkaMirrorMaker2Status() {
        String mm2Url = KafkaMirrorMaker2Resources.url(CLUSTER_NAME, NAMESPACE, 8083);
        String targetClusterName = "target-cluster";
        KafkaResource.kafkaEphemeral(targetClusterName, 1, 1).done();
        KafkaMirrorMaker2Resource.kafkaMirrorMaker2(CLUSTER_NAME, CLUSTER_NAME, targetClusterName, 1, false).done();
        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2Ready(CLUSTER_NAME);
        assertKafkaMirrorMaker2Status(1, mm2Url);

        // Corrupt Mirror Maker pods
        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2Resource(CLUSTER_NAME, mm2 -> mm2.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100000000m"))
                .build()));
        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2NotReady(CLUSTER_NAME);
        // Restore Mirror Maker pod
        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2Resource(CLUSTER_NAME, mm2 -> mm2.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100m"))
                .build()));
        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2Ready(CLUSTER_NAME);
        assertKafkaMirrorMaker2Status(3, mm2Url);
    }

    @Test
    @Tag(MIRROR_MAKER2)
    void testKafkaMirrorMaker2WrongBootstrap() {
        KafkaMirrorMaker2Resource.kafkaMirrorMaker2WithoutWait(
                KafkaMirrorMaker2Resource.defaultKafkaMirrorMaker2(CLUSTER_NAME,
            "non-existing-source", "non-existing-target", 1, false).build());

        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2NotReady(CLUSTER_NAME);

        KafkaMirrorMaker2Resource.deleteKafkaMirrorMaker2WithoutWait(CLUSTER_NAME);
        DeploymentUtils.waitForDeploymentDeletion(KafkaMirrorMakerResources.deploymentName(CLUSTER_NAME));
    }

    @Test
    void testKafkaTopicDecreaseStatus() throws InterruptedException {
        KafkaTopicResource.topic(CLUSTER_NAME, TEST_TOPIC_NAME, 5).done();
        int decreaseTo = 1;

        LOGGER.info("Decreasing number of partitions to {}", decreaseTo);
        KafkaTopicResource.replaceTopicResource(TEST_TOPIC_NAME, kafkaTopic -> kafkaTopic.getSpec().setPartitions(decreaseTo));
        KafkaTopicUtils.waitForKafkaTopicPartitionChange(TEST_TOPIC_NAME, decreaseTo);
        KafkaTopicUtils.waitForKafkaTopicNotReady(TEST_TOPIC_NAME);

        assertKafkaTopicDecreasePartitionsStatus(TEST_TOPIC_NAME);

        // Wait some time to check if error is still present in KafkaTopic status
        LOGGER.info("Wait {} ms for next reconciliation", topicOperatorReconciliationInterval);
        Thread.sleep(topicOperatorReconciliationInterval);
        assertKafkaTopicDecreasePartitionsStatus(TEST_TOPIC_NAME);
    }

    @Test
    void testKafkaTopicChangingInSyncReplicasStatus() throws InterruptedException {
        KafkaTopicResource.topic(CLUSTER_NAME, TEST_TOPIC_NAME, 5).done();
        String invalidValue = "x";

        LOGGER.info("Changing min.insync.replicas to random char");
        KafkaTopicResource.replaceTopicResource(TEST_TOPIC_NAME, kafkaTopic -> kafkaTopic.getSpec().getConfig().replace("min.insync.replicas", invalidValue));
        KafkaTopicUtils.waitForKafkaTopicNotReady(TEST_TOPIC_NAME);

        assertKafkaTopicWrongMinInSyncReplicasStatus(TEST_TOPIC_NAME, invalidValue);

        // Wait some time to check if error is still present in KafkaTopic status
        LOGGER.info("Wait {} ms for next reconciliation", topicOperatorReconciliationInterval);
        Thread.sleep(topicOperatorReconciliationInterval);
        assertKafkaTopicWrongMinInSyncReplicasStatus(TEST_TOPIC_NAME, invalidValue);
    }

    @BeforeAll
    void setup() throws Exception {
        ResourceManager.setClassResources();
        deployTestSpecificResources();
    }

    void deployTestSpecificResources() throws Exception {
        installClusterOperator(NAMESPACE);

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1)
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

        KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_NAME).done();
        KafkaClientsResource.deployKafkaClients(false, KAFKA_CLIENTS_NAME).done();

        topicOperatorReconciliationInterval = KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get()
            .getSpec().getEntityOperator().getTopicOperator().getReconciliationIntervalSeconds() * 1_000 * 2 + 5_000;
    }

    void assertKafkaStatus(long expectedObservedGeneration, String internalAddress) {
        KafkaStatus kafkaStatus = KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus();
        assertThat("Kafka cluster status has incorrect Observed Generation", kafkaStatus.getObservedGeneration(), is(expectedObservedGeneration));

        for (ListenerStatus listener : kafkaStatus.getListeners()) {
            switch (listener.getType()) {
                case "tls":
                    assertThat("TLS bootstrap has incorrect port", listener.getAddresses().get(0).getPort(), is(9093));
                    assertThat("TLS bootstrap has incorrect host", listener.getAddresses().get(0).getHost(), is(internalAddress));
                    break;
                case "plain":
                    assertThat("Plain bootstrap has incorrect port", listener.getAddresses().get(0).getPort(), is(9092));
                    assertThat("Plain bootstrap has incorrect host", listener.getAddresses().get(0).getHost(), is(internalAddress));
                    break;
                case "external":
                    Service extBootstrapService = kubeClient(NAMESPACE).getClient().services()
                            .inNamespace(NAMESPACE)
                            .withName(externalBootstrapServiceName(CLUSTER_NAME))
                            .get();
                    assertThat("External bootstrap has incorrect port", listener.getAddresses().get(0).getPort(), is(extBootstrapService.getSpec().getPorts().get(0).getNodePort()));
                    assertThat("External bootstrap has incorrect host", listener.getAddresses().get(0).getHost() != null);
                    break;
            }
        }
    }

    void assertKafkaMirrorMakerStatus(long expectedObservedGeneration) {
        KafkaMirrorMakerStatus kafkaMirrorMakerStatus = KafkaMirrorMakerResource.kafkaMirrorMakerClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus();
        assertThat("Kafka MirrorMaker cluster status has incorrect Observed Generation", kafkaMirrorMakerStatus.getObservedGeneration(), is(expectedObservedGeneration));
    }

    void assertKafkaMirrorMaker2Status(long expectedObservedGeneration, String apiUrl) {
        KafkaMirrorMaker2Status kafkaMirrorMaker2Status = KafkaMirrorMaker2Resource.kafkaMirrorMaker2Client().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus();
        assertThat("Kafka MirrorMaker2 cluster status has incorrect Observed Generation", kafkaMirrorMaker2Status.getObservedGeneration(), is(expectedObservedGeneration));
        assertThat("Kafka MirrorMaker2 cluster status has incorrect URL", kafkaMirrorMaker2Status.getUrl(), is(apiUrl));
    }

    void assertKafkaBridgeStatus(long expectedObservedGeneration, String bridgeAddress) {
        KafkaBridgeStatus kafkaBridgeStatus = KafkaBridgeResource.kafkaBridgeClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus();
        assertThat("Kafka Bridge cluster status has incorrect Observed Generation", kafkaBridgeStatus.getObservedGeneration(), is(expectedObservedGeneration));
        assertThat("Kafka Bridge cluster status has incorrect URL", kafkaBridgeStatus.getUrl(), is(bridgeAddress));
    }

    void assertKafkaConnectStatus(long expectedObservedGeneration, String expectedUrl) {
        KafkaConnectStatus kafkaConnectStatus = KafkaConnectResource.kafkaConnectClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus();
        assertThat("Kafka Connect cluster status has incorrect Observed Generation", kafkaConnectStatus.getObservedGeneration(), is(expectedObservedGeneration));
        assertThat("Kafka Connect cluster status has incorrect URL", kafkaConnectStatus.getUrl(), is(expectedUrl));

        validateConnectPlugins(kafkaConnectStatus.getConnectorPlugins());
    }

    void assertKafkaConnectS2IStatus(long expectedObservedGeneration, String expectedUrl, String expectedConfigName) {
        KafkaConnectS2IStatus kafkaConnectS2IStatus = KafkaConnectS2IResource.kafkaConnectS2IClient().inNamespace(NAMESPACE).withName(CONNECTS2I_CLUSTER_NAME).get().getStatus();
        assertThat("Kafka ConnectS2I cluster status has incorrect Observed Generation", kafkaConnectS2IStatus.getObservedGeneration(), is(expectedObservedGeneration));
        assertThat("Kafka ConnectS2I cluster status has incorrect URL", kafkaConnectS2IStatus.getUrl(), is(expectedUrl));
        assertThat("Kafka ConnectS2I cluster status has incorrect BuildConfigName", kafkaConnectS2IStatus.getBuildConfigName(), is(expectedConfigName));

        validateConnectPlugins(kafkaConnectS2IStatus.getConnectorPlugins());
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
    void assertKafkaConnectorStatus(String clusterName, long expectedObservedGeneration, String connectorStates, int taskId, String taskState, String type) {
        KafkaConnectorStatus kafkaConnectorStatus = KafkaConnectorResource.kafkaConnectorClient().inNamespace(NAMESPACE).withName(clusterName).get().getStatus();
        assertThat(kafkaConnectorStatus.getObservedGeneration(), is(expectedObservedGeneration));
        Map<String, Object> connectorStatus = kafkaConnectorStatus.getConnectorStatus();
        String currentState = ((LinkedHashMap<String, String>) connectorStatus.get("connector")).get("state");
        assertThat(connectorStates, containsString(currentState));
        assertThat(connectorStatus.get("name"), is(clusterName));
        assertThat(connectorStatus.get("type"), is(type));
        assertThat(connectorStatus.get("tasks"), notNullValue());
    }

    void assertKafkaTopicStatus(long expectedObservedGeneration, String topicName) {
        KafkaTopicStatus kafkaTopicStatus = KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).withName(topicName).get().getStatus();
        assertThat("KafkaTopic status has incorrect Observed Generation", kafkaTopicStatus.getObservedGeneration(), is(expectedObservedGeneration));
    }

    void assertKafkaTopicDecreasePartitionsStatus(String topicName) {
        KafkaTopicStatus kafkaTopicStatus = KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).withName(topicName).get().getStatus();

        assertThat(kafkaTopicStatus.getConditions().stream()
            .anyMatch(condition -> condition.getType().equals(NotReady.toString())), is(true));
        assertThat(kafkaTopicStatus.getConditions().stream()
            .anyMatch(condition -> condition.getReason().equals("PartitionDecreaseException")), is(true));
        assertThat(kafkaTopicStatus.getConditions().stream()
            .anyMatch(condition -> condition.getMessage().contains("Number of partitions cannot be decreased")), is(true));
    }

    void assertKafkaTopicWrongMinInSyncReplicasStatus(String topicName, String invalidValue) {
        KafkaTopicStatus kafkaTopicStatus = KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).withName(topicName).get().getStatus();

        assertThat(kafkaTopicStatus.getConditions().stream()
            .anyMatch(condition -> condition.getType().equals(NotReady.toString())), is(true));
        assertThat(kafkaTopicStatus.getConditions().stream()
            .anyMatch(condition -> condition.getReason().equals("InvalidRequestException")), is(true));
        assertThat(kafkaTopicStatus.getConditions().stream()
            .anyMatch(condition -> condition.getMessage().contains(String.format("Invalid value %s for configuration min.insync.replicas", invalidValue))), is(true));
    }
}
