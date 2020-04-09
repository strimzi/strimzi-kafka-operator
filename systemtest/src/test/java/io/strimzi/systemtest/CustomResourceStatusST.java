/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaConnectS2IResources;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Resources;
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
import io.strimzi.systemtest.kafkaclients.externalClients.BasicExternalKafkaClient;
import io.strimzi.systemtest.annotations.OpenShiftOnly;
import io.strimzi.systemtest.resources.KubernetesResource;
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
import io.strimzi.systemtest.utils.kafkaUtils.KafkaBridgeUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectS2IUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaMirrorMaker2Utils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaMirrorMakerUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
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
import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CONNECTOR_OPERATOR;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.CONNECT_S2I;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER2;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
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
class CustomResourceStatusST extends BaseST {
    static final String NAMESPACE = "status-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(CustomResourceStatusST.class);
    private static final String TOPIC_NAME = "status-topic";
    private static final String CONNECTS2I_CLUSTER_NAME = CLUSTER_NAME + "-s2i";

    @Test
    @Tag(NODEPORT_SUPPORTED)
    void testKafkaStatus() throws Exception {
        LOGGER.info("Checking status of deployed kafka cluster");
        KafkaUtils.waitUntilKafkaCRIsReady(CLUSTER_NAME);

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withConsumerGroupName(CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE))
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
        KafkaUtils.waitUntilKafkaCRIsNotReady(CLUSTER_NAME);

        LOGGER.info("Recovery cluster to Ready state ...");
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec().getEntityOperator().getTopicOperator().setResources(new ResourceRequirementsBuilder()
                    .addToRequests("cpu", new Quantity("100m"))
                    .build());
        });
        KafkaUtils.waitUntilKafkaCRIsReady(CLUSTER_NAME);
        assertKafkaStatus(3, "my-cluster-kafka-bootstrap.status-cluster-test.svc");
    }

    @Test
    void testKafkaUserStatus() {
        String userName = "status-user-test";
        KafkaUserResource.tlsUser(CLUSTER_NAME, userName).done();
        SecretUtils.waitForSecretReady(userName);
        LOGGER.info("Checking status of deployed kafka user");
        Condition kafkaCondition = KafkaUserResource.kafkaUserClient().inNamespace(NAMESPACE).withName(userName).get().getStatus().getConditions().get(0);
        LOGGER.info("Kafka User Status: {}", kafkaCondition.getStatus());
        LOGGER.info("Kafka User Type: {}", kafkaCondition.getType());
        assertThat("Kafka user is in wrong state!", kafkaCondition.getType(), is("Ready"));
        LOGGER.info("Kafka user is in desired state: Ready");
    }

    @Test
    void testKafkaUserStatusNotReady() {
        // Simulate NotReady state with userName longer than 64 characters
        String userName = "sasl-use-rabcdefghijklmnopqrstuvxyzabcdefghijklmnopqrstuvxyzabcdef";
        KafkaUserResource.kafkaUserWithoutWait(KafkaUserResource.defaultUser(CLUSTER_NAME, userName).build());

        KafkaUserUtils.waitForKafkaUserStatus(userName, "NotReady");

        LOGGER.info("Checking status of deployed Kafka User {}", userName);
        Condition kafkaCondition = KafkaUserResource.kafkaUserClient().inNamespace(NAMESPACE).withName(userName).get().getStatus().getConditions().get(0);
        LOGGER.info("Kafka User Status: {}", kafkaCondition.getStatus());
        LOGGER.info("Kafka User Type: {}", kafkaCondition.getType());
        LOGGER.info("Kafka User Message: {}", kafkaCondition.getMessage());
        LOGGER.info("Kafka User Reason: {}", kafkaCondition.getReason());
        assertThat("Kafka User is in wrong state!", kafkaCondition.getType(), is("NotReady"));
        LOGGER.info("Kafka User {} is in desired state: {}", userName, kafkaCondition.getType());

        KafkaUserResource.kafkaUserClient().inNamespace(NAMESPACE).withName(userName).delete();
    }

    @Test
    @Tag(MIRROR_MAKER)
    void testKafkaMirrorMakerStatus() {
        // Deploy Mirror Maker
        KafkaMirrorMakerResource.kafkaMirrorMaker(CLUSTER_NAME, CLUSTER_NAME, CLUSTER_NAME, "my-group" + rng.nextInt(Integer.MAX_VALUE), 1, false).done();
        KafkaMirrorMakerUtils.waitUntilKafkaMirrorMakerStatus(CLUSTER_NAME, "Ready");
        assertKafkaMirrorMakerStatus(1);
        // Corrupt Mirror Maker pods
        KafkaMirrorMakerResource.replaceMirrorMakerResource(CLUSTER_NAME, mm -> mm.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100000000m"))
                .build()));
        KafkaMirrorMakerUtils.waitUntilKafkaMirrorMakerStatus(CLUSTER_NAME, "NotReady");
        // Restore Mirror Maker pod
        KafkaMirrorMakerResource.replaceMirrorMakerResource(CLUSTER_NAME, mm -> mm.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100m"))
                .build()));
        KafkaMirrorMakerUtils.waitUntilKafkaMirrorMakerStatus(CLUSTER_NAME, "Ready");
        assertKafkaMirrorMakerStatus(3);
    }

    @Test
    @Tag(MIRROR_MAKER)
    void testKafkaMirrorMakerStatusWrongBootstrap() {
        KafkaMirrorMakerResource.kafkaMirrorMaker(CLUSTER_NAME, CLUSTER_NAME, CLUSTER_NAME, "my-group" + rng.nextInt(Integer.MAX_VALUE), 1, false).done();
        KafkaMirrorMakerUtils.waitUntilKafkaMirrorMakerStatus(CLUSTER_NAME, "Ready");
        assertKafkaMirrorMakerStatus(1);
        // Corrupt Mirror Maker pods
        KafkaMirrorMakerResource.replaceMirrorMakerResource(CLUSTER_NAME, mm -> mm.getSpec().getConsumer().setBootstrapServers("non-exists-bootstrap"));
        KafkaMirrorMakerUtils.waitUntilKafkaMirrorMakerStatus(CLUSTER_NAME, "NotReady");
        // Restore Mirror Maker pods
        KafkaMirrorMakerResource.replaceMirrorMakerResource(CLUSTER_NAME, mm -> mm.getSpec().getConsumer().setBootstrapServers(KafkaResources.plainBootstrapAddress(CLUSTER_NAME)));
        KafkaMirrorMakerUtils.waitUntilKafkaMirrorMakerStatus(CLUSTER_NAME, "Ready");
        assertKafkaMirrorMakerStatus(3);
    }

    @Test
    void testKafkaBridgeStatus() {
        String bridgeUrl = KafkaBridgeResources.url(CLUSTER_NAME, NAMESPACE, 8080);
        KafkaBridgeResource.kafkaBridge(CLUSTER_NAME, KafkaResources.plainBootstrapAddress(CLUSTER_NAME), 1).done();
        KafkaBridgeUtils.waitUntilKafkaBridgeStatus(CLUSTER_NAME, "Ready");
        assertKafkaBridgeStatus(1, bridgeUrl);

        KafkaBridgeResource.replaceBridgeResource(CLUSTER_NAME, kb -> kb.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100000000m"))
                .build()));
        KafkaBridgeUtils.waitUntilKafkaBridgeStatus(CLUSTER_NAME, "NotReady");

        KafkaBridgeResource.replaceBridgeResource(CLUSTER_NAME, kb -> kb.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("10m"))
                .build()));
        KafkaBridgeUtils.waitUntilKafkaBridgeStatus(CLUSTER_NAME, "Ready");
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
        KafkaConnectUtils.waitForConnectStatus(CLUSTER_NAME, "Ready");
        KafkaConnectorResource.kafkaConnector(CLUSTER_NAME).done();
        KafkaConnectorUtils.waitForConnectorStatus(CLUSTER_NAME, "Ready");
        assertKafkaConnectStatus(1, connectUrl);
        assertKafkaConnectorStatus(CLUSTER_NAME, 1, "RUNNING|UNASSIGNED", 0, "RUNNING", "source");

        KafkaConnectResource.replaceKafkaConnectResource(CLUSTER_NAME, kb -> kb.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100000000m"))
                .build()));
        KafkaConnectUtils.waitForConnectStatus(CLUSTER_NAME, "NotReady");

        KafkaConnectResource.replaceKafkaConnectResource(CLUSTER_NAME, kb -> kb.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100m"))
                .build()));
        KafkaConnectUtils.waitForConnectStatus(CLUSTER_NAME, "Ready");
        assertKafkaConnectStatus(3, connectUrl);

        KafkaConnectorResource.replaceKafkaConnectorResource(CLUSTER_NAME,
            kc -> kc.getMetadata().setLabels(Collections.singletonMap(Labels.STRIMZI_CLUSTER_LABEL, "non-existing-connect-cluster")));
        KafkaConnectorUtils.waitForConnectorStatus(CLUSTER_NAME, "NotReady");
        assertThat(KafkaConnectorResource.kafkaConnectorClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus().getConnectorStatus(), is(nullValue()));

        KafkaConnectorResource.replaceKafkaConnectorResource(CLUSTER_NAME,
            kc -> kc.getMetadata().setLabels(Collections.singletonMap(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME)));
        KafkaConnectorUtils.waitForConnectorStatus(CLUSTER_NAME, "Ready");
        assertKafkaConnectorStatus(CLUSTER_NAME, 1, "RUNNING|UNASSIGNED", 0, "RUNNING", "source");

        String defaultClass = KafkaConnectorResource.kafkaConnectorClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getSpec().getClassName();

        KafkaConnectorResource.replaceKafkaConnectorResource(CLUSTER_NAME,
            kc -> kc.getSpec().setClassName("non-existing-class"));
        KafkaConnectorUtils.waitForConnectorStatus(CLUSTER_NAME, "NotReady");
        assertThat(KafkaConnectorResource.kafkaConnectorClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus().getConnectorStatus(), is(nullValue()));

        KafkaConnectorResource.replaceKafkaConnectorResource(CLUSTER_NAME,
            kc -> {
                kc.getMetadata().setLabels(Collections.singletonMap(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME));
                kc.getSpec().setClassName(defaultClass);
            });

        KafkaConnectorUtils.waitForConnectorStatus(CLUSTER_NAME, "Ready");
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
        KafkaConnectS2IUtils.waitForConnectS2IStatus(CONNECTS2I_CLUSTER_NAME, "Ready");
        assertKafkaConnectS2IStatus(1, connectS2IUrl, connectS2IDeploymentConfigName);

        KafkaConnectS2IResource.replaceConnectS2IResource(CONNECTS2I_CLUSTER_NAME, kb -> kb.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100000000m"))
                .build()));
        KafkaConnectS2IUtils.waitForConnectS2IStatus(CONNECTS2I_CLUSTER_NAME, "NotReady");

        KafkaConnectS2IResource.replaceConnectS2IResource(CONNECTS2I_CLUSTER_NAME, kb -> kb.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100m"))
                .build()));
        KafkaConnectS2IUtils.waitForConnectS2IStatus(CONNECTS2I_CLUSTER_NAME, "Ready");
        assertKafkaConnectS2IStatus(3, connectS2IUrl, connectS2IDeploymentConfigName);
    }

    @Test
    void testKafkaConnectorWithoutClusterConfig() {
        // This test check NPE when connect cluster is not specified in labels
        // Check for NPE in CO logs is performed after every test in BaseST
        KafkaConnectorResource.kafkaConnectorWithoutWait(KafkaConnectorResource.defaultKafkaConnector(CLUSTER_NAME, CLUSTER_NAME, 2)
            .withNewMetadata()
                .withName(CLUSTER_NAME)
                .withNamespace(ResourceManager.kubeClient().getNamespace())
            .endMetadata()
            .build());

        KafkaConnectorUtils.waitForConnectorStatus(CLUSTER_NAME, "NotReady");
    }

    @Test
    void testKafkaTopicStatus() {
        KafkaTopicUtils.waitForKafkaTopicStatus("Ready", TOPIC_NAME);
        // The reason why we have there Observed Generation = 2 cause Kafka sync message.format.version when topic is created
        assertKafkaTopicStatus(2, TOPIC_NAME);
    }

    @Test
    void testKafkaTopicStatusNotReady() {
        String topicName = "my-topic";
        KafkaTopicResource.topicWithoutWait(KafkaTopicResource.defaultTopic(CLUSTER_NAME, topicName, 1, 10, 10).build());
        KafkaTopicUtils.waitForKafkaTopicStatus("NotReady", topicName);
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
        KafkaMirrorMaker2Utils.waitUntilKafkaMirrorMaker2Status(CLUSTER_NAME, "Ready");
        assertKafkaMirrorMaker2Status(1, mm2Url);

        // Corrupt Mirror Maker pods
        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2Resource(CLUSTER_NAME, mm2 -> mm2.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100000000m"))
                .build()));
        KafkaMirrorMaker2Utils.waitUntilKafkaMirrorMaker2Status(CLUSTER_NAME, "NotReady");
        // Restore Mirror Maker pod
        KafkaMirrorMaker2Resource.replaceKafkaMirrorMaker2Resource(CLUSTER_NAME, mm2 -> mm2.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100m"))
                .build()));
        KafkaMirrorMaker2Utils.waitUntilKafkaMirrorMaker2Status(CLUSTER_NAME, "Ready");
        assertKafkaMirrorMaker2Status(3, mm2Url);
    }

    @Test
    void testKafkaMirrorMaker2WrongBootstrap() {
        KafkaMirrorMaker2 kafkaMirrorMaker2 = KafkaMirrorMaker2Resource.kafkaMirrorMaker2WithoutWait(
                KafkaMirrorMaker2Resource.defaultKafkaMirrorMaker2(CLUSTER_NAME,
            "non-existing-source", "non-existing-target", 1, false).build());

        KafkaMirrorMaker2Utils.waitUntilKafkaMirrorMaker2Status(CLUSTER_NAME, "NotReady");

        KafkaMirrorMaker2Resource.deleteKafkaMirrorMaker2WithoutWait(kafkaMirrorMaker2);
    }

    @BeforeAll
    void setup() {
        ResourceManager.setClassResources();
        prepareEnvForOperator(NAMESPACE);

        deployTestSpecificResources();
    }

    @Override
    protected void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) {
        super.recreateTestEnv(coNamespace, bindingsNamespaces, Constants.CO_OPERATION_TIMEOUT_SHORT);
        deployTestSpecificResources();
    }

    void deployTestSpecificResources() {
        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        KubernetesResource.clusterOperator(NAMESPACE, Constants.CO_OPERATION_TIMEOUT_SHORT).done();

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
        assertThat("Kafka Topic status has incorrect Observed Generation", kafkaTopicStatus.getObservedGeneration(), is(expectedObservedGeneration));
    }
}
