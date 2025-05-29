/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.watcher;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlSpec;
import io.strimzi.api.kafka.model.kafka.exporter.KafkaExporterSpec;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.labels.LabelSelectors;
import io.strimzi.systemtest.resources.CrdClients;
import io.strimzi.systemtest.resources.crd.KafkaComponents;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.TestTags.CONNECT;
import static io.strimzi.systemtest.TestTags.CONNECTOR_OPERATOR;
import static io.strimzi.systemtest.TestTags.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.TestTags.MIRROR_MAKER2;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * Test suite that implements exact tests to be executed with Cluster Operator set to watch all or multiple namespaces, based on extending class.
 */
public abstract class AbstractNamespaceST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(AbstractNamespaceST.class);

    // namespace for all resources in this test except for KafkaTopics and KafkaUsers watched by Primary Kafka Cluser.
    static final String MAIN_TEST_NAMESPACE = "main-test-namespace";

    // namespace watched by Primary Kafka cluster's TO and UO for any KafkaTopic and KafkaUser CRs.
    static final String PRIMARY_KAFKA_WATCHED_NAMESPACE = "primary-kafka-watched-namespace";

    // name of kafka cluster which is to be created before any of these tests
    static final String PRIMARY_KAFKA_NAME = "primary-kafka";

    static String scraperPodName = "";

    /**
     * @description This test case verifies that Kafka (including components CruiseControl and Kafka Exporter) deployed in namespace different from one where Cluster Operator
     * is deployed, is still deployed correctly.
     *
     * @steps
     *  1. - As part of setup, in the main namespace, which is different from Cluster Operator namespace, ephemeral Kafka cluster with 3 replicas is deployed, also including KafkaExporter and CruiseControl.
     *     - Kafka and its components are deployed and ready.
     *
     * @usecase
     *  - namespaces
     *  - cluster-operator-watcher
     *  - kafka
     */
    @ParallelTest
    final void testDeployKafkaWithOperandsInNamespaceDifferentFromCO() {
        LOGGER.info("Verifying that Kafka: {}/{} ,and its component (KafkaExporter and CruiseControl) are deployed correctly", MAIN_TEST_NAMESPACE, PRIMARY_KAFKA_NAME);
        assertThat("CruiseControl Deployment is not ready", KubeResourceManager.get().kubeClient().getClient().apps().deployments().inNamespace(MAIN_TEST_NAMESPACE).withName(PRIMARY_KAFKA_NAME + "-cruise-control").isReady());
        assertThat("KafkaExporter Deployment is not ready", KubeResourceManager.get().kubeClient().getClient().apps().deployments().inNamespace(MAIN_TEST_NAMESPACE).withName(PRIMARY_KAFKA_NAME + "-kafka-exporter").isReady());
        assertThat("KafkaExporter Deployment is not ready", KubeResourceManager.get().kubeClient().getClient().apps().deployments().inNamespace(MAIN_TEST_NAMESPACE).withName(PRIMARY_KAFKA_NAME + "-entity-operator").isReady());
    }

    /**
     * @description This test case verifies that Cluster Operator manages KafkaBridge deployed in watched namespace, which is different
     * from one where Cluster Operator resides correctly.
     *
     * @steps
     *  1. - KafkaBridge CustomResource is deployed in namespace watched by Cluster Operator.
     *     - KafkaBridge is transitioned into ready state.
     *
     * @usecase
     *  - namespaces
     *  - cluster-operator-watcher
     *  - bridge
     */
    @ParallelTest
    final void testKafkaBridgeInDifferentNamespaceFromCO(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, MAIN_TEST_NAMESPACE);
        final String bridgeName = testStorage.getClusterName() + "-bridge";

        LOGGER.info("Creating KafkaBridge: {}/{} in Namespace different from Cluster Operator's", MAIN_TEST_NAMESPACE, bridgeName);
        KubeResourceManager.get().createResourceWithWait(KafkaBridgeTemplates.kafkaBridge(testStorage.getNamespaceName(), bridgeName,
                KafkaResources.plainBootstrapAddress(PRIMARY_KAFKA_NAME), 1).build()
        );
    }

    /**
     * @description This test case verifies that Topic Operator configured to watch other namespace than the one it is deployed in still watches and acts upon
     * CustomResources correctly.
     *
     * @steps
     *  1. - As part of setup Kafka cluster is deployed in main namespace, with Topic Operator configured to watch other namespace.
     *     - Kafka and its components are deployed and ready.
     *  2. - KafkaTopic CustomResource is created in namespace watched by Topic Operator.
     *     - Topic Operator acts upon KafkaTopic CustomResource located in watched namespace and creates corresponding KafkaTopic in given Kafka cluster.
     *
     * @usecase
     *  - namespaces
     *  - topic-operator-watcher
     */
    @ParallelTest
    final void testTopicOperatorWatchingOtherNamespace(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        LOGGER.info("Topic Operator in Kafka: {}/{} watches KafkaTopics in (different) Namespace: {}", MAIN_TEST_NAMESPACE, PRIMARY_KAFKA_NAME, PRIMARY_KAFKA_WATCHED_NAMESPACE);

        LOGGER.info("Verifying that KafkaTopic: {}/{} does not exist before test", PRIMARY_KAFKA_WATCHED_NAMESPACE, testStorage.getTopicName());
        List<String> topics = KafkaCmdClient.listTopicsUsingPodCli(MAIN_TEST_NAMESPACE, scraperPodName, KafkaResources.plainBootstrapAddress(PRIMARY_KAFKA_NAME));
        assertThat(topics, not(hasItems(testStorage.getTopicName())));

        LOGGER.info("Verifying that KafkaTopic: {}/{} is watched by TO by asserting its existence", PRIMARY_KAFKA_WATCHED_NAMESPACE, testStorage.getTopicName());
        KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(PRIMARY_KAFKA_WATCHED_NAMESPACE, testStorage.getTopicName(), PRIMARY_KAFKA_NAME).build());
        topics = KafkaCmdClient.listTopicsUsingPodCli(MAIN_TEST_NAMESPACE, scraperPodName, KafkaResources.plainBootstrapAddress(PRIMARY_KAFKA_NAME));
        assertThat(topics, hasItems(testStorage.getTopicName()));
    }

    /**
     * @description This test case verifies that KafkaUser CustomResource managed by is act upon by User Operator from correctly, despite being watched
     * from different namespace.
     *
     * @steps
     *  1. - As part of setup Kafka cluster is deployed in main namespace, with the User Operator configured to watch other namespace.
     *     - Kafka and its components are deployed and ready.
     *  2. - KafkaUser CustomResource is created in namespace watched by Topic Operator.
     *     - Topic Operator acts upon KafkaUser CustomResource which is transitioned into ready state while also creating all other resources (e.g., Secret).
     *  3. - Credentials generated due to this KafkaUser CustomResources are used in order to allow clients to communicate with Kafka cluster.
     *     - Clients are able to successfully communicate with the Kafka cluster.
     *
     * @usecase
     *  - namespaces
     *  - user-operator-watcher
     */
    @ParallelTest
    final void testUserInNamespaceDifferentFromUserOperator(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, MAIN_TEST_NAMESPACE);

        LOGGER.info("Creating KafkaUser: {}/{} residing in separated namespace, which is watched by Kafka located in Namespace: {}", PRIMARY_KAFKA_WATCHED_NAMESPACE, testStorage.getUsername(), MAIN_TEST_NAMESPACE);
        KubeResourceManager.get().createResourceWithWait(KafkaUserTemplates.tlsUser(PRIMARY_KAFKA_WATCHED_NAMESPACE, testStorage.getUsername(), PRIMARY_KAFKA_NAME).build());

        Condition kafkaCondition = CrdClients.kafkaUserClient().inNamespace(PRIMARY_KAFKA_WATCHED_NAMESPACE).withName(testStorage.getUsername())
            .get().getStatus().getConditions().get(0);
        LOGGER.info("KafkaUser condition status: {}", kafkaCondition.getStatus());
        LOGGER.info("KafkaUser condition type: {}", kafkaCondition.getType());

        assertThat(kafkaCondition.getType(), is(Ready.toString()));

        LOGGER.info("Finding and Copying Secrets related to created KafkaUser into Namespace: {} which holds Kafka", testStorage.getNamespaceName());
        List<Secret> secretsOfSecondNamespace = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(PRIMARY_KAFKA_WATCHED_NAMESPACE).list().getItems();

        for (Secret s : secretsOfSecondNamespace) {
            if (s.getMetadata().getName().equals(testStorage.getUsername())) {
                LOGGER.info("Copying Secret: {} from Namespace: {} to Namespace: {}", s, PRIMARY_KAFKA_WATCHED_NAMESPACE, testStorage.getNamespaceName());
                copySecret(s, testStorage.getNamespaceName(), testStorage.getUsername());
            }
        }

        LOGGER.info("Verifying KafkaUser: {}/{} by using its credentials to communicate with Kafka", PRIMARY_KAFKA_WATCHED_NAMESPACE, testStorage.getUsername());
        final KafkaClients kafkaClients = ClientUtils.getInstantTlsClients(testStorage, KafkaResources.tlsBootstrapAddress(PRIMARY_KAFKA_NAME));
        KubeResourceManager.get().createResourceWithWait(
            kafkaClients.producerTlsStrimzi(PRIMARY_KAFKA_NAME),
            kafkaClients.consumerTlsStrimzi(PRIMARY_KAFKA_NAME)
        );
        ClientUtils.waitForClientsSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getProducerName(), testStorage.getMessageCount());
    }

    /**
     * @description This test case verifies that KafkaMirrorMaker2 CustomResource can be created correctly in different namespace than the one containing Cluster Operator.
     *
     * @steps
     *  1. - As part of setup source Kafka cluster is deployed in main namespace,
     *     - Kafka and its components are deployed and ready.
     *  2. - Second Kafka cluster is deployed in the same namespace as the first one.
     *     - Second Kafka cluster is deployed and in ready state.
     *  3. - MirrorMaker2 CustomResource is deployed in same main namespace, pointing as source and target Kafka cluster 2 Kafka Clusters mentioned in previous step.
     *     - KafkaMirrorMaker2 CustomResource is in ready state.
     *
     * @usecase
     *  - namespaces
     *  - cluster-operator-watcher
     *  - mirror-maker-2
     */
    @ParallelTest
    @Tag(MIRROR_MAKER2)
    final void testDeployMirrorMaker2InNamespaceDifferentFromCO(ExtensionContext extensionContext) {
        LOGGER.info("Deploying KafkaMirrorMaker2 in different Namespace than CO");
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final String mirrorMakerName = testStorage.getClusterName() + "-mirror-maker-2";

        LOGGER.info("Target Kafka cluster: {} and consequently MirrorMaker2: {} will be created in Namespace: {}", testStorage.getTargetClusterName(), mirrorMakerName, MAIN_TEST_NAMESPACE);
        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(MAIN_TEST_NAMESPACE, testStorage.getTargetBrokerPoolName(), testStorage.getTargetClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(MAIN_TEST_NAMESPACE, testStorage.getTargetControllerPoolName(), testStorage.getTargetClusterName(), 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(MAIN_TEST_NAMESPACE, testStorage.getTargetClusterName(), 1).build());
        KubeResourceManager.get().createResourceWithWait(KafkaMirrorMaker2Templates.kafkaMirrorMaker2(MAIN_TEST_NAMESPACE, mirrorMakerName, PRIMARY_KAFKA_NAME, testStorage.getTargetClusterName(), 1, false).build());

        LOGGER.info("KafkaMirrorMaker2: {}/{} created and ready", MAIN_TEST_NAMESPACE, mirrorMakerName);
    }


    /**
     * @description This test case verifies that KafkaConnect and KafkaConnector CustomResource can be created correctly in different namespace than the one containing Cluster Operator.
     *
     * @steps
     *  1. - As part of setup source Kafka cluster is deployed in main namespace,
     *     - Kafka and its components are deployed and ready.
     *  2. - KafkaConnect is deployed in another namespace than Cluster Operator.
     *     - KafkaConnect cluster is successfully deployed.
     *  3. - KafkaConnector of Sync type is deployed in the same namespace as KafkaConnect Cluster.
     *     - KafkaConnector is in ready state.
     *  4. - Data are produced into KafkaTopic which is consumed by formerly mentioned KafkaConnector.
     *     - Connector successfully copied data from given KafkaTopic into desired location (file).
     *
     * @usecase
     *  - namespaces
     *  - cluster-operator-watcher
     *  - connect
     *  - connector-operator
     */
    @ParallelTest
    @Tag(CONNECT)
    @Tag(CONNECTOR_OPERATOR)
    @Tag(CONNECT_COMPONENTS)
    final void testDeployKafkaConnectAndKafkaConnectorInNamespaceDifferentFromCO(ExtensionContext extensionContext) {

        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        // Deploy Kafka Connect in other namespace than CO
        KubeResourceManager.get().createResourceWithWait(KafkaConnectTemplates.kafkaConnectWithFilePlugin(MAIN_TEST_NAMESPACE, testStorage.getClusterName(), PRIMARY_KAFKA_NAME, 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .build());

        LOGGER.info("Deploying KafkaConnector: {}/{}", MAIN_TEST_NAMESPACE, testStorage.getClusterName());
        deployKafkaConnectorWithSink(extensionContext, testStorage.getClusterName());
    }

    private void copySecret(Secret sourceSecret, String targetNamespace, String targetName) {
        Secret s = new SecretBuilder(sourceSecret)
            .withNewMetadata()
                .withName(targetName)
                .withNamespace(targetNamespace)
            .endMetadata()
            .build();

        KubeResourceManager.get().createResourceWithWait(s);
    }

    private void deployKafkaConnectorWithSink(ExtensionContext extensionContext, String clusterName) {
        final TestStorage testStorage = new TestStorage(extensionContext, MAIN_TEST_NAMESPACE);

        // Deploy Kafka Connector
        Map<String, Object> connectorConfig = new HashMap<>();
        connectorConfig.put("topics", testStorage.getTopicName());
        connectorConfig.put("file", TestConstants.DEFAULT_SINK_FILE_PATH);
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");

        LOGGER.info("Creating KafkaConnector: {}/{}", testStorage.getNamespaceName(), clusterName);
        KubeResourceManager.get().createResourceWithWait(KafkaConnectorTemplates.kafkaConnector(testStorage.getNamespaceName(), clusterName)
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .withConfig(connectorConfig)
            .endSpec()
            .build());
        KafkaConnectorUtils.waitForConnectorReady(testStorage.getNamespaceName(), clusterName);

        String kafkaConnectPodName = KubeResourceManager.get().kubeClient().listPods(testStorage.getNamespaceName(),
            LabelSelectors.connectLabelSelector(clusterName, KafkaConnectResources.componentName(clusterName))).get(0).getMetadata().getName();
        LOGGER.info("KafkaConnect Pod: {}/{}", testStorage.getNamespaceName(), kafkaConnectPodName);
        KafkaConnectUtils.waitUntilKafkaConnectRestApiIsAvailable(testStorage.getNamespaceName(), kafkaConnectPodName);

        final KafkaClients kafkaClients = ClientUtils.getInstantPlainClients(testStorage, KafkaResources.plainBootstrapAddress(PRIMARY_KAFKA_NAME));
        KubeResourceManager.get().createResourceWithWait(kafkaClients.producerStrimzi(), kafkaClients.consumerStrimzi());
        ClientUtils.waitForInstantClientSuccess(testStorage);

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(testStorage.getNamespaceName(), kafkaConnectPodName, TestConstants.DEFAULT_SINK_FILE_PATH, testStorage.getMessageCount());
    }

    /**
     * Helper method which deploys Kafka cluster and Scraper Pod in primary namespace. It is supposed to be called once there is a running Cluster Operator.
     *
     */
    final protected void deployAdditionalGenericResourcesForAbstractNamespaceST() {

        LOGGER.info("Deploying additional Kafka cluster and Scraper in Namespace: {}", MAIN_TEST_NAMESPACE);

        final String scraperName = PRIMARY_KAFKA_NAME + "-" + TestConstants.SCRAPER_NAME;

        cluster.setNamespace(MAIN_TEST_NAMESPACE);

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(MAIN_TEST_NAMESPACE, KafkaComponents.getBrokerPoolName(PRIMARY_KAFKA_NAME), PRIMARY_KAFKA_NAME, 3).build(),
            KafkaNodePoolTemplates.controllerPool(MAIN_TEST_NAMESPACE, KafkaComponents.getControllerPoolName(PRIMARY_KAFKA_NAME), PRIMARY_KAFKA_NAME, 3).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(MAIN_TEST_NAMESPACE, PRIMARY_KAFKA_NAME, 3)
            .editSpec()
                .withCruiseControl(new CruiseControlSpec())
                .withKafkaExporter(new KafkaExporterSpec())
                .editEntityOperator()
                    .editTopicOperator()
                        .withWatchedNamespace(PRIMARY_KAFKA_WATCHED_NAMESPACE)
                    .endTopicOperator()
                    .editUserOperator()
                        .withWatchedNamespace(PRIMARY_KAFKA_WATCHED_NAMESPACE)
                    .endUserOperator()
                .endEntityOperator()
            .endSpec()
            .build(),
            ScraperTemplates.scraperPod(MAIN_TEST_NAMESPACE, scraperName).build()
        );

        scraperPodName = KubeResourceManager.get().kubeClient().listPodsByPrefixInName(MAIN_TEST_NAMESPACE, scraperName).get(0).getMetadata().getName();

    }
}
