/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.watcher;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.IsolatedSuite;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Arrays;
import java.util.List;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CONNECTOR_OPERATOR;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@Tag(REGRESSION)
@IsolatedSuite
class AllNamespaceIsolatedST extends AbstractNamespaceST {

    private static final Logger LOGGER = LogManager.getLogger(AllNamespaceIsolatedST.class);
    private static final String THIRD_NAMESPACE = "third-namespace-test";
    private String scraperPodName;

    /**
     * Test the case where the TO is configured to watch a different namespace that it is deployed in
     */
    @IsolatedTest
    @KRaftNotSupported("TopicOperator is not supported by KRaft mode and is used in this test case")
    void testTopicOperatorWatchingOtherNamespace(ExtensionContext extensionContext) {
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        LOGGER.info("Deploying TO to watch a different namespace that it is deployed in");
        String previousNamespace = cluster.setNamespace(THIRD_NAMESPACE);
        List<String> topics = KafkaCmdClient.listTopicsUsingPodCli(THIRD_NAMESPACE, scraperPodName, KafkaResources.plainBootstrapAddress(MAIN_NAMESPACE_CLUSTER_NAME));
        assertThat(topics, not(hasItems(TOPIC_NAME)));

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(MAIN_NAMESPACE_CLUSTER_NAME, topicName, SECOND_NAMESPACE).build());
        KafkaTopicResource.kafkaTopicClient().inNamespace(SECOND_NAMESPACE).withName(topicName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
        cluster.setNamespace(previousNamespace);
    }

    /**
     * Test the case when Kafka will be deployed in different namespace than CO
     */
    @IsolatedTest
    @Tag(ACCEPTANCE)
    void testKafkaInDifferentNsThanClusterOperator() {
        LOGGER.info("Deploying Kafka cluster in different namespace than CO when CO watches all namespaces");
        checkKafkaInDiffNamespaceThanCO(SECOND_CLUSTER_NAME, SECOND_NAMESPACE);
    }

    /**
     * Test the case when MirrorMaker will be deployed in different namespace than CO when CO watches all namespaces
     */
    @IsolatedTest
    @Tag(MIRROR_MAKER)
    void testDeployMirrorMakerAcrossMultipleNamespace(ExtensionContext extensionContext) {
        LOGGER.info("Deploying KafkaMirrorMaker in different namespace than CO when CO watches all namespaces");
        checkMirrorMakerForKafkaInDifNamespaceThanCO(extensionContext, SECOND_CLUSTER_NAME);
    }

    @IsolatedTest
    @Tag(CONNECT)
    @Tag(CONNECTOR_OPERATOR)
    @Tag(CONNECT_COMPONENTS)
    void testDeployKafkaConnectAndKafkaConnectorInOtherNamespaceThanCO(ExtensionContext extensionContext) {
        String kafkaConnectName = mapWithClusterNames.get(extensionContext.getDisplayName()) + "kafka-connect";

        String previousNamespace = cluster.setNamespace(SECOND_NAMESPACE);
        // Deploy Kafka Connect in other namespace than CO
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnectWithFilePlugin(kafkaConnectName, SECOND_NAMESPACE, SECOND_CLUSTER_NAME, 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .build());
        // Deploy Kafka Connector
        deployKafkaConnectorWithSink(extensionContext, kafkaConnectName);

        cluster.setNamespace(previousNamespace);
    }

    @IsolatedTest
    void testUOWatchingOtherNamespace(ExtensionContext extensionContext) {
        String previousNamespace = cluster.setNamespace(SECOND_NAMESPACE);
        LOGGER.info("Creating user in other namespace than CO and Kafka cluster with UO");
        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(cluster.getNamespace(), MAIN_NAMESPACE_CLUSTER_NAME, USER_NAME).build());

        cluster.setNamespace(previousNamespace);
    }

    @IsolatedTest
    void testUserInDifferentNamespace(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, SECOND_NAMESPACE);
        String startingNamespace = cluster.setNamespace(SECOND_NAMESPACE);

        KafkaUser user = KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), MAIN_NAMESPACE_CLUSTER_NAME, USER_NAME).build();

        resourceManager.createResource(extensionContext, user);

        Condition kafkaCondition = KafkaUserResource.kafkaUserClient().inNamespace(SECOND_NAMESPACE).withName(USER_NAME)
                .get().getStatus().getConditions().get(0);
        LOGGER.info("KafkaUser condition status: {}", kafkaCondition.getStatus());
        LOGGER.info("KafkaUser condition type: {}", kafkaCondition.getType());

        assertThat(kafkaCondition.getType(), is(Ready.toString()));

        List<Secret> secretsOfSecondNamespace = kubeClient(SECOND_NAMESPACE).listSecrets();

        cluster.setNamespace(THIRD_NAMESPACE);

        for (Secret s : secretsOfSecondNamespace) {
            if (s.getMetadata().getName().equals(USER_NAME)) {
                LOGGER.info("Copying secret {} from namespace {} to namespace {}", s, SECOND_NAMESPACE, THIRD_NAMESPACE);
                copySecret(s, THIRD_NAMESPACE, USER_NAME);
            }
        }

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(TOPIC_NAME)
            .withMessageCount(testStorage.getMessageCount())
            .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(MAIN_NAMESPACE_CLUSTER_NAME))
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(THIRD_NAMESPACE)
            .withUserName(USER_NAME)
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerTlsStrimzi(MAIN_NAMESPACE_CLUSTER_NAME), kafkaClients.consumerTlsStrimzi(MAIN_NAMESPACE_CLUSTER_NAME));
        ClientUtils.waitForClientsSuccess(testStorage.getProducerName(), testStorage.getConsumerName(), THIRD_NAMESPACE, testStorage.getMessageCount());

        cluster.setNamespace(startingNamespace);
    }

    void copySecret(Secret sourceSecret, String targetNamespace, String targetName) {
        Secret s = new SecretBuilder(sourceSecret)
                    .withNewMetadata()
                        .withName(targetName)
                        .withNamespace(targetNamespace)
                    .endMetadata()
                    .build();

        kubeClient(targetNamespace).getClient().secrets().inNamespace(targetNamespace).resource(s).createOrReplace();
    }

    private void deployTestSpecificResources(ExtensionContext extensionContext) {
        LOGGER.info("Creating resources before the test class");

        final String scraperName = MAIN_NAMESPACE_CLUSTER_NAME + "-" + Constants.SCRAPER_NAME;

        clusterOperator.unInstall();
        clusterOperator = clusterOperator.defaultInstallation()
            .withWatchingNamespaces(Constants.WATCH_ALL_NAMESPACES)
            .withBindingsNamespaces(Arrays.asList(clusterOperator.getDeploymentNamespace(), SECOND_NAMESPACE, THIRD_NAMESPACE))
            .createInstallation()
            .runInstallation();

        String previousNamespace = cluster.setNamespace(THIRD_NAMESPACE);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(MAIN_NAMESPACE_CLUSTER_NAME, 1)
            .editSpec()
                .editEntityOperator()
                    .editTopicOperator()
                        .withWatchedNamespace(SECOND_NAMESPACE)
                    .endTopicOperator()
                    .editUserOperator()
                        .withWatchedNamespace(SECOND_NAMESPACE)
                    .endUserOperator()
                .endEntityOperator()
            .endSpec()
            .build(),
            ScraperTemplates.scraperPod(THIRD_NAMESPACE, scraperName).build()
        );

        scraperPodName = kubeClient().listPodsByPrefixInName(THIRD_NAMESPACE, scraperName).get(0).getMetadata().getName();

        cluster.setNamespace(SECOND_NAMESPACE);
        // Deploy Kafka in other namespace than CO
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(SECOND_CLUSTER_NAME, 1).build());

        cluster.setNamespace(previousNamespace);
    }

    @BeforeAll
    void setupEnvironment(ExtensionContext extensionContext) {
        // Strimzi is deployed with cluster-wide access in this class STRIMZI_RBAC_SCOPE=NAMESPACE won't work
        assumeFalse(Environment.isNamespaceRbacScope());

        deployTestSpecificResources(extensionContext);
    }
}
