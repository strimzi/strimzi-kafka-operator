/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.watcher;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.OpenShiftOnly;
import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.resources.kubernetes.ClusterRoleBindingResource;
import io.strimzi.systemtest.resources.operator.BundleResource;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectS2ITemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.templates.kubernetes.ClusterRoleBindingTemplates;
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
import static io.strimzi.systemtest.Constants.CONNECT_S2I;
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
class AllNamespaceST extends AbstractNamespaceST {

    private static final Logger LOGGER = LogManager.getLogger(AllNamespaceST.class);
    private static final String THIRD_NAMESPACE = "third-namespace-test";
    private static final String SECOND_CLUSTER_NAME = MAIN_NAMESPACE_CLUSTER_NAME + "-second";

    /**
     * Test the case where the TO is configured to watch a different namespace that it is deployed in
     */
    @IsolatedTest
    void testTopicOperatorWatchingOtherNamespace(ExtensionContext extensionContext) {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        LOGGER.info("Deploying TO to watch a different namespace that it is deployed in");
        String previousNamespace = cluster.setNamespace(THIRD_NAMESPACE);
        List<String> topics = KafkaCmdClient.listTopicsUsingPodCli(MAIN_NAMESPACE_CLUSTER_NAME, 0);
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
    void testKafkaInDifferentNsThanClusterOperator(ExtensionContext extensionContext) {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        LOGGER.info("Deploying Kafka cluster in different namespace than CO when CO watches all namespaces");
        checkKafkaInDiffNamespaceThanCO(SECOND_CLUSTER_NAME, SECOND_NAMESPACE);
    }

    /**
     * Test the case when MirrorMaker will be deployed in different namespace than CO when CO watches all namespaces
     */
    @IsolatedTest
    @Tag(MIRROR_MAKER)
    void testDeployMirrorMakerAcrossMultipleNamespace(ExtensionContext extensionContext) {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        LOGGER.info("Deploying KafkaMirrorMaker in different namespace than CO when CO watches all namespaces");
        checkMirrorMakerForKafkaInDifNamespaceThanCO(extensionContext, SECOND_CLUSTER_NAME);
    }

    @IsolatedTest
    @Tag(CONNECT)
    @Tag(CONNECTOR_OPERATOR)
    @Tag(CONNECT_COMPONENTS)
    void testDeployKafkaConnectAndKafkaConnectorInOtherNamespaceThanCO(ExtensionContext extensionContext) {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        String kafkaConnectName = mapWithClusterNames.get(extensionContext.getDisplayName()) + "kafka-connect";
        String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());

        String previousNamespace = cluster.setNamespace(SECOND_NAMESPACE);
        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, kafkaClientsName).build());
        // Deploy Kafka Connect in other namespace than CO
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(extensionContext, kafkaConnectName, SECOND_CLUSTER_NAME, 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .build());
        // Deploy Kafka Connector
        deployKafkaConnectorWithSink(extensionContext, kafkaConnectName, SECOND_NAMESPACE, TOPIC_NAME, KafkaConnect.RESOURCE_KIND, SECOND_CLUSTER_NAME);

        cluster.setNamespace(previousNamespace);
    }

    @IsolatedTest
    @OpenShiftOnly
    @Tag(CONNECT_S2I)
    @Tag(CONNECTOR_OPERATOR)
    @Tag(CONNECT_COMPONENTS)
    void testDeployKafkaConnectS2IAndKafkaConnectorInOtherNamespaceThanCO(ExtensionContext extensionContext) {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        String kafkaConnectS2IName = mapWithClusterNames.get(extensionContext.getDisplayName()) + "kafka-connect-s2i";
        String previousNamespace = cluster.setNamespace(SECOND_NAMESPACE);
        String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, kafkaClientsName).build());
        // Deploy Kafka Connect in other namespace than CO
        resourceManager.createResource(extensionContext, KafkaConnectS2ITemplates.kafkaConnectS2I(extensionContext, kafkaConnectS2IName, SECOND_CLUSTER_NAME, 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .build());
        // Deploy Kafka Connector
        deployKafkaConnectorWithSink(extensionContext, kafkaConnectS2IName, SECOND_NAMESPACE, TOPIC_NAME, KafkaConnectS2I.RESOURCE_KIND, SECOND_CLUSTER_NAME);

        cluster.setNamespace(previousNamespace);
    }

    @IsolatedTest
    void testUOWatchingOtherNamespace(ExtensionContext extensionContext) {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        String previousNamespace = cluster.setNamespace(SECOND_NAMESPACE);
        LOGGER.info("Creating user in other namespace than CO and Kafka cluster with UO");
        resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(MAIN_NAMESPACE_CLUSTER_NAME, USER_NAME).build());

        cluster.setNamespace(previousNamespace);
    }

    @IsolatedTest
    void testUserInDifferentNamespace(ExtensionContext extensionContext) {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        String startingNamespace = cluster.setNamespace(SECOND_NAMESPACE);

        KafkaUser user = KafkaUserTemplates.tlsUser(MAIN_NAMESPACE_CLUSTER_NAME, USER_NAME).build();

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

        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, MAIN_NAMESPACE_CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, user).build());

        final String defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(MAIN_NAMESPACE_CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(defaultKafkaClientsPodName)
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(THIRD_NAMESPACE)
            .withClusterName(MAIN_NAMESPACE_CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withKafkaUsername(USER_NAME)
            .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
            .build();

        LOGGER.info("Checking produced and consumed messages to pod:{}", defaultKafkaClientsPodName);

        int sent = internalKafkaClient.sendMessagesTls();
        assertThat(sent, is(MESSAGE_COUNT));

        int received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(MESSAGE_COUNT));

        cluster.setNamespace(startingNamespace);
    }

    void copySecret(Secret sourceSecret, String targetNamespace, String targetName) {
        Secret s = new SecretBuilder(sourceSecret)
                    .withNewMetadata()
                        .withName(targetName)
                        .withNamespace(targetNamespace)
                    .endMetadata()
                    .build();
        kubeClient(targetNamespace).getClient().secrets().inNamespace(targetNamespace).createOrReplace(s);
    }

    private void deployTestSpecificResources(ExtensionContext extensionContext) {
        LOGGER.info("Creating resources before the test class");
        prepareEnvForOperator(extensionContext, CO_NAMESPACE, Arrays.asList(CO_NAMESPACE, SECOND_NAMESPACE, THIRD_NAMESPACE));

        // Apply role bindings in CO namespace
        applyBindings(extensionContext, CO_NAMESPACE);

        // Create ClusterRoleBindings that grant cluster-wide access to all OpenShift projects
        List<ClusterRoleBinding> clusterRoleBindingList = ClusterRoleBindingTemplates.clusterRoleBindingsForAllNamespaces(CO_NAMESPACE);
        clusterRoleBindingList.forEach(clusterRoleBinding ->
            ClusterRoleBindingResource.clusterRoleBinding(extensionContext, clusterRoleBinding));
        // 060-Deployment
        resourceManager.createResource(extensionContext, BundleResource.clusterOperator(CO_NAMESPACE, "*", Constants.RECONCILIATION_INTERVAL).build());

        String previousNamespace = cluster.setNamespace(THIRD_NAMESPACE);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(MAIN_NAMESPACE_CLUSTER_NAME, 1, 1)
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
            .build());

        cluster.setNamespace(SECOND_NAMESPACE);
        // Deploy Kafka in other namespace than CO
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(SECOND_CLUSTER_NAME, 3).build());

        cluster.setNamespace(previousNamespace);
    }

    @BeforeAll
    void setupEnvironment(ExtensionContext extensionContext) {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        deployTestSpecificResources(extensionContext);
    }
}
