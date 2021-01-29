/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.watcher;

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
import io.strimzi.systemtest.annotations.OpenShiftOnly;
import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectS2IResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.resources.operator.BundleResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

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
    @Test
    void testTopicOperatorWatchingOtherNamespace() {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        LOGGER.info("Deploying TO to watch a different namespace that it is deployed in");
        String previousNamespace = cluster.setNamespace(THIRD_NAMESPACE);
        List<String> topics = KafkaCmdClient.listTopicsUsingPodCli(MAIN_NAMESPACE_CLUSTER_NAME, 0);
        assertThat(topics, not(hasItems(TOPIC_NAME)));

        deployNewTopic(SECOND_NAMESPACE, THIRD_NAMESPACE, EXAMPLE_TOPIC_NAME);
        deleteNewTopic(SECOND_NAMESPACE, EXAMPLE_TOPIC_NAME);
        cluster.setNamespace(previousNamespace);
    }

    /**
     * Test the case when Kafka will be deployed in different namespace than CO
     */
    @Test
    @Tag(ACCEPTANCE)
    void testKafkaInDifferentNsThanClusterOperator() {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        LOGGER.info("Deploying Kafka cluster in different namespace than CO when CO watches all namespaces");
        checkKafkaInDiffNamespaceThanCO(SECOND_CLUSTER_NAME, SECOND_NAMESPACE);
    }

    /**
     * Test the case when MirrorMaker will be deployed in different namespace than CO when CO watches all namespaces
     */
    @Test
    @Tag(MIRROR_MAKER)
    void testDeployMirrorMakerAcrossMultipleNamespace() {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        LOGGER.info("Deploying KafkaMirrorMaker in different namespace than CO when CO watches all namespaces");
        checkMirrorMakerForKafkaInDifNamespaceThanCO(SECOND_CLUSTER_NAME);
    }

    @Test
    @Tag(CONNECT)
    @Tag(CONNECTOR_OPERATOR)
    @Tag(CONNECT_COMPONENTS)
    void testDeployKafkaConnectAndKafkaConnectorInOtherNamespaceThanCO() {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        String previousNamespace = cluster.setNamespace(SECOND_NAMESPACE);
        KafkaClientsResource.createAndWaitForReadiness(KafkaClientsResource.deployKafkaClients(false, SECOND_CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).build());
        // Deploy Kafka Connect in other namespace than CO
        KafkaConnectResource.createAndWaitForReadiness(KafkaConnectResource.kafkaConnect(SECOND_CLUSTER_NAME, 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .build());
        // Deploy Kafka Connector
        deployKafkaConnectorWithSink(SECOND_CLUSTER_NAME, SECOND_NAMESPACE, TOPIC_NAME, KafkaConnect.RESOURCE_KIND);

        cluster.setNamespace(previousNamespace);
    }

    @Test
    @OpenShiftOnly
    @Tag(CONNECT_S2I)
    @Tag(CONNECTOR_OPERATOR)
    @Tag(CONNECT_COMPONENTS)
    void testDeployKafkaConnectS2IAndKafkaConnectorInOtherNamespaceThanCO() {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        String previousNamespace = cluster.setNamespace(SECOND_NAMESPACE);
        KafkaClientsResource.createAndWaitForReadiness(KafkaClientsResource.deployKafkaClients(false, SECOND_CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).build());
        // Deploy Kafka Connect in other namespace than CO
        KafkaConnectS2IResource.createAndWaitForReadiness(KafkaConnectS2IResource.kafkaConnectS2I(SECOND_CLUSTER_NAME, SECOND_CLUSTER_NAME, 1)
            .editMetadata()
                .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
            .endMetadata()
            .build());
        // Deploy Kafka Connector
        deployKafkaConnectorWithSink(SECOND_CLUSTER_NAME, SECOND_NAMESPACE, TOPIC_NAME, KafkaConnectS2I.RESOURCE_KIND);

        cluster.setNamespace(previousNamespace);
    }

    @Test
    void testUOWatchingOtherNamespace() {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        String previousNamespace = cluster.setNamespace(SECOND_NAMESPACE);
        LOGGER.info("Creating user in other namespace than CO and Kafka cluster with UO");
        KafkaUserResource.createAndWaitForReadiness(KafkaUserResource.tlsUser(MAIN_NAMESPACE_CLUSTER_NAME, USER_NAME).build());

        cluster.setNamespace(previousNamespace);
    }

    @Test
    void testUserInDifferentNamespace() {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        String startingNamespace = cluster.setNamespace(SECOND_NAMESPACE);
        KafkaUser user = KafkaUserResource.createAndWaitForReadiness(KafkaUserResource.tlsUser(MAIN_NAMESPACE_CLUSTER_NAME, USER_NAME).build());

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

        KafkaClientsResource.createAndWaitForReadiness(KafkaClientsResource.deployKafkaClients(true, MAIN_NAMESPACE_CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, user).build());

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

    private void deployTestSpecificResources() {
        LOGGER.info("Creating resources before the test class");
        prepareEnvForOperator(CO_NAMESPACE, Arrays.asList(CO_NAMESPACE, SECOND_NAMESPACE, THIRD_NAMESPACE));

        // Apply role bindings in CO namespace
        applyBindings(CO_NAMESPACE);

        // Create ClusterRoleBindings that grant cluster-wide access to all OpenShift projects
        List<ClusterRoleBinding> clusterRoleBindingList = KubernetesResource.clusterRoleBindingsForAllNamespaces(CO_NAMESPACE);
        clusterRoleBindingList.forEach(clusterRoleBinding ->
                KubernetesResource.clusterRoleBinding(clusterRoleBinding));
        // 060-Deployment
        BundleResource.createAndWaitForReadiness(BundleResource.clusterOperator("*").build());

        String previousNamespace = cluster.setNamespace(THIRD_NAMESPACE);

        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(MAIN_NAMESPACE_CLUSTER_NAME, 1, 1)
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
        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(SECOND_CLUSTER_NAME, 3).build());

        cluster.setNamespace(previousNamespace);
    }

    @BeforeAll
    void setupEnvironment() {
        // TODO issue #4152 - temporarily disabled for Namespace RBAC scoped
        assumeFalse(Environment.isNamespaceRbacScope());

        deployTestSpecificResources();
    }
}
