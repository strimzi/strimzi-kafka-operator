/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.rbac.KubernetesClusterRoleBinding;
import io.strimzi.test.annotations.ClusterOperator;
import io.strimzi.test.annotations.Namespace;
import io.strimzi.test.extensions.StrimziExtension;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static io.strimzi.test.extensions.StrimziExtension.REGRESSION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;

@ExtendWith(StrimziExtension.class)
@Namespace(AllNamespaceST.CO_NAMESPACE)
@Namespace(value = AllNamespaceST.SECOND_NAMESPACE, use = false)
@Namespace(value = AllNamespaceST.THIRD_NAMESPACE, use = false)
@ClusterOperator
class AllNamespaceST extends AbstractNamespaceST {

    private static final Logger LOGGER = LogManager.getLogger(AllNamespaceST.class);
    static final String THIRD_NAMESPACE = "third-namespace-test";
    private static Resources thirdNamespaceResources;

    /**
     * Test the case where the TO is configured to watch a different namespace that it is deployed in
     */
    @Test
    @Tag(REGRESSION)
    void testTopicOperatorWatchingOtherNamespace() {
        LOGGER.info("Deploying TO to watch a different namespace that it is deployed in");

        kubeClient.namespace(THIRD_NAMESPACE);

        List<String> topics = listTopicsUsingPodCLI(CLUSTER_NAME, 0);
        assertThat(topics, not(hasItems(TOPIC_NAME)));

        deployNewTopic(SECOND_NAMESPACE, THIRD_NAMESPACE, TOPIC_NAME);
        deleteNewTopic(SECOND_NAMESPACE, TOPIC_NAME);
    }

    /**
     * Test the case when Kafka will be deployed in different namespace than CO
     */
    @Test
    @Tag(REGRESSION)
    void testKafkaInDifferentNsThanClusterOperator() {
        LOGGER.info("Deploying Kafka cluster in different namespace than CO when CO watches all namespaces");
        checkKafkaInDiffNamespaceThanCO();
    }

    /**
     * Test the case when MirrorMaker will be deployed in different namespace than CO when CO watches all namespaces
     */
    @Test
    @Tag(REGRESSION)
    void testDeployMirrorMakerAcrossMultipleNamespace() {
        LOGGER.info("Deploying Kafka MirrorMaker in different namespace than CO when CO watches all namespaces");
        checkMirrorMakerForKafkaInDifNamespaceThanCO();
    }

    @Test
    @Tag(REGRESSION)
    void testDeployKafkaConnectInOtherNamespaceThanCO() {
        // Deploy Kafka in other namespace than CO
        secondNamespaceResources.kafkaEphemeral(CLUSTER_NAME, 3).done();
        // Deploy Kafka Connect in other namespace than CO
        secondNamespaceResources.kafkaConnect(CLUSTER_NAME, 1).done();
        // Check that Kafka Connect was deployed
        kubeClient.waitForDeployment(kafkaConnectName(CLUSTER_NAME), 1);
    }

    @Test
    @Tag(REGRESSION)
    void testUOWatchingOtherNamespace() {
        LOGGER.info("Creating user in other namespace than CO and Kafka cluster with UO");
        secondNamespaceResources.tlsUser(CLUSTER_NAME, USER_NAME).done();

        String previousNamespace = kubeClient.namespace(SECOND_NAMESPACE);
        // Check that UO created a secret for new user
        kubeClient.waitForResourceCreation("Secret", USER_NAME);

        kubeClient.namespace(previousNamespace);
    }

    @BeforeAll
    static void createClassResources(TestInfo testInfo) {
        // Apply role bindings in CO namespace
        applyRoleBindings(CO_NAMESPACE);

        // Create ClusterRoleBindings that grant cluster-wide access to all OpenShift projects
        List<KubernetesClusterRoleBinding> clusterRoleBindingList = testClassResources.clusterRoleBindingsForAllNamespaces(CO_NAMESPACE);
        clusterRoleBindingList.forEach(kubernetesClusterRoleBinding ->
            testClassResources.kubernetesClusterRoleBinding(kubernetesClusterRoleBinding, CO_NAMESPACE));

        LOGGER.info("Deploying CO to watch all namespaces");
        testClassResources.clusterOperator("*").done();

        String previousNamespace = kubeClient.namespace(THIRD_NAMESPACE);
        thirdNamespaceResources = new Resources(namespacedClient());

        thirdNamespaceResources.kafkaEphemeral(CLUSTER_NAME, 1)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .withNewTls()
                        .endTls()
                    .endListeners()
                .endKafka()
                .editEntityOperator()
                    .editTopicOperator()
                        .withWatchedNamespace(SECOND_NAMESPACE)
                    .endTopicOperator()
                    .editUserOperator()
                        .withWatchedNamespace(SECOND_NAMESPACE)
                    .endUserOperator()
                .endEntityOperator()
            .endSpec()
            .done();

        kubeClient.namespace(previousNamespace);

        testClass = testInfo.getTestClass().get().getSimpleName();
    }

    @AfterAll
    static void deleteClassResources() {
        thirdNamespaceResources.deleteResources();
    }
}
