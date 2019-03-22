/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.rbac.KubernetesClusterRoleBinding;
import io.strimzi.test.extensions.StrimziExtension;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.List;

import static io.strimzi.test.extensions.StrimziExtension.REGRESSION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;

@ExtendWith(StrimziExtension.class)
class AllNamespaceST extends AbstractNamespaceST {

    private static final Logger LOGGER = LogManager.getLogger(AllNamespaceST.class);
    private static final String THIRD_NAMESPACE = "third-namespace-test";
    private static Resources thirdNamespaceResources;

    /**
     * Test the case where the TO is configured to watch a different namespace that it is deployed in
     */
    @Test
    @Tag(REGRESSION)
    void testTopicOperatorWatchingOtherNamespace() {
        LOGGER.info("Deploying TO to watch a different namespace that it is deployed in");
        String previousNamespce = KUBE_CLIENT.namespace(THIRD_NAMESPACE);
        List<String> topics = listTopicsUsingPodCLI(CLUSTER_NAME, 0);
        assertThat(topics, not(hasItems(TOPIC_NAME)));

        deployNewTopic(SECOND_NAMESPACE, THIRD_NAMESPACE, TOPIC_NAME);
        deleteNewTopic(SECOND_NAMESPACE, TOPIC_NAME);
        KUBE_CLIENT.namespace(previousNamespce);
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
        String previousNamespace = KUBE_CLIENT.namespace(SECOND_NAMESPACE);
        // Deploy Kafka in other namespace than CO
        secondNamespaceResources.kafkaEphemeral(CLUSTER_NAME, 3).done();
        // Deploy Kafka Connect in other namespace than CO
        secondNamespaceResources.kafkaConnect(CLUSTER_NAME, 1).done();
        // Check that Kafka Connect was deployed
        KUBE_CLIENT.waitForDeployment(kafkaConnectName(CLUSTER_NAME), 1);
        KUBE_CLIENT.namespace(previousNamespace);
    }

    @Test
    @Tag(REGRESSION)
    void testUOWatchingOtherNamespace() {
        LOGGER.info("Creating user in other namespace than CO and Kafka cluster with UO");
        secondNamespaceResources.tlsUser(CLUSTER_NAME, USER_NAME).done();

        String previousNamespace = KUBE_CLIENT.namespace(SECOND_NAMESPACE);
        // Check that UO created a secret for new user
        KUBE_CLIENT.waitForResourceCreation("Secret", USER_NAME);

        KUBE_CLIENT.namespace(previousNamespace);
    }

    private void deployTestSpecificResources() {
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
    }

    @BeforeAll
    void setupEnvironment(TestInfo testInfo) {
        LOGGER.info("Creating resources before the test class");
        prepareEnvForOperator(CO_NAMESPACE, Arrays.asList(CO_NAMESPACE, SECOND_NAMESPACE, THIRD_NAMESPACE));
        createTestClassResources();

        // Apply role bindings in CO namespace
        applyRoleBindings(CO_NAMESPACE);

        // Create ClusterRoleBindings that grant cluster-wide access to all OpenShift projects
        List<KubernetesClusterRoleBinding> clusterRoleBindingList = testClassResources.clusterRoleBindingsForAllNamespaces(CO_NAMESPACE);
        clusterRoleBindingList.forEach(kubernetesClusterRoleBinding ->
                testClassResources.kubernetesClusterRoleBinding(kubernetesClusterRoleBinding, CO_NAMESPACE));

        LOGGER.info("Deploying CO to watch all namespaces");
        testClassResources.clusterOperator("*").done();

        String previousNamespace = KUBE_CLIENT.namespace(THIRD_NAMESPACE);
        thirdNamespaceResources = new Resources(namespacedClient());

        deployTestSpecificResources();

        KUBE_CLIENT.namespace(previousNamespace);
    }

    @AfterAll
    void teardownEnvironment() {
        thirdNamespaceResources.deleteResources();
        testClassResources.deleteResources();
        teardownEnvForOperator();
    }

    @Override
    void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) {
        super.recreateTestEnv(coNamespace, bindingsNamespaces);
        deployTestSpecificResources();
    }
}
