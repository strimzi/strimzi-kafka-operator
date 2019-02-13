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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static io.strimzi.test.extensions.StrimziExtension.REGRESSION;

@ExtendWith(StrimziExtension.class)
@Namespace(MultipleNamespaceST.CO_NAMESPACE)
@Namespace(value = MultipleNamespaceST.SECOND_NAMESPACE, use = false)
@ClusterOperator
public class AllNamespaceST extends AbstractNamespaceST {

    private static final Logger LOGGER = LogManager.getLogger(AllNamespaceST.class);

    /**
     * Test the case where the TO is configured to watch a different namespace that it is deployed in
     */
    @Test
    @Tag(REGRESSION)
    void testTopicOperatorWatchingOtherNamespace() {
        LOGGER.info("Deploying TO in different namespace than CO when CO watches all namespaces");
        checkTOInDiffNamespaceThanCO();
    }

    /**
     * Test the case when Kafka will be deployed in different namespace than CO
     */
    @Test
    @Tag(REGRESSION)
    void testKafkaInDifferentNsThanClusterOperator() throws InterruptedException {
        LOGGER.info("Deploying Kafka cluster in different namespace than CO when CO watches all namespaces");
        checkKafkaInDiffNamespaceThanCO();
    }

    /**
     * Test the case when MirrorMaker will be deployed in different namespace across multiple namespaces
     */
    @Test
    @Tag(REGRESSION)
    void testDeployMirrorMakerAcrossMultipleNamespace() throws InterruptedException {
        LOGGER.info("Deploying Kafka MirrorMaker in different namespace than CO when CO watches all namespaces");
        checkMirrorMakerForKafkaInDifNamespaceThanCO();
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

        classResources = new Resources(namespacedClient());
        classResources().kafkaEphemeral(CLUSTER_NAME, 3)
            .editSpec()
                .editEntityOperator()
                    .editTopicOperator()
                        .withWatchedNamespace(SECOND_NAMESPACE)
                    .endTopicOperator()
                .endEntityOperator()
            .endSpec()
            .done();

        testClass = testInfo.getTestClass().get().getSimpleName();
    }

    private static Resources classResources() {
        return classResources;
    }
}
