/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.operator.BundleResource;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.resources.ResourceManager.cmdKubeClient;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(REGRESSION)
public class ClusterOperatorRbacST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(ClusterOperatorRbacST.class);
    public static final String NAMESPACE = "cluster-operator-test";

    @Test
    @Tag(CONNECT)
    void testCRBDeletionErrorIsIgnoredWhenRackAwarenessIsNotEnabled() {
        applyRoleBindingsWithoutCRBs();
        // 060-Deployment
        BundleResource.clusterOperator(NAMESPACE).done();

        String coPodName = kubeClient().getClusterOperatorPodName();
        LOGGER.info("Deploying Kafka: {}, which should be deployed even the CRBs are not present", CLUSTER_NAME);
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3).done();

        LOGGER.info("CO log should contain some information about ignoring forbidden access to CRB for Kafka");
        String log = cmdKubeClient().execInCurrentNamespace(false, "logs", coPodName).out();
        assertTrue(log.contains("Ignoring forbidden access to ClusterRoleBindings resource which does not seem to be required."));

        LOGGER.info("Deploying KafkaConnect: {} without rack awareness, the CR should be deployed without error", CLUSTER_NAME);
        KafkaConnectResource.kafkaConnect(CLUSTER_NAME, 1, false).done();

        LOGGER.info("CO log should contain some information about ignoring forbidden access to CRB for KafkaConnect");
        log = cmdKubeClient().execInCurrentNamespace(false, "logs", coPodName, "--tail", "50").out();
        assertTrue(log.contains("Ignoring forbidden access to ClusterRoleBindings resource which does not seem to be required."));
    }

    @Test
    @Tag(CONNECT)
    void testCRBDeletionErrorsWhenRackAwarenessIsEnabled() {
        applyRoleBindingsWithoutCRBs();
        // 060-Deployment
        BundleResource.clusterOperator(NAMESPACE).done();

        String rackKey = "rack-key";

        LOGGER.info("Deploying Kafka: {}, which should not be deployed and error should be present in CR status message", CLUSTER_NAME);
        KafkaResource.kafkaWithoutWait(KafkaResource.defaultKafka(CLUSTER_NAME, 3, 3)
            .editOrNewSpec()
                .editOrNewKafka()
                    .withNewRack()
                        .withTopologyKey(rackKey)
                    .endRack()
                .endKafka()
            .endSpec()
            .build());

        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(CLUSTER_NAME, NAMESPACE, ".*Forbidden!.*");
        Condition kafkaStatusCondition = KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus().getConditions().get(0);
        assertTrue(kafkaStatusCondition.getMessage().contains("Configured service account doesn't have access."));
        assertThat(kafkaStatusCondition.getType(), is(NotReady.toString()));

        KafkaConnectResource.kafkaConnectWithoutWait(KafkaConnectResource.defaultKafkaConnect(CLUSTER_NAME, CLUSTER_NAME, 1)
            .editSpec()
                .withNewRack(rackKey)
            .endSpec()
            .build());

        KafkaConnectUtils.waitUntilKafkaConnectStatusConditionContainsMessage(CLUSTER_NAME, NAMESPACE, ".*Forbidden!.*");
        Condition kafkaConnectStatusCondition = KafkaConnectResource.kafkaConnectClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus().getConditions().get(0);
        assertTrue(kafkaConnectStatusCondition.getMessage().contains("Configured service account doesn't have access."));
        assertThat(kafkaConnectStatusCondition.getType(), is(NotReady.toString()));
    }

    private static void applyRoleBindingsWithoutCRBs() {
        // 020-RoleBinding
        KubernetesResource.roleBinding(TestUtils.USER_PATH + "/../install/cluster-operator/020-RoleBinding-strimzi-cluster-operator.yaml", NAMESPACE, NAMESPACE);
        // 031-RoleBinding
        KubernetesResource.roleBinding(TestUtils.USER_PATH + "/../install/cluster-operator/031-RoleBinding-strimzi-cluster-operator-entity-operator-delegation.yaml", NAMESPACE, NAMESPACE);
        // 032-RoleBinding
        KubernetesResource.roleBinding(TestUtils.USER_PATH + "/../install/cluster-operator/032-RoleBinding-strimzi-cluster-operator-topic-operator-delegation.yaml", NAMESPACE, NAMESPACE);
    }

    @BeforeAll
    void setup() {
        ResourceManager.setClassResources();
        prepareEnvForOperator(NAMESPACE);
    }
}
