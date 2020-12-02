/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.fabric8.kubernetes.api.model.rbac.ClusterRole;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.operator.BundleResource;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag(REGRESSION)
class NamespaceRbacScopeOperatorST extends AbstractST {

    static final String NAMESPACE = "roles-only-cluster-test";
    static final String CLUSTER_NAME = "roles-only-cluster";

    private static final Logger LOGGER = LogManager.getLogger(NamespaceRbacScopeOperatorST.class);

    @Test
    void testNamespacedRbacScopeDeploysRoles() {
        assumeTrue(Environment.isNamespaceRbacScope());
        prepareEnvironment();

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 3)
                .editMetadata()
                    .addToLabels("app", "strimzi")
                .endMetadata()
                .done();

        // Wait for Kafka to be Ready to ensure all potentially erroneous ClusterRole applications have happened
        KafkaUtils.waitForKafkaReady(CLUSTER_NAME);

        // Assert that no ClusterRoles are present on the server that have app strimzi
        // Naturally returns false positives if another Strimzi operator has been installed
        List<ClusterRole> strimziClusterRoles = kubeClient().listClusterRoles().stream()
                .filter(cr -> {
                    Map<String, String> labels = cr.getMetadata().getLabels();
                    return "strimzi".equals(labels.get("app"));
                })
                .collect(Collectors.toList());
        assertThat(strimziClusterRoles, is(Collections.emptyList()));
    }

    private void prepareEnvironment() {
        prepareEnvForOperator(NAMESPACE);
        applyBindings(NAMESPACE);
        // 060-Deployment
        BundleResource.clusterOperator(NAMESPACE).done();
    }
}
