/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.specific;

import io.fabric8.kubernetes.api.model.rbac.ClusterRole;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.resources.NamespaceManager;
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.kubernetes.ClusterRoleBindingTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.logs.CollectorElement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static io.strimzi.systemtest.TestConstants.REGRESSION;
import static io.strimzi.systemtest.TestConstants.SPECIFIC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@Tag(SPECIFIC)
@Tag(REGRESSION)
public class SpecificST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(SpecificST.class);

    @IsolatedTest
    void testClusterWideOperatorWithLimitedAccessToSpecificNamespaceViaRbacRole() {
        assumeFalse(Environment.isNamespaceRbacScope());

        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final String namespaceWhereCreationOfCustomResourcesIsApproved = "example-1";

        // --- a) defining Role and ClusterRoles
        final Role strimziClusterOperator020 = TestUtils.configFromYaml(SetupClusterOperator.getInstance().switchClusterRolesToRolesIfNeeded(new File(TestConstants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/020-ClusterRole-strimzi-cluster-operator-role.yaml"), true), Role.class);

        // specify explicit namespace for Role (for ClusterRole we do not specify namespace because ClusterRole is a non-namespaced resource
        strimziClusterOperator020.getMetadata().setNamespace(namespaceWhereCreationOfCustomResourcesIsApproved);

        final ClusterRole strimziClusterOperator021 = TestUtils.configFromYaml(new File(TestConstants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/021-ClusterRole-strimzi-cluster-operator-role.yaml"), ClusterRole.class);
        final ClusterRole strimziClusterOperator022 = TestUtils.configFromYaml(SetupClusterOperator.getInstance().changeLeaseNameInResourceIfNeeded(new File(
            TestConstants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/022-ClusterRole-strimzi-cluster-operator-role.yaml").getAbsolutePath()), ClusterRole.class);
        final ClusterRole strimziClusterOperator023 = TestUtils.configFromYaml(new File(TestConstants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/023-ClusterRole-strimzi-cluster-operator-role.yaml"), ClusterRole.class);
        final ClusterRole strimziClusterOperator030 = TestUtils.configFromYaml(new File(TestConstants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/030-ClusterRole-strimzi-kafka-broker.yaml"), ClusterRole.class);
        final ClusterRole strimziClusterOperator031 = TestUtils.configFromYaml(new File(TestConstants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/031-ClusterRole-strimzi-entity-operator.yaml"), ClusterRole.class);
        final ClusterRole strimziClusterOperator033 = TestUtils.configFromYaml(new File(TestConstants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/033-ClusterRole-strimzi-kafka-client.yaml"), ClusterRole.class);

        final List<Role> roles = Arrays.asList(strimziClusterOperator020);
        final List<ClusterRole> clusterRoles = Arrays.asList(strimziClusterOperator021, strimziClusterOperator022,
                strimziClusterOperator023, strimziClusterOperator030, strimziClusterOperator031, strimziClusterOperator033);

        // ---- b) defining RoleBindings
        final RoleBinding strimziClusterOperator020Namespaced = TestUtils.configFromYaml(SetupClusterOperator.getInstance().switchClusterRolesToRolesIfNeeded(new File(
            TestConstants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/020-RoleBinding-strimzi-cluster-operator.yaml"), true), RoleBinding.class);
        final RoleBinding strimziClusterOperator022LeaderElection = TestUtils.configFromYaml(SetupClusterOperator.getInstance().changeLeaseNameInResourceIfNeeded(new File(
            TestConstants.PATH_TO_LEASE_ROLE_BINDING).getAbsolutePath()), RoleBinding.class);

        // specify explicit namespace for RoleBindings
        strimziClusterOperator020Namespaced.getMetadata().setNamespace(namespaceWhereCreationOfCustomResourcesIsApproved);
        strimziClusterOperator022LeaderElection.getMetadata().setNamespace(clusterOperator.getDeploymentNamespace());

        // reference Cluster Operator service account in RoleBindings
        strimziClusterOperator020Namespaced.getSubjects().stream().findFirst().get().setNamespace(clusterOperator.getDeploymentNamespace());
        strimziClusterOperator022LeaderElection.getSubjects().stream().findFirst().get().setNamespace(clusterOperator.getDeploymentNamespace());

        final List<RoleBinding> roleBindings = Arrays.asList(
                strimziClusterOperator020Namespaced,
                strimziClusterOperator022LeaderElection
        );

        // ---- c) defining ClusterRoleBindings
        final List<ClusterRoleBinding> clusterRoleBindings = Arrays.asList(
            ClusterRoleBindingTemplates.getClusterOperatorWatchedCrb(clusterOperator.getClusterOperatorName(), clusterOperator.getDeploymentNamespace()),
            ClusterRoleBindingTemplates.getClusterOperatorEntityOperatorCrb(clusterOperator.getClusterOperatorName(), clusterOperator.getDeploymentNamespace())
        );

        clusterOperator.unInstall();

        // create namespace, where we will be able to deploy Custom Resources
        NamespaceManager.getInstance().createNamespaceAndPrepare(namespaceWhereCreationOfCustomResourcesIsApproved,
            CollectorElement.createCollectorElement(ResourceManager.getTestContext().getRequiredTestClass().getName(), ResourceManager.getTestContext().getRequiredTestMethod().getName()));

        clusterOperator = clusterOperator.defaultInstallation()
            // use our pre-defined Roles
            .withRoles(roles)
            // use our pre-defined RoleBindings
            .withRoleBindings(roleBindings)
            // use our pre-defined ClusterRoles
            .withClusterRoles(clusterRoles)
            // use our pre-defined ClusterRoleBindings
            .withClusterRoleBindings(clusterRoleBindings)
            .createInstallation()
            .runBundleInstallation();

        resourceManager.createResourceWithoutWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(Environment.TEST_SUITE_NAMESPACE, testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(Environment.TEST_SUITE_NAMESPACE, testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithoutWait(KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3)
                .editMetadata()
                // this should not work
                    .withNamespace(Environment.TEST_SUITE_NAMESPACE)
                .endMetadata()
                .build());

        // implicit verification that a user is able to deploy Kafka cluster in namespace <example-1>, where we are allowed
        // to create Custom Resources because of `*-namespaced Role`
        resourceManager.createResourceWithoutWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(namespaceWhereCreationOfCustomResourcesIsApproved, testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(namespaceWhereCreationOfCustomResourcesIsApproved, testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3)
                .editMetadata()
                // this should work
                    .withNamespace(namespaceWhereCreationOfCustomResourcesIsApproved)
                .endMetadata()
                .build());

        // verify that in `infra-namespace` we are not able to deploy Kafka cluster
        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(testStorage.getClusterName(), Environment.TEST_SUITE_NAMESPACE,
                ".*code=403.*");

        final Condition condition = KafkaResource.kafkaClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(testStorage.getClusterName()).get().getStatus().getConditions().stream().filter(c -> "NotReady".equals(c.getType())).findFirst().orElseThrow();

        assertThat(condition.getReason(), CoreMatchers.is("KubernetesClientException"));
        assertThat(condition.getStatus(), CoreMatchers.is("True"));
    }

    @BeforeAll
    void setUp() {
        this.clusterOperator = this.clusterOperator
            .defaultInstallation()
            .createInstallation();
    }
}
