/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.specific;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.coordination.v1.Lease;
import io.fabric8.kubernetes.api.model.coordination.v1.LeaseBuilder;
import io.fabric8.kubernetes.api.model.rbac.ClusterRole;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.resources.CrdClients;
import io.strimzi.systemtest.resources.operator.ClusterOperatorConfiguration;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.resources.operator.YamlInstallation;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.kubernetes.ClusterRoleBindingTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.NamespaceUtils;
import io.strimzi.systemtest.utils.kubeUtils.rbac.RbacUtils;
import io.strimzi.test.ReadWriteUtils;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Tag;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static io.strimzi.systemtest.TestTags.REGRESSION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@Tag(REGRESSION)
public class RbacST extends AbstractST {

    @IsolatedTest
    void testClusterWideOperatorWithLimitedAccessToSpecificNamespaceViaRbacRole() {
        assumeFalse(Environment.isNamespaceRbacScope());

        // Used here only for (re)creation of Cluster Operator's Namespace
        SetupClusterOperator
            .getInstance()
            .withDefaultConfiguration()
            .createClusterOperatorNamespace();

        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final String namespaceWhereCreationOfCustomResourcesIsApproved = "example-1";
        final ClusterOperatorConfiguration clusterOperatorConfiguration = new ClusterOperatorConfiguration();

        // create namespace, where we will be able to deploy CustomResources
        NamespaceUtils.createNamespaceAndPrepare(namespaceWhereCreationOfCustomResourcesIsApproved);

        // --- a) defining Role and ClusterRoles
        final Role strimziClusterOperator020 = ReadWriteUtils.readObjectFromYamlFilepath(RbacUtils.switchClusterRolesToRolesIfNeeded(new File(TestConstants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/020-ClusterRole-strimzi-cluster-operator-role.yaml"), true), Role.class);

        // specify explicit namespace for Role (for ClusterRole we do not specify namespace because ClusterRole is a non-namespaced resource
        strimziClusterOperator020.getMetadata().setNamespace(namespaceWhereCreationOfCustomResourcesIsApproved);

        final ClusterRole strimziClusterOperator021 = ReadWriteUtils.readObjectFromYamlFilepath(new File(TestConstants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/021-ClusterRole-strimzi-cluster-operator-role.yaml"), ClusterRole.class);
        final ClusterRole strimziClusterOperator022 = ReadWriteUtils.readObjectFromYamlFilepath(new File(TestConstants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/022-ClusterRole-strimzi-cluster-operator-role.yaml"), ClusterRole.class);
        final ClusterRole strimziClusterOperator023 = ReadWriteUtils.readObjectFromYamlFilepath(new File(TestConstants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/023-ClusterRole-strimzi-cluster-operator-role.yaml"), ClusterRole.class);
        final ClusterRole strimziClusterOperator030 = ReadWriteUtils.readObjectFromYamlFilepath(new File(TestConstants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/030-ClusterRole-strimzi-kafka-broker.yaml"), ClusterRole.class);
        final ClusterRole strimziClusterOperator031 = ReadWriteUtils.readObjectFromYamlFilepath(new File(TestConstants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/031-ClusterRole-strimzi-entity-operator.yaml"), ClusterRole.class);
        final ClusterRole strimziClusterOperator033 = ReadWriteUtils.readObjectFromYamlFilepath(new File(TestConstants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/033-ClusterRole-strimzi-kafka-client.yaml"), ClusterRole.class);

        KubeResourceManager.get().createResourceWithWait(
            // Apply our ClusterRoles
            strimziClusterOperator021,
            strimziClusterOperator022,
            strimziClusterOperator023,
            strimziClusterOperator030,
            strimziClusterOperator031,
            strimziClusterOperator033,
            // Apply our Role
            strimziClusterOperator020
        );

        // ---- b) defining RoleBindings
        final RoleBinding strimziClusterOperator020Namespaced = ReadWriteUtils.readObjectFromYamlFilepath(RbacUtils.switchClusterRolesToRolesIfNeeded(new File(
            TestConstants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/020-RoleBinding-strimzi-cluster-operator.yaml"), true), RoleBinding.class);
        final RoleBinding strimziClusterOperator022LeaderElection = ReadWriteUtils.readObjectFromYamlFilepath(
            new File(TestConstants.PATH_TO_LEASE_ROLE_BINDING), RoleBinding.class);

        // specify explicit namespace for RoleBindings
        strimziClusterOperator020Namespaced.getMetadata().setNamespace(namespaceWhereCreationOfCustomResourcesIsApproved);
        strimziClusterOperator022LeaderElection.getMetadata().setNamespace(clusterOperatorConfiguration.getNamespaceName());

        // reference Cluster Operator service account in RoleBindings
        strimziClusterOperator020Namespaced.getSubjects().stream().findFirst().get().setNamespace(clusterOperatorConfiguration.getNamespaceName());
        strimziClusterOperator022LeaderElection.getSubjects().stream().findFirst().get().setNamespace(clusterOperatorConfiguration.getNamespaceName());

        KubeResourceManager.get().createResourceWithWait(
            // Apply our RoleBindings
            strimziClusterOperator020Namespaced,
            strimziClusterOperator022LeaderElection
        );

        // ---- c) defining ClusterRoleBindings
        KubeResourceManager.get().createResourceWithWait(
            // Apply our ClusterRoleBindings
            ClusterRoleBindingTemplates.getClusterOperatorWatchedCrb(clusterOperatorConfiguration.getNamespaceName(), clusterOperatorConfiguration.getOperatorDeploymentName()),
            ClusterRoleBindingTemplates.getClusterOperatorEntityOperatorCrb(clusterOperatorConfiguration.getNamespaceName(), clusterOperatorConfiguration.getOperatorDeploymentName())
        );

        // Apply all CRDs, SA etc.
        applyOtherInstallationFilesThatAreNotRbac(clusterOperatorConfiguration);

        // Deploy CO deployment
        YamlInstallation.deployClusterOperator(clusterOperatorConfiguration);

        KubeResourceManager.get().createResourceWithoutWait(
            KafkaNodePoolTemplates.brokerPool(Environment.TEST_SUITE_NAMESPACE, testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(Environment.TEST_SUITE_NAMESPACE, testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        KubeResourceManager.get().createResourceWithoutWait(KafkaTemplates.kafka(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName(), 3).build());

        // implicit verification that a user is able to deploy Kafka cluster in namespace <example-1>, where we are allowed
        // to create CustomResources because of `*-namespaced Role`
        KubeResourceManager.get().createResourceWithoutWait(
            KafkaNodePoolTemplates.brokerPool(namespaceWhereCreationOfCustomResourcesIsApproved, testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(namespaceWhereCreationOfCustomResourcesIsApproved, testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        // this should work
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(namespaceWhereCreationOfCustomResourcesIsApproved, testStorage.getClusterName(), 3).build());

        // verify that in `infra-namespace` we are not able to deploy Kafka cluster
        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName(),
            ".*code=403.*");

        final Condition condition = CrdClients.kafkaClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(testStorage.getClusterName()).get().getStatus().getConditions().stream().filter(c -> "NotReady".equals(c.getType())).findFirst().orElseThrow();

        assertThat(condition.getReason(), CoreMatchers.is("KubernetesClientException"));
        assertThat(condition.getStatus(), CoreMatchers.is("True"));
    }

    /**
     * Similar method that we have in {@link YamlInstallation#applyInstallationFiles()} with different resource types that it handles.
     * This is here because it's used only in this test and having it directly in the {@link YamlInstallation#applyInstallationFiles()}
     * would be too hacky.
     * It applies all resources from the {@link YamlInstallation#CO_INSTALL_DIR} except Roles, ClusterRoles, ClusterRoleBindings, RoleBindings and Deployment.
     * These resources are applied in the tests directly.
     *
     * @param clusterOperatorConfiguration  desired ClusterOperator configuration
     */
    private void applyOtherInstallationFilesThatAreNotRbac(ClusterOperatorConfiguration clusterOperatorConfiguration) {
        List<File> operatorFiles = Arrays.stream(new File(YamlInstallation.CO_INSTALL_DIR).listFiles()).sorted()
            .filter(File::isFile)
            .filter(file ->
                !file.getName().matches(".*(Deployment|Role)-.*"))
            .toList();

        for (File operatorFile : operatorFiles) {
            final String resourceType = operatorFile.getName().split("-")[1];

            switch (resourceType) {
                case TestConstants.SERVICE_ACCOUNT:
                    ServiceAccount serviceAccount = ReadWriteUtils.readObjectFromYamlFilepath(operatorFile, ServiceAccount.class);
                    KubeResourceManager.get().createResourceWithWait(new ServiceAccountBuilder(serviceAccount)
                        .editMetadata()
                            .withNamespace(clusterOperatorConfiguration.getNamespaceName())
                        .endMetadata()
                        .build());
                    break;
                case TestConstants.CONFIG_MAP:
                    ConfigMap configMap = ReadWriteUtils.readObjectFromYamlFilepath(operatorFile, ConfigMap.class);
                    KubeResourceManager.get().createResourceWithWait(new ConfigMapBuilder(configMap)
                        .editMetadata()
                            .withNamespace(clusterOperatorConfiguration.getNamespaceName())
                            .withName(clusterOperatorConfiguration.getOperatorDeploymentName())
                        .endMetadata()
                        .build());
                    break;
                case TestConstants.LEASE:
                    // Loads the resource through Fabric8 Kubernetes Client => that way we do not need to add a direct
                    // dependency on Jackson Datatype JSR310 to decode the Lease resource
                    Lease lease = KubeResourceManager.get().kubeClient().getClient().leases().load(operatorFile).item();
                    KubeResourceManager.get().createResourceWithWait(new LeaseBuilder(lease)
                        .editMetadata()
                            .withNamespace(clusterOperatorConfiguration.getNamespaceName())
                            .withName(clusterOperatorConfiguration.getOperatorDeploymentName())
                        .endMetadata()
                        .build());
                    break;
                case TestConstants.CUSTOM_RESOURCE_DEFINITION_SHORT:
                    CustomResourceDefinition customResourceDefinition = ReadWriteUtils.readObjectFromYamlFilepath(operatorFile, CustomResourceDefinition.class);
                    KubeResourceManager.get().createResourceWithWait(customResourceDefinition);
                    break;
                default:
                    break;
            }
        }
    }
}
