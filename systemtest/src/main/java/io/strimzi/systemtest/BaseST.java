/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.test.k8s.KubeClusterResource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import resources.KubernetesResource;
import resources.ResourceManager;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BaseST {

    protected KubeClusterResource cluster = KubeClusterResource.getInstance();
    protected static final String CLUSTER_NAME = "my-cluster";

    protected String testClass;
    protected String testName;

    /**
     * Prepare environment for cluster operator which includes creation of namespaces, custom resources and operator
     * specific config files such as ServiceAccount, Roles and CRDs.
     * @param clientNamespace namespace which will be created and used as default by kube client
     * @param namespaces list of namespaces which will be created
     * @param resources list of path to yaml files with resources specifications
     */
    protected void prepareEnvForOperator(String clientNamespace, List<String> namespaces, String... resources) {
        cluster.createNamespaces(clientNamespace, namespaces);
        cluster.createCustomResources(resources);
        cluster.applyClusterOperatorInstallFiles();
    }

    /**
     * Prepare environment for cluster operator which includes creation of namespaces, custom resources and operator
     * specific config files such as ServiceAccount, Roles and CRDs.
     * @param clientNamespace namespace which will be created and used as default by kube client
     * @param resources list of path to yaml files with resources specifications
     */
    protected void prepareEnvForOperator(String clientNamespace, String... resources) {
        prepareEnvForOperator(clientNamespace, Collections.singletonList(clientNamespace), resources);
    }

    /**
     * Prepare environment for cluster operator which includes creation of namespaces, custom resources and operator
     * specific config files such as ServiceAccount, Roles and CRDs.
     * @param clientNamespace namespace which will be created and used as default by kube client
     */
    protected void prepareEnvForOperator(String clientNamespace) {
        prepareEnvForOperator(clientNamespace, Collections.singletonList(clientNamespace));
    }

    /**
     * Clear cluster from all created namespaces and configurations files for cluster operator.
     */
    protected void teardownEnvForOperator() {
        cluster.deleteClusterOperatorInstallFiles();
        cluster.deleteCustomResources();
        cluster.deleteNamespaces();
    }

    /**
     * Recreate namespace and CO after test failure
     * @param coNamespace namespace where CO will be deployed to
     * @param bindingsNamespaces array of namespaces where Bindings should be deployed to.
     */
    protected void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) {
        recreateTestEnv(coNamespace, bindingsNamespaces, Constants.CO_OPERATION_TIMEOUT_DEFAULT);
    }

    /**
     * Recreate namespace and CO after test failure
     * @param coNamespace namespace where CO will be deployed to
     * @param bindingsNamespaces array of namespaces where Bindings should be deployed to.
     * @param operationTimeout timeout for CO operations
     */
    protected void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces, long operationTimeout) {
        ResourceManager.deleteMethodResources();
        ResourceManager.deleteClassResources();

        KubeClusterResource.getInstance().deleteClusterOperatorInstallFiles();
        KubeClusterResource.getInstance().deleteNamespaces();

        KubeClusterResource.getInstance().createNamespaces(coNamespace, bindingsNamespaces);
        KubeClusterResource.getInstance().applyClusterOperatorInstallFiles();

        ResourceManager.setClassResources();

        applyRoleBindings(coNamespace, bindingsNamespaces);
        // 050-Deployment
        KubernetesResource.clusterOperator(coNamespace).done();
    }

    /**
     * Method for apply Strimzi cluster operator specific Role and ClusterRole bindings for specific namespaces.
     * @param namespace namespace where CO will be deployed to
     * @param bindingsNamespaces list of namespaces where Bindings should be deployed to
     */
    public static void applyRoleBindings(String namespace, List<String> bindingsNamespaces) {
        for (String bindingsNamespace : bindingsNamespaces) {
            // 020-RoleBinding
            KubernetesResource.roleBinding("../install/cluster-operator/020-RoleBinding-strimzi-cluster-operator.yaml", namespace, bindingsNamespace);
            // 021-ClusterRoleBinding
            KubernetesResource.clusterRoleBinding("../install/cluster-operator/021-ClusterRoleBinding-strimzi-cluster-operator.yaml", namespace, bindingsNamespace);
            // 030-ClusterRoleBinding
            KubernetesResource.clusterRoleBinding("../install/cluster-operator/030-ClusterRoleBinding-strimzi-cluster-operator-kafka-broker-delegation.yaml", namespace, bindingsNamespace);
            // 031-RoleBinding
            KubernetesResource.roleBinding("../install/cluster-operator/031-RoleBinding-strimzi-cluster-operator-entity-operator-delegation.yaml", namespace, bindingsNamespace);
            // 032-RoleBinding
            KubernetesResource.roleBinding("../install/cluster-operator/032-RoleBinding-strimzi-cluster-operator-topic-operator-delegation.yaml", namespace, bindingsNamespace);
        }
    }

    /**
     * Method for apply Strimzi cluster operator specific Role and ClusterRole bindings for specific namespaces.
     * @param namespace namespace where CO will be deployed to
     */
    public static void applyRoleBindings(String namespace) {
        applyRoleBindings(namespace, Collections.singletonList(namespace));
    }

    /**
     * Method for apply Strimzi cluster operator specific Role and ClusterRole bindings for specific namespaces.
     * @param namespace namespace where CO will be deployed to
     * @param bindingsNamespaces array of namespaces where Bindings should be deployed to
     */
    public static void applyRoleBindings(String namespace, String... bindingsNamespaces) {
        applyRoleBindings(namespace, Arrays.asList(bindingsNamespaces));
    }

    @BeforeEach
    void setTestName(TestInfo testInfo) {
        if (testInfo.getTestMethod().isPresent()) {
            testName = testInfo.getTestMethod().get().getName();
        }
    }

    @BeforeAll
    void createTestClassResources(TestInfo testInfo) {
        if (testInfo.getTestClass().isPresent()) {
            testClass = testInfo.getTestClass().get().getName();
        }
    }
}
