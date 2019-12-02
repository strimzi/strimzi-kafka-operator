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
