/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.strimzi.test.k8s.KubeClient;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.timemeasuring.Operation;
import io.strimzi.test.timemeasuring.TimeMeasuringSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BaseITST {

    private static final String CO_INSTALL_DIR = "../install/cluster-operator";

    private static final Logger LOGGER = LogManager.getLogger(BaseITST.class);
    protected static final String CLUSTER_NAME = "my-cluster";

    public static final KubeClusterResource CLUSTER = new KubeClusterResource();
    protected static final DefaultKubernetesClient CLIENT = new DefaultKubernetesClient();
    public static final KubeClient<?> KUBE_CLIENT = CLUSTER.client();

    private List<String> deploymentResources = Collections.EMPTY_LIST;
    private List<String> deploymentNamespaces = Collections.EMPTY_LIST;
    private Map<File, String> clusterOperatorMap = Collections.EMPTY_MAP;

    protected String testClass;
    protected String testName;

    private void applyClusterOperatorInstallFiles() {
        TimeMeasuringSystem.setTestName(testClass, testClass);
        TimeMeasuringSystem.startOperation(Operation.CO_CREATION);
        clusterOperatorMap = Arrays.stream(new File(CO_INSTALL_DIR).listFiles()).sorted().filter(file ->
                !file.getName().matches(".*(Binding|Deployment)-.*")
        ).collect(Collectors.toMap(file -> file, f -> TestUtils.getContent(f, TestUtils::toYamlString), (x, y) -> x, LinkedHashMap::new));
        for (Map.Entry<File, String> entry : clusterOperatorMap.entrySet()) {
            LOGGER.info("Applying configuration file: {}", entry.getKey());
            KUBE_CLIENT.clientWithAdmin().applyContent(entry.getValue());
        }
        TimeMeasuringSystem.stopOperation(Operation.CO_CREATION);
    }

    private void deleteClusterOperatorInstallFiles() {
        TimeMeasuringSystem.setTestName(testClass, testClass);
        TimeMeasuringSystem.startOperation(Operation.CO_DELETION);
        for (Map.Entry<File, String> entry : clusterOperatorMap.entrySet()) {
            LOGGER.info("Removing configuration file: {}", entry.getKey());
            KUBE_CLIENT.clientWithAdmin().deleteContent(entry.getValue());
        }
        TimeMeasuringSystem.stopOperation(Operation.CO_DELETION);
    }

    private void createNamespaces(String useNamespace, List<String> namespaces) {
        deploymentNamespaces = namespaces;
        for (String namespace: namespaces) {
            LOGGER.info("Creating namespace: {}", namespace);
            KUBE_CLIENT.createNamespace(namespace);
            KUBE_CLIENT.waitForResourceCreation("Namespace", namespace);
            LOGGER.info("Namespace {} created", namespace);
        }
        LOGGER.info("Using namespace {}", useNamespace);
        KUBE_CLIENT.namespace(useNamespace);
    }

    protected void createNamespaces(String useNamespace) {
        createNamespaces(useNamespace, Collections.singletonList(useNamespace));
    }

    protected void deleteNamespaces() {
        for (String namespace: deploymentNamespaces) {
            LOGGER.info("Deleting namespace: {}", namespace);
            KUBE_CLIENT.deleteNamespace(namespace);
            KUBE_CLIENT.waitForResourceDeletion("Namespace", namespace);
            LOGGER.info("Namespace {} deleted", namespace);
        }
        LOGGER.info("Using namespace {}", CLUSTER.defaultNamespace());
        KUBE_CLIENT.namespace(CLUSTER.defaultNamespace());
    }

    protected void createCustomResources(List<String> resources) {
        deploymentResources = resources;
        for (String resource : resources) {
            LOGGER.info("Creating resources {}", resource);
            KUBE_CLIENT.clientWithAdmin().create(resource);
        }
    }

    protected void deleteCustomResources() {
        for (String resource : deploymentResources) {
            LOGGER.info("Deleting resources {}", resource);
            KUBE_CLIENT.delete(resource);
        }
    }

    protected void prepareEnvForOperator(String clientNamespace, List<String> namespaces, List<String> resources) {
        createNamespaces(clientNamespace, namespaces);
        createCustomResources(resources);
        applyClusterOperatorInstallFiles();
    }

    protected void prepareEnvForOperator(String clientNamespace, List<String> resources) {
        prepareEnvForOperator(clientNamespace, Collections.singletonList(clientNamespace), resources);
    }

    protected void prepareEnvForOperator(String clientNamespace) {
        prepareEnvForOperator(clientNamespace, Collections.singletonList(clientNamespace), Collections.emptyList());
    }

    protected void teardownEnvForOperator() {
        deleteClusterOperatorInstallFiles();
        deleteCustomResources();
        deleteNamespaces();
    }

    @BeforeEach
    void setTestName(TestInfo testInfo) {
        testName = testInfo.getTestMethod().get().getName();
    }

    @BeforeAll
    void createTestClassResources(TestInfo testInfo) {
        testClass = testInfo.getTestClass().get().getSimpleName();
    }
}
