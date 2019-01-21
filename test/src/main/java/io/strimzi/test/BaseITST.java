/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.strimzi.test.k8s.KubeClient;
import io.strimzi.test.k8s.KubeClusterResource;
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

    public static KubeClusterResource cluster = new KubeClusterResource();
    protected static DefaultKubernetesClient client = new DefaultKubernetesClient();
    private static KubeClient<?> kubeClient = cluster.client();

    private static List<String> deploymentResources;
    private static List<String> deploymentNamespaces;
    private static String clientNamespace;
    private static Map<File, String> clusterOperatorMap;

    protected static String testClass;
    protected static String testName;

    private void applyClusterOperatorInstallFiles() {
        clusterOperatorMap = Arrays.stream(new File(CO_INSTALL_DIR).listFiles()).sorted().filter(file ->
                !file.getName().matches(".*(Binding|Deployment)-.*")
        ).collect(Collectors.toMap(file -> file, f -> TestUtils.getContent(f, TestUtils::toYamlString), (x, y) -> x, LinkedHashMap::new));
        for (Map.Entry<File, String> entry : clusterOperatorMap.entrySet()) {
            LOGGER.info("Creating possibly modified version of {}", entry.getKey());
            kubeClient.clientWithAdmin().applyContent(entry.getValue());
        }
    }

    private void deleteClusterOperatorInstallFiles() {
        for (Map.Entry<File, String> entry : clusterOperatorMap.entrySet()) {
            LOGGER.info("Deleting {}", entry.getKey());
            kubeClient.clientWithAdmin().deleteContent(entry.getValue());
        }
    }

    private void createNamespaces(String useNamespace, List<String> namespaces) {
        clientNamespace = useNamespace;
        deploymentNamespaces = namespaces;
        for (String namespace: namespaces) {
            LOGGER.info("Creating namespace: {}", namespace);
            kubeClient.createNamespace(namespace);
            kubeClient.waitForResourceCreation("Namespace", namespace);
            LOGGER.info("Namespace {} created", namespace);
        }
        LOGGER.info("Using namespace {}", useNamespace);
        kubeClient.namespace(useNamespace);
    }

    protected void createNamespaces(String useNamespace) {
        createNamespaces(useNamespace, Collections.singletonList(useNamespace));
    }

    protected void deleteNamespaces() {
        for (String namespace: deploymentNamespaces) {
            LOGGER.info("Deleting namespace: {}", namespace);
            kubeClient.deleteNamespace(namespace);
            kubeClient.waitForResourceDeletion("Namespace", namespace);
            LOGGER.info("Namespace {} deleted", namespace);
        }
        LOGGER.info("Using namespace {}", clientNamespace);
        kubeClient.namespace(clientNamespace);
    }

    protected void createCustomResources(List<String> resources) {
        LOGGER.info(System.getProperty("user.dir"));
        deploymentResources = resources;
        for (String resource : resources) {
            LOGGER.info("Creating resources {}", resource);
            LOGGER.info(kubeClient.namespace());
            kubeClient.clientWithAdmin().create(resource);
        }
    }

    protected void deleteCustomResources() {
        for (String resource : deploymentResources) {
            LOGGER.info("Deleting resources {}", resource);
            kubeClient.delete(resource);
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
    static void createTestClassResources(TestInfo testInfo) {
        testClass = testInfo.getTestClass().get().getSimpleName();
    }
}
