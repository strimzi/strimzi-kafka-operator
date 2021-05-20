/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

import com.fasterxml.jackson.databind.JsonNode;
import io.strimzi.test.k8s.cluster.KubeCluster;
import io.strimzi.test.k8s.cluster.Minishift;
import io.strimzi.test.k8s.cluster.OpenShift;
import io.strimzi.test.k8s.cmdClient.KubeCmdClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A Junit resource which discovers the running cluster and provides an appropriate KubeClient for it,
 * for use with {@code @BeforeAll} (or {@code BeforeEach}.
 * For example:
 * <pre><code>
 *     public static KubeClusterResource testCluster = new KubeClusterResources();
 *
 *     &#64;BeforeEach
 *     void before() {
 *         testCluster.before();
 *     }
 * </code></pre>
 */
public class KubeClusterResource {

    private static final Logger LOGGER = LogManager.getLogger(KubeClusterResource.class);

    private KubeCluster kubeCluster;
    private KubeCmdClient cmdClient;
    private KubeClient client;
    private HelmClient helmClient;
    private static KubeClusterResource kubeClusterResource;

    private String namespace;
    private String testNamespace;
    // {test-suite-name} -> {{namespace-1}, {namespace-2},...,}
    protected static Map<String, Set<String>> mapWithSuiteNamespaces = new HashMap<>();

    protected List<String> bindingsNamespaces = new ArrayList<>();
    private List<String> deploymentNamespaces = new ArrayList<>();
    private List<String> deploymentResources = new ArrayList<>();

    public static synchronized KubeClusterResource getInstance() {
        if (kubeClusterResource == null) {
            kubeClusterResource = new KubeClusterResource();
            initNamespaces();
            LOGGER.info("Cluster default namespace is {}", kubeClusterResource.getNamespace());
            LOGGER.info("Cluster command line client default namespace is {}", kubeClusterResource.getTestNamespace());
        }
        return kubeClusterResource;
    }

    private KubeClusterResource() { }

    private static void initNamespaces() {
        kubeClusterResource.setDefaultNamespace(cmdKubeClient().defaultNamespace());
        kubeClusterResource.setTestNamespace(cmdKubeClient().defaultNamespace());
    }

    public void setTestNamespace(String testNamespace) {
        this.testNamespace = testNamespace;
    }

    public void setDefaultNamespace(String namespace) {
        this.namespace = namespace;
    }

    /**
     * Sets the namespace value for Kubernetes clients
     * @param futureNamespace Namespace which should be used in Kubernetes clients
     * @return Previous namespace which was used in Kubernetes clients
     */
    public String setNamespace(String futureNamespace) {
        String previousNamespace = namespace;
        LOGGER.info("Changing to {} namespace", futureNamespace);
        namespace = futureNamespace;
        return previousNamespace;
    }

    public List<String> getBindingsNamespaces() {
        return bindingsNamespaces;
    }

    /**
     * Gets namespace which is used in Kubernetes clients at the moment
     * @return Used namespace
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * Provides appropriate CMD client for running cluster
     * @return CMD client
     */
    public static KubeCmdClient<?> cmdKubeClient() {
        return kubeClusterResource.cmdClient().namespace(kubeClusterResource.getNamespace());
    }

    /**
     * Provides appropriate CMD client with expected namespace for running cluster
     * @param inNamespace Namespace will be used as a current namespace for client
     * @return CMD client with expected namespace in configuration
     */
    public static KubeCmdClient<?> cmdKubeClient(String inNamespace) {
        return kubeClusterResource.cmdClient().namespace(inNamespace);
    }

    /**
     * Provides appropriate Kubernetes client for running cluster
     * @return Kubernetes client
     */
    public static KubeClient kubeClient() {
        return kubeClusterResource.client().namespace(kubeClusterResource.getNamespace());
    }

    /**
     * Provides approriate Helm client for running Helm operations in specific namespace
     * @return Helm client
     */
    public static HelmClient helmClusterClient() {
        return kubeClusterResource.helmClient().namespace(kubeClusterResource.getNamespace());
    }

    /**
     * Provides appropriate Kubernetes client with expected namespace for running cluster
     * @param inNamespace Namespace will be used as a current namespace for client
     * @return Kubernetes client with expected namespace in configuration
     */
    public static KubeClient kubeClient(String inNamespace) {
        return kubeClusterResource.client().namespace(inNamespace);
    }

    /**
     * Create namespaces for test resources.
     * @param useNamespace namespace which will be used as default by kubernetes client
     * @param namespaces list of namespaces which will be created
     */
    public void createNamespaces(ExtensionContext extensionContext, String useNamespace, List<String> namespaces) {
        bindingsNamespaces = namespaces;
        for (String namespace: namespaces) {

            if (kubeClient().getNamespace(namespace) != null && System.getenv("SKIP_TEARDOWN") == null) {
                LOGGER.warn("Namespace {} is already created, going to delete it", namespace);
                kubeClient().deleteNamespace(namespace);
                cmdKubeClient().waitForResourceDeletion("Namespace", namespace);
            }

            LOGGER.info("Creating Namespace {}", namespace);
            deploymentNamespaces.add(namespace);
            kubeClient().createNamespace(namespace);
            cmdKubeClient().waitForResourceCreation("Namespace", namespace);
            if (extensionContext != null) addNamespaceToSet(extensionContext, namespace);
        }
        testNamespace = useNamespace;
        LOGGER.info("Using Namespace {}", useNamespace);
        kubeClusterResource.setNamespace(useNamespace);
    }

    /**
     * Create namespace for test resources. Deletion is up to caller and can be managed
     * by calling {@link #deleteNamespaces()}
     * @param useNamespace namespace which will be created and used as default by kubernetes client
     */
    public void createNamespace(ExtensionContext extensionContext, String useNamespace) {
        createNamespaces(extensionContext, useNamespace, Collections.singletonList(useNamespace));
    }

    public void createNamespace(String useNamespace) {
        createNamespaces(null, useNamespace, Collections.singletonList(useNamespace));
    }

    /**
     * Delete all created namespaces. Namespaces are deleted in the reverse order than they were created.
     */
    public void deleteNamespaces(ExtensionContext extensionContext) {
        Collections.reverse(deploymentNamespaces);
        for (String namespace: deploymentNamespaces) {
            LOGGER.info("Deleting Namespace {}", namespace);
            kubeClient().deleteNamespace(namespace);
            cmdKubeClient().waitForResourceDeletion("Namespace", namespace);
            if (extensionContext != null) deleteNamespaceFromSet(extensionContext, namespace);
        }
        deploymentNamespaces.clear();
        bindingsNamespaces = null;
        LOGGER.info("Using Namespace {}", testNamespace);
        setNamespace(testNamespace);

        if (extensionContext != null) deleteNamespaceFromSet(extensionContext, testNamespace);
    }

    public void deleteNamespaces() {
        deleteNamespaces(null);
    }

    public void deleteNamespace(ExtensionContext extensionContext, String namespaceName) {
        kubeClient().deleteNamespace(namespaceName);
        cmdKubeClient().waitForResourceDeletion("Namespace", namespaceName);
        if (extensionContext != null) deleteNamespaceFromSet(extensionContext, testNamespace);
    }

    /**
     * Replaces custom resources for CO such as templates. Deletion is up to caller and can be managed
     * by calling {@link #deleteCustomResources()}
     *
     * @param resources array of paths to yaml files with resources specifications
     */
    public void replaceCustomResources(String... resources) {
        for (String resource : resources) {
            LOGGER.info("Replacing resources {} in Namespace {}", resource, getNamespace());
            deploymentResources.add(resource);
            cmdKubeClient().namespace(getNamespace()).replace(resource);
        }
    }

    /**
     * Creates custom resources for CO such as templates. Deletion is up to caller and can be managed
     * by calling {@link #deleteCustomResources()}
     * @param extensionContext extension context of specific test case because of namespace name
     * @param resources array of paths to yaml files with resources specifications
     */
    public void createCustomResources(ExtensionContext extensionContext, String... resources) {
        final String namespaceName = !extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get("NAMESPACE_NAME").toString().isEmpty() ?
            extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get("NAMESPACE_NAME").toString() :
            getNamespace();

        for (String resource : resources) {
            LOGGER.info("Creating resources {} in Namespace {}", resource, namespaceName);
            deploymentResources.add(resource);
            cmdKubeClient(namespaceName).create(resource);
        }
    }

    /**
     * Creates custom resources for CO such as templates. Deletion is up to caller and can be managed
     * by calling {@link #deleteCustomResources()}
     * @param resources array of paths to yaml files with resources specifications
     */
    public void createCustomResources(String... resources) {
        for (String resource : resources) {
            LOGGER.info("Creating resources {} in Namespace {}", resource, getNamespace());
            deploymentResources.add(resource);
            cmdKubeClient(getNamespace()).create(resource);
        }
    }

    /**
     * Waits for a CRD resource to be ready
     *
     * @param name  Name of the CRD to wait for
     */
    public void waitForCustomResourceDefinition(String name) {
        cmdKubeClient().waitFor("crd", name, crd -> {
            JsonNode json = (JsonNode) crd;
            if (json != null
                    && json.hasNonNull("status")
                    && json.get("status").hasNonNull("conditions")
                    && json.get("status").get("conditions").isArray()) {
                for (JsonNode condition : json.get("status").get("conditions")) {
                    if ("Established".equals(condition.get("type").asText())
                            && "True".equals(condition.get("status").asText()))   {
                        return true;
                    }
                }

                return false;
            }

            return false;
        });
    }

    /**
     * Delete custom resources such as templates. Resources are deleted in the reverse order than they were created.
     */
    public void deleteCustomResources() {
        Collections.reverse(deploymentResources);
        for (String resource : deploymentResources) {
            LOGGER.info("Deleting resources {}", resource);
            cmdKubeClient().delete(resource);
        }
        deploymentResources.clear();
    }

    /**
     * Delete custom resources such as templates. Resources are deleted in the reverse order than they were created.
     */
    public void deleteCustomResources(String... resources) {
        for (String resource : resources) {
            LOGGER.info("Deleting resources {}", resource);
            cmdKubeClient().delete(resource);
            deploymentResources.remove(resource);
        }
    }

    public void deleteCustomResources(ExtensionContext extensionContext, String... resources) {
        final String namespaceName = !extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get("NAMESPACE_NAME").toString().isEmpty() ?
            extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get("NAMESPACE_NAME").toString() :
            getNamespace();

        for (String resource : resources) {
            LOGGER.info("Deleting resources {}", resource);
            cmdKubeClient(namespaceName).delete(resource);
            deploymentResources.remove(resource);
        }
    }

    /** Gets the namespace in use */
    public String defaultNamespace() {
        return cmdClient().defaultNamespace();
    }

    public KubeCmdClient cmdClient() {
        if (cmdClient == null) {
            cmdClient = cluster().defaultCmdClient();
        }
        return cmdClient;
    }

    public KubeClient client() {
        if (client == null) {
            this.client = cluster().defaultClient();
        }
        return client;
    }

    public HelmClient helmClient() {
        if (helmClient == null) {
            this.helmClient = HelmClient.findClient(cmdClient());
        }
        return helmClient;
    }

    public KubeCluster cluster() {
        if (kubeCluster == null) {
            kubeCluster = KubeCluster.bootstrap();
        }
        return kubeCluster;
    }

    public String getTestNamespace() {
        return testNamespace;
    }

    public String getDefaultOlmNamespace() {
        return cluster().defaultOlmNamespace();
    }

    public boolean isNotKubernetes() {
        return kubeClusterResource.cluster() instanceof Minishift || kubeClusterResource.cluster() instanceof OpenShift;
    }

    /** Returns list of currently deployed resources */
    public List<String> getListOfDeployedResources() {
        return deploymentResources;
    }

    private synchronized void addNamespaceToSet(ExtensionContext extensionContext, String namespaceName) {
        String testClass = null;

        if (extensionContext.getTestClass().isPresent()) testClass = extensionContext.getRequiredTestClass().getName();

        if (mapWithSuiteNamespaces.containsKey(testClass)) {
            Set<String> testSuiteNamespaces = mapWithSuiteNamespaces.get(testClass);
            testSuiteNamespaces.add(namespaceName);
            mapWithSuiteNamespaces.put(testClass, testSuiteNamespaces);
        } else {
            // test-suite is new
            mapWithSuiteNamespaces.put(testClass, new HashSet<>(Set.of(namespaceName)));
        }

        LOGGER.debug("SUITE_NAMESPACE_MAP: \n{}", mapWithSuiteNamespaces);
    }

    private synchronized void deleteNamespaceFromSet(ExtensionContext extensionContext, String namespaceName) {
        String testClass = null;

        if (extensionContext.getTestClass().isPresent()) testClass = extensionContext.getRequiredTestClass().getName();

        // dynamically removing from the map
        Set<String> testSuiteNamespaces = mapWithSuiteNamespaces.get(testClass);
        testSuiteNamespaces.remove(namespaceName);
        mapWithSuiteNamespaces.put(testClass, testSuiteNamespaces);
        LOGGER.debug("SUITE_NAMESPACE_MAP after deletion: \n{}", mapWithSuiteNamespaces);
    }

    public static Map<String, Set<String>> getMapWithSuiteNamespaces() {
        return mapWithSuiteNamespaces;
    }
}
