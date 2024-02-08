/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.logs;

import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.enums.ClusterOperatorInstallType;
import io.strimzi.systemtest.resources.NamespaceManager;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.k8s.KubeClient;
import io.strimzi.test.logs.CollectorElement;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static io.strimzi.test.TestUtils.writeFile;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;

/**
 * LogCollector collects logs from all resources **if test case or preparation phase fails**. All can be found
 * inside ./systemtest/target/logs directory where the structure of the logs are as follows:
 *
 * ./logs
 *      /time/
 *          /test-suite_time/
 *              /test-case/
 *                  namespace-1/
 *                      deployment.log
 *                      configmap.log
 *                      ...
 *                  namespace-2/
 *                      deployment.log
 *                      configmap.log
 *                      describe-pod-kafka-cluster-entity-operator-56t123sd-31221-container...log
 *                      logs-pod-kafka-cluster-entity-operator-56t123sd-31221.log
 *                      ...
*                 ...
 *              cluster-operator.log    // shared cluster operator logs for all tests inside one test suite
 *          /another-test-suite_time/
 *      ...
 */
public class LogCollector {
    private static final Logger LOGGER = LogManager.getLogger(LogCollector.class);

    private static final String CURRENT_DATE;

    static {
        // Get current date to create a unique folder
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss");
        dateTimeFormatter = dateTimeFormatter.withZone(ZoneId.of("GMT"));
        CURRENT_DATE = dateTimeFormatter.format(LocalDateTime.now());
    }

    private final KubeClient kubeClient;
    private final Path testSuitePath;
    private final Path testCasePath;
    private final String clusterOperatorNamespace;
    private Path namespacePath;
    private final CollectorElement collectorElement;
    private final ExtensionContext extensionContext;

    public LogCollector(ExtensionContext extensionContext, CollectorElement collectorElement, KubeClient kubeClient, String logDir) throws IOException {
        this.extensionContext = extensionContext;
        this.collectorElement = collectorElement;
        this.kubeClient = kubeClient;

        Path logDirPath = Paths.get(logDir).resolve(CURRENT_DATE);
        final Path logSuiteDir = logDirPath.resolve(collectorElement.getTestClassName());

        this.testSuitePath = logSuiteDir;

        // contract only one Cluster Operator deployment inside all namespaces
        Pod clusterOperatorPod = kubeClient.getClient().pods().inAnyNamespace().list().getItems().stream()
                .filter(pod -> pod.getMetadata().getName().contains(TestConstants.STRIMZI_DEPLOYMENT_NAME))
                // contract only one Cluster Operator deployment inside all namespaces
                .findFirst()
                .orElseGet(Pod::new);

        this.clusterOperatorNamespace = clusterOperatorPod.getMetadata() != null ?
                clusterOperatorPod.getMetadata().getNamespace() :
                kubeClient.getNamespace();

        this.testCasePath = logSuiteDir.resolve(collectorElement.getTestMethodName());

        boolean logDirExist = logDirPath.toFile().exists() || logDirPath.toFile().mkdirs();
        boolean logTestSuiteDirExist = this.testSuitePath.toFile().exists() || this.testSuitePath.toFile().mkdirs();
        boolean logTestCaseDirExist = this.testCasePath.toFile().exists() || this.testCasePath.toFile().mkdirs();

        if (!logDirExist) {
            throw new IOException("Unable to create path");
        }
        if (!logTestSuiteDirExist) {
            throw new IOException("Unable to create path");
        }
        if (!logTestCaseDirExist) {
            throw new IOException("Unable to create path");
        }
    }

    /**
     * Core method, which collects logs from {@link #collectEvents(String)}, {@link #collectConfigMaps(String)},
     * {@link #collectLogsFromPods(String)}, {@link #collectAllResourcesFromNamespace(String)},
     * {@link #collectStrimzi(String)} and lastly {@link #collectClusterInfo()}.
     *
     * If anything fails in @BeforeAll or @AfterAll suite we gather these logs from @cde{this.testSuite}/@code{namespace}.
     * Otherwise, we collect it in code@{this.testCase}/code{namespace}.
     *
     * There are a few scenarios, which has to be taken into account. Un-excepted exception was throw in:
     *      1.@BeforeAll scope
     *      2.@BeforeEach scope
     *      3.@Test scope
     *          b) @IsolatedTest
     *          a) @ParallelTest
     *          c) @ParallelNamespaceTest, which is basically in namespace with following pattern namespace-[n], where n is greater than 0.
     *      4.@AfterEach scope
     *      5.@AfterAll scope
     */
    public synchronized void collect() {
        Set<String> namespaces = NamespaceManager.getMapWithSuiteNamespaces().get(this.collectorElement);
        // ensure that namespaces created in beforeAll are also collected
        if (namespaces == null) {
            CollectorElement classCollectorElement = new CollectorElement(this.collectorElement.getTestClassName(), "");
            namespaces = NamespaceManager.getMapWithSuiteNamespaces().get(classCollectorElement);
        }
        // ensure that namespaces is never null
        namespaces = namespaces == null ? new HashSet<>() : namespaces;
        namespaces.add(clusterOperatorNamespace);

        // it's not test suite but test case, and we are going to collect logs
        if (!this.collectorElement.getTestMethodName().isEmpty()) {
            // @ParallelSuite -> this is generated namespace, but we collect logs only iff STRIMZI_RBAC_SCOPE=CLUSTER
            // because when we run STRIMZI_RBAC_SCOPE=NAMESPACE mode we use one namespace (i.e., clusterOperatorNamespace).
            if (!Environment.isNamespaceRbacScope()) {
                // @IsolatedTest or @ParallelTest or @ParallelNamespaceTest -> are executed in that generated namespace
                if (StUtils.isIsolatedTest(extensionContext) ||
                    StUtils.isParallelTest(extensionContext) ||
                    StUtils.isParallelNamespaceTest(extensionContext))  {

                    String testMethod = extensionContext.getTestMethod().isPresent() ? extensionContext.getTestMethod().get().getName() : "";

                    final Set<String> generatedTestSuiteNamespaces =
                        NamespaceManager.getMapWithSuiteNamespaces().get(
                            CollectorElement.createCollectorElement(
                                extensionContext.getRequiredTestClass().getName(),
                                testMethod
                            ));

                    if (generatedTestSuiteNamespaces != null) {
                        namespaces.addAll(generatedTestSuiteNamespaces);
                    }

                    LOGGER.debug("Namespace: {} added to set of Namespaces, which should be collected: {}", generatedTestSuiteNamespaces, namespaces.toString());
                }
            }
        }

        // collect logs for all namespace related to test suite
        namespaces.forEach(namespace -> {
            if (this.collectorElement.getTestMethodName().isEmpty()) {
                namespacePath = this.testSuitePath.resolve(namespace);
            } else {
                namespacePath = this.testCasePath.resolve(namespace);
            }

            boolean namespaceLogDirExist = this.namespacePath.toFile().exists() || this.namespacePath.toFile().mkdirs();
            if (!namespaceLogDirExist) {
                throw new RuntimeException("Unable to create path");
            }

            this.collectEvents(namespace);
            this.collectConfigMaps(namespace);
            this.collectSecrets(namespace);
            this.collectDeployments(namespace);
            this.collectLogsFromPods(namespace);
            this.collectAllResourcesFromNamespace(namespace);
            this.collectStrimzi(namespace);
            this.collectOlm(namespace);
        });

        // collect cluster-wide information
        this.collectClusterInfo();
    }

    private void collectLogsForTestSuite(final Pod pod) {
        if (pod.getMetadata().getLabels().containsKey(TestConstants.TEST_SUITE_NAME_LABEL)) {
            if (pod.getMetadata().getLabels().get(TestConstants.TEST_SUITE_NAME_LABEL).equals(StUtils.removePackageName(this.collectorElement.getTestClassName()))) {
                LOGGER.debug("Collecting logs for TestSuite: {}, and Pod: {}/{}", this.collectorElement.getTestClassName(), pod.getMetadata().getNamespace(), pod.getMetadata().getName());
                pod.getStatus().getContainerStatuses().forEach(
                    containerStatus -> scrapeAndCreateLogs(namespacePath, pod.getMetadata().getName(), containerStatus, pod.getMetadata().getNamespace()));
            }
        // Tracing Pods (they can't be labeled because CR of the Jaeger does not propagate labels to the Pods )
        } else if (pod.getMetadata().getName().contains("jaeger") || pod.getMetadata().getName().contains("cert-manager")) {
            LOGGER.debug("Collecting logs for TestSuite: {}, and Jaeger Pods: {}/{}", this.collectorElement.getTestClassName(), pod.getMetadata().getNamespace(), pod.getMetadata().getName());
            pod.getStatus().getContainerStatuses().forEach(
                containerStatus -> scrapeAndCreateLogs(namespacePath, pod.getMetadata().getName(), containerStatus, pod.getMetadata().getNamespace()));
        }
    }

    private void collectLogsForTestCase(final Pod pod) {
        if (pod.getMetadata().getLabels().containsKey(TestConstants.TEST_CASE_NAME_LABEL)) {
            // collect these Pods, which are deployed in that test case
            // startWith is used because when we put inside Pod label with test case sometimes this test case exceed 63
            // characters, and we have to cut it to avoid exception
            if (this.collectorElement.getTestMethodName().startsWith(pod.getMetadata().getLabels().get(TestConstants.TEST_CASE_NAME_LABEL))) {
                LOGGER.debug("Collecting logs for TestCase: {}, and Pod: {}/{}", this.collectorElement.getTestMethodName(), pod.getMetadata().getNamespace(), pod.getMetadata().getName());
                pod.getStatus().getContainerStatuses().forEach(
                    containerStatus -> scrapeAndCreateLogs(namespacePath, pod.getMetadata().getName(), containerStatus, pod.getMetadata().getNamespace()));
            }
        }
    }

    private void collectLogsFromPods(String namespace) {
        LOGGER.info("Collecting logs from Pod(s) in Namespace: {}", namespace);

        // in case we are in the cluster operator namespace we wants shared logs for whole test suite
        if (namespace.equals(this.clusterOperatorNamespace)) {
            kubeClient.listPods(namespace).forEach(pod -> {
                final String podName = pod.getMetadata().getName();
                try {
                    pod.getStatus().getContainerStatuses().forEach(
                            containerStatus -> scrapeAndCreateLogs(namespacePath, podName, containerStatus, namespace));
                } catch (RuntimeException ex) {
                    LOGGER.warn("Failed to collect logs from Pod: {}/{}", namespace, podName);
                }
            });
        // scrape for Pods, which are not in `cluster-operator` namespace
        } else {
            kubeClient.listPods(namespace).forEach(pod -> {
                try {
                    // we are collecting inside for test case
                    if (extensionContext.getTestMethod().isPresent()) {
                        // Pods, which are created by ResourceManager
                        collectLogsForTestCase(pod);
                        // Pods, which are shared between test cases
                        collectLogsForTestSuite(pod);
                    } else {
                        // Pods, which are shared between test cases (@BeforeAll, @AfterAll)
                        collectLogsForTestSuite(pod);
                    }
                } catch (RuntimeException ex) {
                    LOGGER.warn("Failed to collect logs from Pod: {}/{}", namespace, pod.getMetadata().getName());
                }
            });
        }
    }

    private void collectEvents(String namespace) {
        LOGGER.info("Collecting events in Namespace: {}", namespace);
        String events = cmdKubeClient(namespace).getEvents();
        // Write events to file
        writeFile(namespacePath.resolve("events.log"), events);
    }

    private void collectConfigMaps(String namespace) {
        Path configMapPath = namespacePath.resolve("configmaps");
        if (configMapPath.toFile().exists() || configMapPath.toFile().mkdirs()) {
            LOGGER.info("Collecting ConfigMaps in Namespace: {}", namespace);
            kubeClient.listConfigMaps(namespace).forEach(configMap ->
                    writeFile(configMapPath.resolve(configMap.getMetadata().getName() + ".log"), configMap.toString()));
        }
    }

    private void collectSecrets(String namespace) {
        Path secretPath = namespacePath.resolve("secrets");
        if (secretPath.toFile().exists() || secretPath.toFile().mkdirs()) {
            LOGGER.info("Collecting Secrets in Namespace: {}", namespace);
            kubeClient.listSecrets(namespace).forEach(secret ->
                    writeFile(secretPath.resolve(secret.getMetadata().getName() + ".log"), secret.toString()));
        }
    }

    private void collectAllResourcesFromNamespace(String namespace) {
        List<String> resources = new ArrayList<>(Arrays.asList(TestConstants.DEPLOYMENT, TestConstants.REPLICA_SET));

        // check if StrimziPodSets CRD is applied, if so, collect the yamls
        if (kubeClient.getCustomResourceDefinition(StrimziPodSet.CRD_NAME) != null) {
            resources.add(StrimziPodSet.RESOURCE_KIND);
        }

        resources.forEach(resource -> collectResource(resource, namespace));
    }

    private void collectOlm(String namespace) {
        if (Environment.CLUSTER_OPERATOR_INSTALL_TYPE == ClusterOperatorInstallType.OLM) {
            this.collectOperatorGroups(namespace);
            this.collectSubscriptions(namespace);
            this.collectClusterServiceVersions(namespace);
        }
    }

    private void collectResource(String kind, String namespace) {
        LOGGER.info("Collecting: {} in Namespace: {}", kind, namespace);
        writeFile(namespacePath.resolve(String.format("%ss.log", kind.toLowerCase(Locale.ROOT))), cmdKubeClient(namespace).getResourcesAsYaml(kind.toLowerCase(Locale.ROOT)));
    }

    private void collectStrimzi(String namespace) {
        LOGGER.info("Collecting Strimzi resources in Namespace: {}", namespace);
        String crData = cmdKubeClient(namespace).exec(false, Level.DEBUG, "get", "strimzi", "-o", "yaml", "-n", namespace).out();
        writeFile(namespacePath.resolve("strimzi-custom-resources.log"), crData);
    }

    private void collectDeployments(String namespace) {
        LOGGER.info("Collecting Deployments resources in Namespace: {}", namespace);
        String crData = cmdKubeClient(namespace).exec(false, Level.DEBUG, "get", "deployment", "-o", "yaml", "-n", namespace).out();
        writeFile(namespacePath.resolve("deployments.log"), crData);
    }

    private void collectClusterInfo() {
        LOGGER.info("Collecting cluster status");
        String nodes = cmdKubeClient().exec(false, Level.DEBUG, "describe", "nodes").out();
        writeFile(this.testSuitePath.resolve("cluster-status.log"), nodes);
    }

    private void collectOperatorGroups(String namespace) {
        LOGGER.info("Collecting OperatorGroups in Namespace: {}", namespace);
        String operatorGroups = cmdKubeClient(namespace).exec(false, Level.DEBUG, "get", "operatorGroups", "-o", "yaml", "-n", namespace).out();
        writeFile(namespacePath.resolve("operator-groups.log"), operatorGroups);
    }

    private void collectSubscriptions(String namespace) {
        LOGGER.info("Collecting Subscriptions in Namespace: {}", namespace);
        String subscriptions = cmdKubeClient(namespace).exec(false, Level.DEBUG, "get", "subscriptions", "-o", "yaml", "-n", namespace).out();
        writeFile(namespacePath.resolve("subscriptions.log"), subscriptions);
    }

    private void collectClusterServiceVersions(String namespace) {
        LOGGER.info("Collecting ClusterServiceVersions in Namespace: {}", namespace);
        String clusterServiceVersions = cmdKubeClient(namespace).exec(false, Level.DEBUG, "get", "clusterServiceVersions", "-o", "yaml", "-n", namespace).out();
        writeFile(namespacePath.resolve("cluster-service-versions.log"), clusterServiceVersions);
    }

    private void scrapeAndCreateLogs(Path path, String podName, ContainerStatus containerStatus, String namespace) {
        try {
            String log = kubeClient.getPodResource(namespace, podName).inContainer(containerStatus.getName()).getLog();
            // Write logs from containers to files
            writeFile(path.resolve("logs-pod-" + podName + "-container-" + containerStatus.getName() + ".log"), log);
        } catch (RuntimeException e) {
            LOGGER.warn("Unable to collect log from Pod: {}/{} and container: {} - Pod container is not initialized", namespace, podName, containerStatus.getName());
        }

        // Collect logs from previous version of the container
        try {
            String terminatedLog = kubeClient.getPodResource(namespace, podName).inContainer(containerStatus.getName()).terminated().getLog();
            writeFile(path.resolve("logs-pod-" + podName + "-container-" + containerStatus.getName() + ".terminated.log"), terminatedLog);
        } catch (RuntimeException e) {
            // For most of the Pods it will fail as it doesn't have restarted container, so we just want to skip the exception
        }

        // Describe all Pods
        String describe = cmdKubeClient(namespace).describe("pod", podName);
        writeFile(path.resolve("describe-pod-" + podName + "-container-" + containerStatus.getName() + ".log"), describe);
    }
}
