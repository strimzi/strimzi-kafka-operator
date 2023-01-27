/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.logs;

import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.ParallelSuite;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.logs.CollectorElement;
import io.strimzi.test.k8s.KubeClient;
import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.apache.logging.log4j.Level;

import java.io.File;
import java.io.IOException;
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
    private final File testSuite;
    private final File testCase;
    private final File logDir;
    private final String clusterOperatorNamespace;
    private File namespaceFile;
    private CollectorElement collectorElement;
    private ExtensionContext extensionContext;

    public LogCollector(ExtensionContext extensionContext, CollectorElement collectorElement, KubeClient kubeClient, String logDir) throws IOException {
        this.extensionContext = extensionContext;
        this.collectorElement = collectorElement;
        this.kubeClient = kubeClient;

        this.logDir = new File(logDir + "/" + CURRENT_DATE);
        final String logSuiteDir = this.logDir + "/" + collectorElement.getTestClassName();

        this.testSuite = new File(logSuiteDir);

        // contract only one Cluster Operator deployment inside all namespaces
        Pod clusterOperatorPod = kubeClient.getClient().pods().inAnyNamespace().list().getItems().stream()
                .filter(pod -> pod.getMetadata().getName().contains(Constants.STRIMZI_DEPLOYMENT_NAME))
                // contract only one Cluster Operator deployment inside all namespaces
                .findFirst()
                .orElseGet(Pod::new);

        this.clusterOperatorNamespace = clusterOperatorPod.getMetadata() != null ?
                clusterOperatorPod.getMetadata().getNamespace() :
                kubeClient.getNamespace();

        this.testCase = new File(logSuiteDir + "/" + collectorElement.getTestMethodName());

        boolean logDirExist = this.logDir.exists() || this.logDir.mkdirs();
        boolean logTestSuiteDirExist = this.testSuite.exists() || this.testSuite.mkdirs();
        boolean logTestCaseDirExist = this.testCase.exists() || this.testCase.mkdirs();

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
     * {@link #collectLogsFromPods(String)}, {@link #collectAllResourcesFromNamespace(String)} (String)},
     * {@link #collectStrimzi(String)} and lastly {@link #collectClusterInfo(String)}.
     *
     * If anything fails in @BeforeAll or @AfterAll suite we gather these logs from @cde{this.testSuite}/@code{namespace}.
     * Otherwise we collect it in code@{this.testCase}/code{namespace}.
     *
     * There are a few scenarios, which has to be take into account. Un-excepted exception was throw in:
     *      1.@BeforeAll scope
     *      2.@BeforeEach scope
     *      3.@Test scope
     *          b) @IsolatedTest
     *          a) @ParallelTest
     *          c) @ParallelNamespaceTest, which is basically in namespace with following pattern namespace-[n], where n is greater than 0.
     *      4.@AfterEach scope
     *      5.@AfterAll scope
     *
     *  More advanced is when {@link ParallelTest#annotationType()} or {@link IsolatedTest#annotationType()} are executed inside
     *  {@link ParallelSuite#annotationType()}, which means they use automatically generated namespace by
     *  {@link io.strimzi.systemtest.parallel.TestSuiteNamespaceManager#createAdditionalNamespaces(ExtensionContext)}.
     */
    public synchronized void collect() {
        Set<String> namespaces = KubeClusterResource.getMapWithSuiteNamespaces().get(this.collectorElement);
        // ensure that namespaces is never null
        namespaces = namespaces == null ? new HashSet<>() : namespaces;
        namespaces.add(clusterOperatorNamespace);

        // it's not test suite but test case, and we are gonna collect logs
        if (!this.collectorElement.getTestMethodName().isEmpty()) {
            // @ParallelSuite -> this is generated namespace but we collect logs only iff STRIMZI_RBAC_SCOPE=CLUSTER
            // because when we run STRIMZI_RBAC_SCOPE=NAMESPACE mode we use one namespace (i.e., clusterOperatorNamespace).
            if (StUtils.isParallelSuite(extensionContext.getParent().get()) && !Environment.isNamespaceRbacScope()) {
                // @IsolatedTest or @ParallelTest or @ParallelNamespaceTest -> are executed in that generated namespace
                if (StUtils.isIsolatedTest(extensionContext) ||
                    StUtils.isParallelTest(extensionContext) ||
                    StUtils.isParallelNamespaceTest(extensionContext))  {

                    String testMethod = extensionContext.getTestMethod().isPresent() ? extensionContext.getTestMethod().get().getName() : "";

                    final Set<String> generatedTestSuiteNamespaces =
                        KubeClusterResource.getMapWithSuiteNamespaces().get(
                            CollectorElement.createCollectorElement(
                                extensionContext.getRequiredTestClass().getName(),
                                testMethod
                            ));

                    if (generatedTestSuiteNamespaces != null) {
                        namespaces.addAll(generatedTestSuiteNamespaces);
                    }

                    LOGGER.debug("{} adding to all namespaces, which should be collected: {}", generatedTestSuiteNamespaces, namespaces.toString());
                }
            }
        }

        // collect logs for all namespace related to test suite
        namespaces.forEach(namespace -> {
            if (this.collectorElement.getTestMethodName().isEmpty()) {
                namespaceFile = new File(this.testSuite +  "/" + namespace);
            } else {
                namespaceFile = new File(this.testCase + "/" + namespace);
            }

            boolean namespaceLogDirExist = this.namespaceFile.exists() || this.namespaceFile.mkdirs();
            if (!namespaceLogDirExist) {
                throw new RuntimeException("Unable to create path");
            }

            this.collectEvents(namespace);
            this.collectConfigMaps(namespace);
            this.collectLogsFromPods(namespace);
            this.collectAllResourcesFromNamespace(namespace);
            this.collectStrimzi(namespace);
            this.collectClusterInfo(namespace);
        });
    }

    private final void collectLogsForTestSuite(final Pod pod) {
        if (pod.getMetadata().getLabels().containsKey(Constants.TEST_SUITE_NAME_LABEL)) {
            if (pod.getMetadata().getLabels().get(Constants.TEST_SUITE_NAME_LABEL).equals(StUtils.removePackageName(this.collectorElement.getTestClassName()))) {
                LOGGER.debug("Collecting logs for TestSuite: {}, and Pod {}/{}", this.collectorElement.getTestClassName(), pod.getMetadata().getNamespace(), pod.getMetadata().getName());
                pod.getStatus().getContainerStatuses().forEach(
                    containerStatus -> scrapeAndCreateLogs(namespaceFile, pod.getMetadata().getName(), containerStatus, pod.getMetadata().getNamespace()));
            }
        // Tracing pods (they can't be labeled because CR of the Jaeger does not propagate labels to the Pods )
        } else if (pod.getMetadata().getName().contains("jaeger") || pod.getMetadata().getName().contains("cert-manager")) {
            LOGGER.debug("Collecting logs for TestSuite: {}, and Jaeger Pods {}/{}", this.collectorElement.getTestClassName(), pod.getMetadata().getNamespace(), pod.getMetadata().getName());
            pod.getStatus().getContainerStatuses().forEach(
                containerStatus -> scrapeAndCreateLogs(namespaceFile, pod.getMetadata().getName(), containerStatus, pod.getMetadata().getNamespace()));
        }
    }

    private final void collectLogsForTestCase(final Pod pod) {
        if (pod.getMetadata().getLabels().containsKey(Constants.TEST_CASE_NAME_LABEL)) {
            // collect these Pods, which are deployed in that test case
            // startWith is used because when we put inside Pod label with test case sometimes this test case exceed 63
            // characters and we have to cut it to avoid exception
            if (this.collectorElement.getTestMethodName().startsWith(pod.getMetadata().getLabels().get(Constants.TEST_CASE_NAME_LABEL))) {
                LOGGER.debug("Collecting logs for TestCase: {}, and Pod {}/{}", this.collectorElement.getTestMethodName(), pod.getMetadata().getNamespace(), pod.getMetadata().getName());
                pod.getStatus().getContainerStatuses().forEach(
                    containerStatus -> scrapeAndCreateLogs(namespaceFile, pod.getMetadata().getName(), containerStatus, pod.getMetadata().getNamespace()));
            }
        }
    }

    private void collectLogsFromPods(String namespace) {
        LOGGER.info("Collecting logs for Pod(s) in Namespace {}", namespace);

        // in case we are in the cluster operator namespace we wants shared logs for whole test suite
        if (namespace.equals(this.clusterOperatorNamespace)) {
            kubeClient.listPods(namespace).forEach(pod -> {
                final String podName = pod.getMetadata().getName();
                try {
                    pod.getStatus().getContainerStatuses().forEach(
                            containerStatus -> scrapeAndCreateLogs(namespaceFile, podName, containerStatus, namespace));
                } catch (Exception ex) {
                    LOGGER.warn("Failed to collect logs from Pod {}/{}", namespace, podName);
                }
            });
        // scrape for Pods, which are not in `cluster-operator` namespace
        } else {
            kubeClient.listPods(namespace).forEach(pod -> {
                try {
                    // we are collecting inside for test case
                    if (extensionContext.getTestMethod().isPresent()) {
                        // pods, which are created by ResourceManager
                        collectLogsForTestCase(pod);
                        // pods, which are shared between test cases
                        collectLogsForTestSuite(pod);
                    } else {
                        // pods, which are shared between test cases (@BeforeAll, @AfterAll)
                        collectLogsForTestSuite(pod);
                    }
                } catch (Exception ex) {
                    LOGGER.warn("Failed to collect logs from Pod {}/{}", namespace, pod.getMetadata().getName());
                }
            });
        }
    }

    private void collectEvents(String namespace) {
        LOGGER.info("Collecting events in Namespace {}", namespace);
        String events = cmdKubeClient(namespace).getEvents();
        // Write events to file
        writeFile(namespaceFile + "/events.log", events);
    }

    private void collectConfigMaps(String namespace) {
        LOGGER.info("Collecting ConfigMaps in Namespace {}", namespace);
        kubeClient.listConfigMaps(namespace).forEach(configMap -> {
            writeFile(namespaceFile + "/" + configMap.getMetadata().getName() + ".log", configMap.toString());
        });
    }

    private void collectAllResourcesFromNamespace(String namespace) {
        List<String> resources = new ArrayList<>(Arrays.asList(Constants.DEPLOYMENT, Constants.REPLICA_SET));

        if (!Environment.isStrimziPodSetEnabled()) {
            resources.add(Constants.STATEFUL_SET);
        } else {
            // check if StrimziPodSets CRD is applied, if so, collect the yamls
            if (kubeClient.getCustomResourceDefinition(StrimziPodSet.CRD_NAME) != null) {
                resources.add(StrimziPodSet.RESOURCE_KIND);
            }
        }

        resources.forEach(resource -> collectResource(resource, namespace));
    }

    private void collectResource(String kind, String namespace) {
        LOGGER.info("Collecting {} in Namespace {}", kind, namespace);
        writeFile(String.format("%s/%ss.log", namespaceFile, kind.toLowerCase(Locale.ROOT)), cmdKubeClient(namespace).getResourcesAsYaml(kind.toLowerCase(Locale.ROOT)));
    }

    private void collectStrimzi(String namespace) {
        LOGGER.info("Collecting Strimzi in Namespace {}", namespace);
        String crData = cmdKubeClient(namespace).exec(false, Level.DEBUG, "get", "strimzi", "-o", "yaml", "-n", namespaceFile.getName()).out();
        writeFile(namespaceFile + "/strimzi-custom-resources.log", crData);
    }

    private void collectClusterInfo(String namespace) {
        LOGGER.info("Collecting cluster status");
        String nodes = cmdKubeClient(namespace).exec(false, Level.DEBUG, "describe", "nodes").out();
        writeFile(this.testSuite + "/cluster-status.log", nodes);
    }

    private void scrapeAndCreateLogs(File path, String podName, ContainerStatus containerStatus, String namespace) {
        try {
            String log = kubeClient.getPodResource(namespace, podName).inContainer(containerStatus.getName()).getLog();
            // Write logs from containers to files
            writeFile(path + "/logs-pod-" + podName + "-container-" + containerStatus.getName() + ".log", log);
        } catch (Exception e) {
            LOGGER.warn("Unable to collect log from pod: {} and container: {} - pod container is not initialized", podName, containerStatus.getName());
        }

        // Describe all pods
        String describe = cmdKubeClient(namespace).describe("pod", podName);
        writeFile(path + "/describe-pod-" + podName + "-container-" + containerStatus.getName() + ".log", describe);
    }
}
