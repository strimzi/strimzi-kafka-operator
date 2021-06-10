/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.logs;

import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.systemtest.Constants;
import io.strimzi.test.k8s.KubeClient;
import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
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
 *                  deployment.log
 *                  configmap.log
 *                  ...
 *
 *              cluster-operator.log    // shared cluster operator logs for all tests inside one test suite
 *          /another-test-suite_time/
 *      ...
 */
public class LogCollector implements LogCollect {
    private static final Logger LOGGER = LogManager.getLogger(LogCollector.class);

    private static final String CURRENT_DATE;

    static {
        // Get current date to create a unique folder
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
        dateTimeFormatter = dateTimeFormatter.withZone(ZoneId.of("GMT"));
        CURRENT_DATE = dateTimeFormatter.format(LocalDate.now());
    }

    private final KubeClient kubeClient;
    private final File testSuite;
    private final String testSuiteName;
    private final File testCase;
    private final File logDir;
    private final Pod clusterOperatorPod;
    private final String clusterOperatorNamespace;
    private File namespaceFile;

    public LogCollector(String testSuiteName, String testCaseName, KubeClient kubeClient, String logDir) throws IOException {
        this.kubeClient = kubeClient;
        this.testSuiteName = testSuiteName;

        this.logDir = new File(logDir + "/" + CURRENT_DATE);
        final String logSuiteDir = this.logDir + "/" + this.testSuiteName;

        this.testSuite = new File(logSuiteDir);

        // @BeforeAll in AbstractST always pass, and I ensure that we always deploy CO and after that some error can occur
        this.clusterOperatorPod = kubeClient.getClient().pods().inAnyNamespace().list().getItems().stream()
            .filter(pod -> pod.getMetadata().getName().contains(Constants.STRIMZI_DEPLOYMENT_NAME))
            // contract only one Cluster Operator deployment inside all namespaces
            .findFirst()
            .orElseThrow();
        this.clusterOperatorNamespace = this.clusterOperatorPod.getMetadata().getNamespace();
        this.testCase = new File(logSuiteDir + "/" + testCaseName);

        boolean logDirExist = this.logDir.exists() || this.logDir.mkdirs();
        boolean logTestSuiteDirExist = this.testSuite.exists() || this.testSuite.mkdirs();
        boolean logTestCaseDirExist = this.testCase.exists() || this.testCase.mkdirs();

        if (!logDirExist) throw new IOException("Unable to create path");
        if (!logTestSuiteDirExist) throw new IOException("Unable to create path");
        if (!logTestCaseDirExist) throw new IOException("Unable to create path");
    }

    /**
     * Core method which collects all logs from events, configs-maps, pods, deployment, statefulset, replicaset...in case test fails.
     */
    public void collect() {
        final Set<String> namespaces = KubeClusterResource.getInstance().getMapWithSuiteNamespaces().get(this.testSuiteName);

        if (namespaces != null) {
            // collect logs for all namespace related to test suite
            namespaces.stream().forEach(namespace -> {
                namespaceFile = new File(testCase + "/" + namespace);

                boolean namespaceLogDirExist = this.namespaceFile.exists() || this.namespaceFile.mkdirs();
                if (!namespaceLogDirExist) throw new RuntimeException("Unable to create path");

                this.collectEvents();
                this.collectConfigMaps();
                this.collectLogsFromPods();
                this.collectDeployments();
                this.collectStatefulSets();
                this.collectReplicaSets();
                this.collectStrimzi();
                this.collectClusterInfo();
            });
        }
    }

    @Override
    public void collectLogsFromPods() {
        try {
            LOGGER.info("Collecting logs for Pod(s) in namespace {}", namespaceFile.getName());

            // in case we are in the cluster operator namespace we wants shared logs for whole test suite
            if (namespaceFile.getName().equals(this.clusterOperatorNamespace)) {
                kubeClient.listPods(namespaceFile.getName()).forEach(pod -> {
                    String podName = pod.getMetadata().getName();
                    pod.getStatus().getContainerStatuses().forEach(containerStatus -> {
                        scrapeAndCreateLogs(testSuite, podName, containerStatus);
                    });
                });
            } else {
                kubeClient.listPods(namespaceFile.getName()).forEach(pod -> {
                    String podName = pod.getMetadata().getName();
                    pod.getStatus().getContainerStatuses().forEach(containerStatus -> {
                        scrapeAndCreateLogs(namespaceFile, podName, containerStatus);
                    });
                });
            }
        } catch (Exception allExceptions) {
            LOGGER.warn("Searching for logs in all pods failed! Some of the logs will not be stored. Exception message" + allExceptions.getMessage());
        }
    }

    @Override
    public void collectEvents() {
        LOGGER.info("Collecting events in Namespace {}", namespaceFile);
        String events = cmdKubeClient(namespaceFile.getName()).getEvents();
        // Write events to file
        writeFile(namespaceFile + "/events.log", events);
    }

    @Override
    public void collectConfigMaps() {
        LOGGER.info("Collecting ConfigMaps in Namespace {}", namespaceFile);
        kubeClient.listConfigMaps(namespaceFile.getName()).forEach(configMap -> {
            writeFile(namespaceFile + "/" + configMap.getMetadata().getName() + ".log", configMap.toString());
        });
    }

    @Override
    public void collectDeployments() {
        LOGGER.info("Collecting Deployments in Namespace {}", namespaceFile);
        writeFile(namespaceFile + "/deployments.log", cmdKubeClient(namespaceFile.getName()).getResourcesAsYaml(Constants.DEPLOYMENT));
    }

    @Override
    public void collectStatefulSets() {
        LOGGER.info("Collecting StatefulSets in Namespace {}", namespaceFile);
        writeFile(namespaceFile + "/statefulsets.log", cmdKubeClient(namespaceFile.getName()).getResourcesAsYaml(Constants.STATEFUL_SET));
    }

    @Override
    public void collectReplicaSets() {
        LOGGER.info("Collecting ReplicaSet in Namespace {}", namespaceFile);
        writeFile(namespaceFile + "/replicasets.log", cmdKubeClient(namespaceFile.getName()).getResourcesAsYaml("replicaset"));
    }

    public void collectStrimzi() {
        LOGGER.info("Collecting Strimzi in Namespace {}", namespaceFile);
        String crData = cmdKubeClient(namespaceFile.getName()).exec(false, "get", "strimzi", "-o", "yaml", "-n", namespaceFile.getName()).out();
        writeFile(namespaceFile + "/strimzi-custom-resources.log", crData);
    }

    public void collectClusterInfo() {
        LOGGER.info("Collecting cluster status");
        String nodes = cmdKubeClient(namespaceFile.getName()).exec(false, "describe", "nodes").out();
        writeFile(namespaceFile + "/cluster-status.log", nodes);
    }

    private void scrapeAndCreateLogs(File path, String podName, ContainerStatus containerStatus) {
        String log = kubeClient.getPodResource(namespaceFile.getName(), podName).inContainer(containerStatus.getName()).getLog();
        // Write logs from containers to files
        writeFile(path + "/logs-pod-" + podName + "-container-" + containerStatus.getName() + ".log", log);
        // Describe all pods
        String describe = cmdKubeClient(namespaceFile.getName()).describe("pod", podName);
        writeFile(path + "/describe-pod-" + podName + "-container-" + containerStatus.getName() + ".log", describe);
    }
}
