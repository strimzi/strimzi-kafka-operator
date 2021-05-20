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
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Set;
import java.util.TimeZone;

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
        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        CURRENT_DATE = simpleDateFormat.format(Calendar.getInstance().getTime());
    }

    private final KubeClient kubeClient;
    private final File testSuite;
    private final String testSuiteName;
    private final File testCase;
    private final File logDir;
    private final Pod clusterOperatorPod;
    private final String clusterOperatorNamespace;

    public LogCollector(String testSuiteName, String testCaseName, KubeClient kubeClient, String logDir) throws IOException {
        this.kubeClient = kubeClient;
        this.testSuiteName = testSuiteName;

        this.logDir = new File(logDir + "/ " + CURRENT_DATE);
        final String logSuiteDir = this.logDir + "/" + this.testSuiteName;

        this.testSuite = new File(logSuiteDir);

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

        // collect logs for all namespace related to test suite
        namespaces.stream().forEach(namespace -> {
            this.collectEvents(namespace);
            this.collectConfigMaps(namespace);
            this.collectLogsFromPods(namespace);
            this.collectDeployments(namespace);
            this.collectStatefulSets(namespace);
            this.collectReplicaSets(namespace);
            this.collectStrimzi(namespace);
            this.collectClusterInfo(namespace);
        });
    }

    @Override
    public void collectLogsFromPods(String namespaceName) {
        try {
            LOGGER.info("Collecting logs for Pod(s) in namespace {}", namespaceName);

            // in case we are in the cluster operator namespace we wants shared logs for whole test suite
            if (namespaceName.equals(this.clusterOperatorNamespace)) {
                kubeClient.listPods(namespaceName).forEach(pod -> {
                    String podName = pod.getMetadata().getName();
                    pod.getStatus().getContainerStatuses().forEach(containerStatus -> {
                        scrapeAndCreateLogs(testSuite, namespaceName, podName, containerStatus);
                    });
                });
            } else {
                kubeClient.listPods(namespaceName).forEach(pod -> {
                    String podName = pod.getMetadata().getName();
                    pod.getStatus().getContainerStatuses().forEach(containerStatus -> {
                        scrapeAndCreateLogs(testCase, namespaceName, podName, containerStatus);
                    });
                });
            }
        } catch (Exception allExceptions) {
            LOGGER.warn("Searching for logs in all pods failed! Some of the logs will not be stored. Exception message" + allExceptions.getMessage());
        }
    }

    @Override
    public void collectEvents(String namespaceName) {
        LOGGER.info("Collecting events in Namespace {}", namespaceName);
        String events = cmdKubeClient(namespaceName).getEvents();
        // Write events to file
        writeFile(testCase + "/" + namespaceName + "_events.log", events);
    }

    @Override
    public void collectConfigMaps(String namespaceName) {
        LOGGER.info("Collecting ConfigMaps in Namespace {}", namespaceName);
        kubeClient.listConfigMaps(namespaceName).forEach(configMap -> {
            writeFile(testCase + "/" + namespaceName + "_" + configMap.getMetadata().getName() + ".log", configMap.toString());
        });
    }

    @Override
    public void collectDeployments(String namespaceName) {
        LOGGER.info("Collecting Deployments in Namespace {}", namespaceName);
        writeFile(testCase + "/" + namespaceName + "_deployments.log", cmdKubeClient(namespaceName).getResourcesAsYaml(Constants.DEPLOYMENT));
    }

    @Override
    public void collectStatefulSets(String namespaceName) {
        LOGGER.info("Collecting StatefulSets in Namespace {}", namespaceName);
        writeFile(testCase + "/" + namespaceName + "_statefulsets.log", cmdKubeClient(namespaceName).getResourcesAsYaml(Constants.STATEFUL_SET));
    }

    @Override
    public void collectReplicaSets(String namespaceName) {
        LOGGER.info("Collecting ReplicaSet in Namespace {}", namespaceName);
        writeFile(testCase + "/" + namespaceName + "_replicasets.log", cmdKubeClient(namespaceName).getResourcesAsYaml("replicaset"));
    }

    public void collectStrimzi(String namespaceName) {
        LOGGER.info("Collecting Strimzi in Namespace {}", namespaceName);
        String crData = cmdKubeClient(namespaceName).exec(false, "get", "strimzi", "-o", "yaml", "-n", namespaceName).out();
        writeFile(testCase + "/" + namespaceName + "_strimzi-custom-resources.log", crData);
    }

    public void collectClusterInfo(String namespaceName) {
        LOGGER.info("Collecting cluster status");
        String nodes = cmdKubeClient(namespaceName).exec(false, "describe", "nodes").out();
        writeFile(testCase + "/" + namespaceName + "_cluster-status.log", nodes);
    }

    private void scrapeAndCreateLogs(File path, String namespaceName, String podName, ContainerStatus containerStatus) {
        String log = kubeClient.getPodResource(namespaceName, podName).inContainer(containerStatus.getName()).getLog();
        // Write logs from containers to files
        writeFile(path + "/" + namespaceName + "_logs-pod-" + podName + "-container-" + containerStatus.getName() + ".log", log);
        // Describe all pods
        String describe = cmdKubeClient(namespaceName).describe("pod", podName);
        writeFile(path + "/" + namespaceName + "_describe-pod-" + podName + "-container-" + containerStatus.getName() + ".log", describe);
    }
}
