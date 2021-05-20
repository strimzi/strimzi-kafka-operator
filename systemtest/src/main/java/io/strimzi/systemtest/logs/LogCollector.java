/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.logs;

import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.systemtest.Constants;
import io.strimzi.test.k8s.KubeClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
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
 *                  /configmaps/
 *                  /events
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
        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        CURRENT_DATE = simpleDateFormat.format(Calendar.getInstance().getTime());
    }

    private final KubeClient kubeClient;
    private final String namespace;
    private final File testSuite;
    private final File testCase;
    private final File logDir;
    private final File configMapDir;
    private final File eventsDir;
    private CoLogCollector coLogCollector;

    public LogCollector(String namespaceName, String testSuite, String testCase, KubeClient kubeClient, String logDir) throws IOException {
        this.kubeClient = kubeClient;
        this.namespace = namespaceName;


        this.logDir = new File(logDir + "_" + CURRENT_DATE);
        final String logSuiteDir = this.logDir + "/" + testSuite;

        this.testSuite = new File(logSuiteDir);
        this.coLogCollector = new CoLogCollector();
        this.testCase = new File(logSuiteDir + "/" + testCase);
        this.eventsDir = new File(this.testCase + "/events");
        this.configMapDir = new File(this.testCase + "/configMaps");

        boolean logDirExist = this.logDir.exists() || this.logDir.mkdirs();
        boolean logTestSuiteDirExist = this.testSuite.exists() || this.testSuite.mkdirs();
        boolean logTestCaseDirExist = this.testCase.exists() || this.testCase.mkdirs();
        boolean logEventDirExist = this.eventsDir.exists() || this.eventsDir.mkdirs();
        boolean logConfigMapExist = this.configMapDir.exists() || this.configMapDir.mkdirs();

        if (!logDirExist) throw new IOException("Unable to create path");
        if (!logTestSuiteDirExist) throw new IOException("Unable to create path");
        if (!logTestCaseDirExist) throw new IOException("Unable to create path");
        if (!logEventDirExist) throw new IOException("Unable to create path");
        if (!logConfigMapExist) throw new IOException("Unable to create path");
    }

    /**
     * CoLogCollector encapsulates different implementation of collecting logs from resource such as Deployment, ReplicaSet, ConfigMaps and so on.
     */
    class CoLogCollector implements LogCollect {

        private final Pod clusterOperatorPod;
        private final String clusterOperatorNamespace;

        public CoLogCollector() {
            this.clusterOperatorPod = kubeClient.getClient().pods().inAnyNamespace().list().getItems().stream()
                .filter(pod -> pod.getMetadata().getName().contains(Constants.STRIMZI_DEPLOYMENT_NAME))
                // contract only one Cluster Operator deployment inside all namespaces
                .findFirst()
                .orElseThrow();
            this.clusterOperatorNamespace = this.clusterOperatorPod.getMetadata().getNamespace();
        }

        @Override
        public void collectLogsFromPods() {
            // cluster operator pod logs
            final String coPodName = this.clusterOperatorPod.getMetadata().getName();
            this.clusterOperatorPod.getStatus().getContainerStatuses().forEach(containerStatus -> {
                scrapeAndCreateLogs(testSuite, this.clusterOperatorNamespace, coPodName, containerStatus);
            });
        }

        @Override
        public void collectEvents() {
            String events = cmdKubeClient(this.clusterOperatorNamespace).getEvents();
            // Write events to file
            writeFile(eventsDir + "/" + "cluster-operator-events-in-namespace" + this.clusterOperatorNamespace + ".log", events);
        }

        @Override
        public void collectConfigMaps() {
            kubeClient.getClient().configMaps().inNamespace(this.clusterOperatorNamespace).list().getItems().stream()
                .filter(configMap -> configMap.getMetadata().getName().contains(Constants.STRIMZI_DEPLOYMENT_NAME))
                .forEach(configMap -> writeFile(configMapDir + "/" + configMap.getMetadata().getName() + "-" + namespace + ".log", configMap.toString()));
        }

        @Override
        public void collectDeployments() {
            kubeClient.getClient().apps().deployments().inNamespace(this.clusterOperatorNamespace).list().getItems().stream()
                .filter(deployment -> deployment.getMetadata().getName().contains(Constants.STRIMZI_DEPLOYMENT_NAME))
                .forEach(deployment -> writeFile(testCase + "/" + deployment.getMetadata().getName() + "-" + namespace + ".log", deployment.toString()));
        }
        @Override
        public void collectStatefulSets() {
            kubeClient.getClient().apps().statefulSets().inNamespace(this.clusterOperatorNamespace).list().getItems().stream()
                .filter(statefulSet -> statefulSet.getMetadata().getName().contains(Constants.STRIMZI_DEPLOYMENT_NAME))
                .forEach(statefulSet -> writeFile(testCase + "/" + statefulSet.getMetadata().getName() + "-" + namespace + ".log", statefulSet.toString()));
        }
        @Override
        public void collectReplicaSets() {
            kubeClient.getClient().apps().replicaSets().inNamespace(this.clusterOperatorNamespace).list().getItems().stream()
                .filter(replicaSet -> replicaSet.getMetadata().getName().contains(Constants.STRIMZI_DEPLOYMENT_NAME))
                .forEach(replicaSet -> writeFile(testCase + "/" + replicaSet.getMetadata().getName() + "-" + namespace + ".log", replicaSet.toString()));
        }
    }

    @Override
    public void collectLogsFromPods() {
        try {
            coLogCollector.collectLogsFromPods();
            LOGGER.info("Collecting logs for Pod(s) in namespace {}", namespace);

            kubeClient.listPods(namespace).forEach(pod -> {
                String podName = pod.getMetadata().getName();
                pod.getStatus().getContainerStatuses().forEach(containerStatus -> {
                    scrapeAndCreateLogs(testCase, namespace, podName, containerStatus);
                });
            });
        } catch (Exception allExceptions) {
            LOGGER.warn("Searching for logs in all pods failed! Some of the logs will not be stored. Exception message" + allExceptions.getMessage());
        }
    }

    @Override
    public void collectEvents() {
        coLogCollector.collectEvents();
        LOGGER.info("Collecting events in Namespace {}", namespace);
        String events = cmdKubeClient(namespace).getEvents();
        // Write events to file
        writeFile(eventsDir + "/" + "events-in-namespace" + namespace + ".log", events);
    }

    @Override
    public void collectConfigMaps() {
        coLogCollector.collectConfigMaps();
        LOGGER.info("Collecting ConfigMaps in Namespace {}", namespace);
        kubeClient.listConfigMaps(namespace).forEach(configMap -> {
            writeFile(configMapDir + "/" + configMap.getMetadata().getName() + "-" + namespace + ".log", configMap.toString());
        });
    }

    @Override
    public void collectDeployments() {
        coLogCollector.collectDeployments();
        LOGGER.info("Collecting Deployments in Namespace {}", namespace);
        writeFile(testCase + "/deployments.log", cmdKubeClient(namespace).getResourcesAsYaml(Constants.DEPLOYMENT));
    }

    @Override
    public void collectStatefulSets() {
        coLogCollector.collectStatefulSets();
        LOGGER.info("Collecting StatefulSets in Namespace {}", namespace);
        writeFile(testCase + "/statefulsets.log", cmdKubeClient(namespace).getResourcesAsYaml(Constants.STATEFUL_SET));
    }

    @Override
    public void collectReplicaSets() {
        coLogCollector.collectReplicaSets();
        LOGGER.info("Collecting ReplicaSet in Namespace {}", namespace);
        writeFile(testCase + "/replicasets.log", cmdKubeClient(namespace).getResourcesAsYaml("replicaset"));
    }

    public void collectStrimzi() {
        LOGGER.info("Collecting Strimzi in Namespace {}", namespace);
        String crData = cmdKubeClient(namespace).exec(false, "get", "strimzi", "-o", "yaml", "-n", namespace).out();
        writeFile(testCase + "/strimzi-custom-resources.log", crData);
    }

    public void collectClusterInfo() {
        LOGGER.info("Collecting cluster status");
        String nodes = cmdKubeClient(namespace).exec(false, "describe", "nodes").out();
        writeFile(testCase + "/cluster-status.log", nodes);
    }

    private void scrapeAndCreateLogs(File path, String namespaceName, String podName, ContainerStatus containerStatus) {
        String log = kubeClient.getPodResource(namespaceName, podName).inContainer(containerStatus.getName()).getLog();
        // Write logs from containers to files
        writeFile(path + "/" + "logs-pod-" + podName + "-container-" + containerStatus.getName() + ".log", log);
        // Describe all pods
        String describe = cmdKubeClient(namespaceName).describe("pod", podName);
        writeFile(path + "/" + "describe-pod-" + podName + "-container-" + containerStatus.getName() + ".log", describe);
    }
}
