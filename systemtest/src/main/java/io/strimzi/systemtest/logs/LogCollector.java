/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.logs;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.strimzi.test.k8s.KubeClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;

import static io.strimzi.test.TestUtils.writeFile;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;

public class LogCollector {
    private static final Logger LOGGER = LogManager.getLogger(LogCollector.class);

    private KubeClient kubeClient;
    private String namespace;
    private File logDir;
    private File configMapDir;
    private File eventsDir;

    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    public LogCollector(KubeClient kubeClient, File logDir) {
        this.kubeClient = kubeClient;
        this.namespace = kubeClient.getNamespace();
        this.logDir = logDir;
        this.eventsDir = new File(logDir + "/events");
        this.configMapDir = new File(logDir + "/configMaps");
        logDir.mkdirs();

        if (!eventsDir.exists()) {
            eventsDir.mkdirs();
        }
        if (!configMapDir.exists()) {
            configMapDir.mkdirs();
        }
    }

    public void collectLogsFromPods() {
        LOGGER.info("Collecting logs for Pod(s) in namespace {}", namespace);

        try {
            kubeClient.listPods().forEach(pod -> {
                String podName = pod.getMetadata().getName();
                pod.getStatus().getContainerStatuses().forEach(containerStatus -> {
                    String log = kubeClient.getPodResource(podName).inContainer(containerStatus.getName()).getLog();
                    // Write logs from containers to files
                    writeFile(logDir + "/" + "logs-pod-" + podName + "-container-" + containerStatus.getName() + ".log", log);
                    // Describe all pods
                    String describe = cmdKubeClient().describe("pod", podName);
                    writeFile(logDir + "/" + "describe-pod-" + podName + "-container-" + containerStatus.getName() + ".log", describe);
                });
            });
        } catch (Exception allExceptions) {
            LOGGER.warn("Searching for logs in all pods failed! Some of the logs will not be stored.");
        }
    }

    public void collectEvents() {
        LOGGER.info("Collecting events in Namespace {}", namespace);
        String events = cmdKubeClient().getEvents();
        // Write events to file
        writeFile(eventsDir + "/" + "events-in-namespace" + kubeClient.getNamespace() + ".log", events);
    }

    public void collectConfigMaps() {
        LOGGER.info("Collecting ConfigMaps in Namespace {}", namespace);
        kubeClient.listConfigMaps().forEach(configMap -> {
            writeFile(configMapDir + "/" + configMap.getMetadata().getName() + "-" + namespace + ".log", configMap.toString());
        });
    }

    public void collectDeployments() {
        LOGGER.info("Collecting Deployments in Namespace {}", namespace);
        writeFile(logDir + "/deployments.log", cmdKubeClient().list("deployment").toString());
    }

    public void collectStatefulSets() {
        LOGGER.info("Collecting StatefulSets in Namespace {}", namespace);
        writeFile(logDir + "/statefulsets.log", cmdKubeClient().list("statefulset").toString());
    }

    public void collectReplicaSets() {
        LOGGER.info("Collecting ReplicaSet in Namespace {}", namespace);
        writeFile(logDir + "/replicasets.log", cmdKubeClient().list("replicaset").toString());
    }

    public void collectStrimzi() {
        LOGGER.info("Collecting Strimzi in Namespace {}", namespace);
        String crData = cmdKubeClient().exec(false, "get", "strimzi", "-o", "yaml", "-n", namespace).out();
        writeFile(logDir + "/strimzi-custom-resources.log", crData);
    }

    public void collectClusterInfo() {
        LOGGER.info("Collecting cluster status");
        String nodes = cmdKubeClient().exec(false, "describe", "nodes").out();
        writeFile(logDir + "/cluster-status.log", nodes);
    }
}
