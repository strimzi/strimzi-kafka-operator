/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils;

import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.strimzi.systemtest.AbstractST;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;

import static io.strimzi.test.BaseITST.KUBE_CLIENT;
import static io.strimzi.test.TestUtils.writeFile;

public class LogCollector {
    private static final Logger LOGGER = LogManager.getLogger(AbstractST.class);

    NamespacedKubernetesClient client;
    String namespace;
    File logDir;
    File configMapDir;
    File eventsDir;

    public LogCollector(NamespacedKubernetesClient client, File logDir) {
        this.client = client;
        this.namespace = client.getNamespace();
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
        LOGGER.info("Collecting logs for pods in namespace {}", namespace);

        try {
            client.pods().inNamespace(namespace).list().getItems().forEach(pod -> {
                String podName = pod.getMetadata().getName();
                pod.getStatus().getContainerStatuses().forEach(containerStatus -> {
                    String log = client.pods().inNamespace(namespace).withName(podName).inContainer(containerStatus.getName()).getLog();
                    // Write logs from containers to files
                    writeFile(logDir + "/" + "logs-pod-" + podName + "-container-" + containerStatus.getName() + ".log", log);
                    // Describe all pods
                    String describe = KUBE_CLIENT.describe("pod", podName);
                    writeFile(logDir + "/" + "describe-pod-" + podName + "-container-" + containerStatus.getName() + ".log", describe);
                });
            });
        } catch (Exception allExceptions) {
            LOGGER.warn("Searching for logs in all pods failed! Some of the logs will not be stored.");
        }
    }

    public void collectEvents() {
        LOGGER.info("Collecting events in namespace {}", namespace);
        String events = KUBE_CLIENT.getEvents();
        // Write events to file
        writeFile(eventsDir + "/" + "events-in-namespace" + KUBE_CLIENT.namespace() + ".log", events);
    }

    public void collectConfigMaps() {
        LOGGER.info("Collecting configmaps in namespace {}", namespace);
        client.configMaps().inNamespace(namespace).list().getItems().forEach(configMap -> {
            writeFile(configMapDir + "/" + configMap.getMetadata().getName() + "-" + namespace + ".log", configMap.toString());
        });
    }
}
