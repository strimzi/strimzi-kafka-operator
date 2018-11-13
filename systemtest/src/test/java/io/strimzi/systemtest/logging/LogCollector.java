/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.logging;

import io.fabric8.kubernetes.api.model.DoneablePod;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.strimzi.systemtest.AbstractST;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class LogCollector {

    private static final Logger LOGGER = LogManager.getLogger(AbstractST.class);
    private final Map<String, LogCollector> collectorMap = new HashMap<>();
    private final NamespacedKubernetesClient client;
    private final String namespace;

    public LogCollector(NamespacedKubernetesClient client) {
        this.client = client;
        this.namespace = client.getNamespace();
    }

    public void collectConfigMaps() {
        System.out.println("CONFIG MAPS");
    }

    public void collectLogsTerminatedPods() {
        // Get pods
//        client.pods().list().getItems().stream().forEach(p -> p.getMetadata().getName());

//        client.namespaces().l


        //        PodList pods = client.pods().list();
//
//        for (Pod pod : pods.getItems()) {
//            pod.
//        }
        System.out.println("PODS");
    }

    public void collectEvents() {
        LOGGER.info("Collecting events in namespace {}", namespace);
        client.events().list().getItems().stream().forEach(e -> {
            LOGGER.info(e.getMessage());
        });
    }
}
