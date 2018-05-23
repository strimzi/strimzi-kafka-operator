/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.init.kafka;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static final String RACK_FILE_PATH = "./rack/rack.id";

    public static void main(String[] args) {

        // passed by the cluster operator when creating the init container
        String rackTopologyKey = System.getenv("RACK_TOPOLOGY_KEY");
        log.info("RACK_TOPOLOGY_KEY = {}", rackTopologyKey);

        // using Downward API for getting current node
        String nodeName = System.getenv("NODE_NAME");
        log.info("NODE_NAME = {}", nodeName);

        KubernetesClient client = new DefaultKubernetesClient();

        Map<String, String> nodeLabels = client.nodes().withName(nodeName).get().getMetadata().getLabels();
        log.info("nodeLabels = {}", nodeLabels);
        String rackId = nodeLabels.get(rackTopologyKey);
        log.info("Rack: {} = {}", rackTopologyKey, rackId);

        PrintWriter writer = null;
        try {
            writer = new PrintWriter(RACK_FILE_PATH, "UTF-8");
            writer.write(rackId);
            log.info("Rack written successfully");
        } catch (IOException e) {
            log.error("Error writing the rackid", e);
        } finally {
            if (writer != null)
                writer.close();
        }

        client.close();
    }
}
