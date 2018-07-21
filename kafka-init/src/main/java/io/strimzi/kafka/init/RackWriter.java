/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.init;

import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

public class RackWriter {

    private static final Logger log = LogManager.getLogger(RackWriter.class);

    private KubernetesClient client;

    public RackWriter(KubernetesClient client) {
        this.client = client;
    }

    /**
     * Write the rack-id
     *
     * @param config Rack Writer configuration instance
     * @return if the operation was executed successfully
     */
    public boolean write(RackWriterConfig config) {

        Map<String, String> nodeLabels = client.nodes().withName(config.getNodeName()).get().getMetadata().getLabels();
        log.info("NodeLabels = {}", nodeLabels);
        String rackId = nodeLabels.get(config.getRackTopologyKey());
        log.info("Rack: {} = {}", config.getRackTopologyKey(), rackId);

        if (rackId == null) {
            log.error("Node {} doesn't have the label {} for getting the rackid",
                    config.getNodeName(), config.getRackTopologyKey());
            return false;
        }

        PrintWriter writer = null;
        boolean isWritten;
        try {
            writer = new PrintWriter(config.getRackFolder() + "/rack.id", "UTF-8");
            writer.write(rackId);
            log.info("Rack written successfully");
            isWritten = true;
        } catch (IOException e) {
            log.error("Error writing the rackid", e);
            isWritten = false;
        } finally {
            if (writer != null)
                writer.close();
        }

        return isWritten;
    }
}
