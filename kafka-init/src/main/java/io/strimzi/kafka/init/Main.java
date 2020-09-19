/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.init;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Main {

    private static final Logger log = LogManager.getLogger(Main.class);

    public static void main(String[] args) {

        log.info("Init-kafka {} is starting", Main.class.getPackage().getImplementationVersion());
        InitWriterConfig config = InitWriterConfig.fromMap(System.getenv());

        KubernetesClient client = new DefaultKubernetesClient();

        log.info("Init-kafka started with config: {}", config);

        InitWriter writer = new InitWriter(client, config);

        if (config.getRackTopologyKey() != null) {
            if (!writer.writeRack()) {
                System.exit(1);
            }
        }

        if (config.isExternalAddress()) {
            if (!writer.writeExternalAddress()) {
                System.exit(1);
            }
        }

        client.close();
    }
}
