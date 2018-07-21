/*
 * Copyright 2017-2018, Strimzi authors.
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

        RackWriterConfig config = RackWriterConfig.fromMap(System.getenv());
        KubernetesClient client = new DefaultKubernetesClient();

        log.info("Init-kafka started with config: {}", config);

        RackWriter writer = new RackWriter(client);
        if (!writer.write(config)) {
            System.exit(1);
        }

        client.close();
    }
}
