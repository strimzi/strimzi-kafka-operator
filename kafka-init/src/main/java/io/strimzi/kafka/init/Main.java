/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.init;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.operator.common.Util;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Main {

    private static final Logger log = LogManager.getLogger(Main.class);

    public static void main(String[] args) {

        log.info("Init-kafka {} is starting", Main.class.getPackage().getImplementationVersion());
        InitWriterConfig config = InitWriterConfig.fromMap(System.getenv());

        // Workaround for https://github.com/fabric8io/kubernetes-client/issues/2212
        // Can be removed after upgrade to Fabric8 4.10.2 or higher or to Java 11
        if (Util.shouldDisableHttp2()) {
            System.setProperty("http2.disable", "true");
        }

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
