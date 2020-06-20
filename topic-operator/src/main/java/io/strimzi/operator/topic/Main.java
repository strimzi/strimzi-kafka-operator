/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.operator.common.Util;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.util.HashMap;
import java.util.Map;
import io.vertx.core.VertxOptions;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;

/**
 * The entry-point to the topic operator.
 * Main responsibility is to deploy a {@link Session} with an appropriate Config and KubeClient,
 * redeploying if the config changes.
 */
public class Main {

    private final static Logger LOGGER = LogManager.getLogger(Main.class);

    public static void main(String[] args) {
        LOGGER.info("TopicOperator {} is starting", Main.class.getPackage().getImplementationVersion());
        Main main = new Main();
        main.run();
    }

    public void run() {
        Map<String, String> m = new HashMap<>(System.getenv());
        m.keySet().retainAll(Config.keyNames());
        Config config = new Config(m);
        deploy(config);
    }

    private void deploy(Config config) {
        // Workaround for https://github.com/fabric8io/kubernetes-client/issues/2212
        // Can be removed after upgrade to Fabric8 4.10.2 or higher or to Java 11
        if (Util.shouldDisableHttp2()) {
            System.setProperty("http2.disable", "true");
        }

        DefaultKubernetesClient kubeClient = new DefaultKubernetesClient();
        Crds.registerCustomKinds();
        VertxOptions options = new VertxOptions().setMetricsOptions(
                new MicrometerMetricsOptions()
                        .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true))
                        .setJvmMetricsEnabled(true)
                        .setEnabled(true));
        Vertx vertx = Vertx.vertx(options);
        Session session = new Session(kubeClient, config);
        vertx.deployVerticle(session, ar -> {
            if (ar.succeeded()) {
                LOGGER.info("Session deployed");
            } else {
                LOGGER.error("Error deploying Session", ar.cause());
            }
        });
    }
}
