/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.operator.common.ReconciliationLogger;
import io.vertx.core.Vertx;

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

    private final static ReconciliationLogger LOGGER = ReconciliationLogger.create(Main.class);

    public static void main(String[] args) {
        LOGGER.infoOp("TopicOperator {} is starting", Main.class.getPackage().getImplementationVersion());
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
                LOGGER.infoOp("Session deployed");
            } else {
                LOGGER.errorOp("Error deploying Session", ar.cause());
            }
        });
    }
}
