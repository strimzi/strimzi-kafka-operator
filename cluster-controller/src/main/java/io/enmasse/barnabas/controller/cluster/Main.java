package io.enmasse.barnabas.controller.cluster;

import io.vertx.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class.getName());

    public static void main(String args[]) {
        String namespace = "myproject";
        Map<String, String> labels = new HashMap<>();
        labels.put("app", "barnabas");
        labels.put("type", "deployment");
        //labels.put("kind", "kafka");

        try {
            Vertx vertx = Vertx.vertx();
            vertx.deployVerticle(new ClusterController(new ClusterControllerConfig(namespace, labels)));
        } catch (IllegalArgumentException e) {
            log.error("Unable to parse arguments", e);
            System.exit(1);
        } catch (Exception e) {
            log.error("Error starting cluster controller:", e);
            System.exit(1);
        }
    }
}
