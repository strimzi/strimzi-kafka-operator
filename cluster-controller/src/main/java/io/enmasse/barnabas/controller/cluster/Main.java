package io.enmasse.barnabas.controller.cluster;

import io.vertx.core.*;

public class Main {
    public static void main(String args[]) {
        try {
            Vertx vertx = Vertx.vertx();
            vertx.deployVerticle(new ClusterController());
        } catch (IllegalArgumentException e) {
            System.out.println(String.format("Unable to parse arguments: %s", e.getMessage()));
            System.exit(1);
        } catch (Exception e) {
            System.out.println("Error starting address controller1: " + e.getMessage());
            System.exit(1);
        }
    }
}
