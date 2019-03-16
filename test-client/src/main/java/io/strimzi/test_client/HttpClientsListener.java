/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test_client;

import io.strimzi.test.k8s.Exec;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * HTTP listener which can handle vert.x client messages and run proper commands on client pod
 */
public class HttpClientsListener extends AbstractVerticle {
    private HttpServer httpServer = null;
    private static final Logger LOGGER = LogManager.getLogger(HttpClientsListener.class);
    private HashMap<String, Exec> executors = new HashMap<>();

    @Override
    public void start() {
        vertx = Vertx.vertx();
        httpServer = vertx.createHttpServer();
        httpServer.requestHandler(request -> {
            switch (request.method()) {
                case POST:
                    postHandler(request);
                    break;
                case GET:
                    getHandler(request);
                    break;
                case DELETE:
                    deleteHandler(request);
                    break;
            }
        });
        int port = 4242;
        httpServer.listen(port);
        LOGGER.info("Client listener listening on port: {}", port);
    }

    private void deleteHandler(HttpServerRequest request) {
        request.bodyHandler(handler -> {
            JsonObject json = handler.toJsonObject();
            LOGGER.info("Incoming DELETE request: {}", json);
            String clientUUID = json.getString("id");

            Exec executor = executors.get(clientUUID);
            executor.stop();
            executors.remove(clientUUID);

            HttpServerResponse response = successfulResponse(request);
            JsonObject responseData = new JsonObject();
            responseData.put("ecode", executor.getRetCode());
            responseData.put("stdOut", executor.getStdOut());
            responseData.put("stdErr", executor.getStdErr());
            responseData.put("isRunning", executor.isRunning());
            response.end(responseData.toString());

        });
    }

    private void getHandler(HttpServerRequest request) {
        LOGGER.info(executors);
        request.bodyHandler(handler -> {
            JsonObject json = handler.toJsonObject();
            LOGGER.info("Incoming GET request: {}", json);
            String clientUUID = json.getString("id");

            Exec executor = executors.get(clientUUID);

            HttpServerResponse response = successfulResponse(request);
            JsonObject responseData = new JsonObject();

            responseData.put("ecode", executor.getRetCode());
            responseData.put("stdOut", executor.getStdOut());
            responseData.put("stdErr", executor.getStdErr());
            responseData.put("isRunning", executor.isRunning());
            response.end(responseData.toString());
        });
    }

    private void postHandler(HttpServerRequest request) {
        request.bodyHandler(handler -> {
            JsonObject json = handler.toJsonObject();
            LOGGER.info("Incoming POST request: {}", json);
            Exec executor = new Exec(Paths.get("/opt/logs/"));
            UUID uuid = UUID.randomUUID();

            JsonArray command = json.getJsonArray("command");
            int count = json.getInteger("count");

            JsonArray clientsIDs = new JsonArray();
            for (int i = 0; i < count; i++) {
                try {
                    CompletableFuture.runAsync(() -> {
                        try {
                            LOGGER.info("Execute command: {}", command);
                            executor.execute(null, command.getList(), 0);
                        } catch (IOException | InterruptedException | ExecutionException e) {
                            e.printStackTrace();
                        }
                    }, runnable -> new Thread(runnable).start());
                } catch (Exception e) {
                    e.printStackTrace();
                }
                executors.put(uuid.toString(), executor);
                clientsIDs.add(uuid.toString());
            }

            HttpServerResponse response = successfulResponse(request);
            JsonObject responseData = new JsonObject();
            responseData.put("clients", clientsIDs);
            response.end(responseData.toString());
        });
    }

    private HttpServerResponse successfulResponse(HttpServerRequest request) {
        HttpServerResponse response = request.response();
        response.setStatusCode(200);
        response.headers().add("Content-Type", "application/json");
        return response;
    }
}
