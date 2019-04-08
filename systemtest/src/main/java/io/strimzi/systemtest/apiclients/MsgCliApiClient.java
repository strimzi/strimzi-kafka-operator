/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.apiclients;

import io.strimzi.systemtest.kafkaclients.AbstractClient;
import io.strimzi.test.TestUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.net.HttpURLConnection.HTTP_OK;

/**
 * API for communication with kafka clients deployed as a Deployment inside Kubernetes
 */
public class MsgCliApiClient {
    private static final Logger LOGGER = LogManager.getLogger(MsgCliApiClient.class);
    private WebClient webClient;
    private URL endpoint;
    private Vertx vertx;
    private final int initRetry = 10;

    /**
     * Constructor for api clients
     * @param endpoint URL on which deployed clients listening
     */
    public MsgCliApiClient(URL endpoint) {
        this.endpoint = endpoint;
        this.vertx = Vertx.vertx();
        this.connect();
    }

    private String apiClientName() {
        return "Kafka Clients";
    }

    protected void connect() {
        this.webClient = WebClient.create(vertx, new WebClientOptions()
                .setSsl(false)
                .setTrustAll(true)
                .setVerifyHost(false));
    }

    protected <T> T doRequestNTimes(int retry, Callable<T> fn, Optional<Runnable> reconnect) throws Exception {
        return TestUtils.doRequestNTimes(retry, fn, reconnect);
    }

    private <T> void responseHandler(AsyncResult<HttpResponse<T>> ar, CompletableFuture<T> promise, int expectedCode,
                                     String warnMessage) {
        try {
            if (ar.succeeded()) {
                HttpResponse<T> response = ar.result();
                T body = response.body();
                if (response.statusCode() != expectedCode) {
                    LOGGER.error("expected-code: {}, response-code: {}, body: {}", expectedCode, response.statusCode(), response.body());
                    promise.completeExceptionally(new RuntimeException("Status " + response.statusCode() + " body: " + (body != null ? body.toString() : null)));
                } else if (response.statusCode() < HTTP_OK || response.statusCode() >= HttpURLConnection.HTTP_MULT_CHOICE) {
                    promise.completeExceptionally(new RuntimeException(body.toString()));
                } else {
                    promise.complete(ar.result().body());
                }
            } else {
                LOGGER.warn(warnMessage);
                promise.completeExceptionally(ar.cause());
            }
        } catch (io.vertx.core.json.DecodeException decEx) {
            if (ar.result().bodyAsString().contains("application is not available")) {
                LOGGER.warn("'{}' is not available.", apiClientName(), ar.cause());
                throw new IllegalStateException(String.format("'%s' is not available.", apiClientName()));
            } else {
                LOGGER.warn("Unexpected object received", ar.cause());
                throw new IllegalStateException("JsonObject expected, but following object was received: " + ar.result().bodyAsString());
            }
        }
    }

    /**
     * Start new messaging kafka client(s)
     *
     * @param clientArguments list of arguments for kafka client (together with kafka client name!)
     * @param count           count of clients that will be started
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws TimeoutException
     */
    public JsonObject startClients(List<String> clientArguments, int count) throws Exception {
        CompletableFuture<JsonObject> responsePromise = new CompletableFuture<>();
        JsonObject request = new JsonObject();
        request.put("command", new JsonArray(clientArguments));
        request.put("count", count);

        return doRequestNTimes(initRetry, () -> {
            webClient.post(endpoint.getPort(), endpoint.getHost(), "")
                    .as(BodyCodec.jsonObject())
                    .timeout(120_000)
                    .sendJson(request,
                        ar -> responseHandler(ar, responsePromise, HttpURLConnection.HTTP_OK, "Error starting messaging clients"));
            return responsePromise.get(150_000, TimeUnit.SECONDS);
        },
        Optional.empty());
    }

    /**
     * Get all info about messaging kafka client (uuid, stdOut, stdErr, code, isRunning)
     *
     * @param uuid kafka client id
     * @return client info
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws TimeoutException
     */
    public JsonObject getClientInfo(String uuid) throws Exception {
        CompletableFuture<JsonObject> responsePromise = new CompletableFuture<>();

//        Future<JsonObject> responsePromise = Future.future();

        JsonObject request = new JsonObject();
        request.put("id", uuid);

        return doRequestNTimes(initRetry, () -> {
            webClient.get(endpoint.getPort(), endpoint.getHost(), "")
                    .as(BodyCodec.jsonObject())
                    .timeout(120000)
                    .sendJson(request,
                        ar -> responseHandler(ar, responsePromise, HttpURLConnection.HTTP_OK, "Error getting messaging clients info"));
            return responsePromise.get(150000, TimeUnit.SECONDS);
        },
        Optional.empty());

    }

    /**
     * Stop messaging kafka client and get all information about them (uuid, stdOut, stdErr, code, isRunning)
     *
     * @param uuid kafka client id
     * @return cline info
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws TimeoutException
     */
    public JsonObject stopClient(String uuid) throws Exception {
        CompletableFuture<JsonObject> responsePromise = new CompletableFuture<>();
        JsonObject request = new JsonObject();
        request.put("id", uuid);

        return doRequestNTimes(initRetry, () -> {
            webClient.delete(endpoint.getPort(), endpoint.getHost(), "")
                    .as(BodyCodec.jsonObject())
                    .timeout(120000)
                    .sendJson(request,
                        ar -> responseHandler(ar, responsePromise, HttpURLConnection.HTTP_OK, "Error removing messaging clients"));
            return responsePromise.get(150000, TimeUnit.SECONDS);
        },
        Optional.empty());
    }

    /***
     * Send request with one kafka client and receive result
     * @param client kafka client
     * @return result of kafka client
     */
    public JsonObject sendAndGetStatus(AbstractClient client) throws Exception {
        List<String> apiArgument = new LinkedList<>();
        apiArgument.add(client.getExecutable());
        apiArgument.addAll(client.getArguments());

        JsonObject response = startClients(apiArgument, 1);
        JsonArray ids = response.getJsonArray("clients");
        String uuid = ids.getString(0);

        Thread.sleep(5000);

        response = getClientInfo(uuid);
        response.put("UUID", uuid);
        return response;
    }

    /***
     * Send request with one kafka client and receive id
     * @param client kafka client
     * @return id of kafka client
     */
    public String sendAndGetId(AbstractClient client) throws Exception {
        List<String> apiArgument = new LinkedList<>();
        apiArgument.add(client.getExecutable());
        apiArgument.addAll(client.getArguments());

        JsonObject response = startClients(apiArgument, 1);

        JsonArray ids = response.getJsonArray("clients");
        return ids.getString(0);
    }
}
