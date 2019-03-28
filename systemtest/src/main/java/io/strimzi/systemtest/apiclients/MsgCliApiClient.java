/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.apiclients;

import io.strimzi.systemtest.kafkaclients.AbstractClient;
import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.net.HttpURLConnection.HTTP_OK;

public class MsgCliApiClient {
    private static final Logger LOGGER = LogManager.getLogger(MsgCliApiClient.class);
    private WebClient webClient;
    private URL endpoint;
    private Vertx vertx;

    public MsgCliApiClient(URL endpoint) {
        this.endpoint = endpoint;
        VertxOptions options = new VertxOptions()
                .setWorkerPoolSize(1)
                .setInternalBlockingPoolSize(1)
                .setEventLoopPoolSize(1);
        this.vertx = Vertx.vertx(options);
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
     * Start new messaging webClient(s)
     *
     * @param clientArguments list of arguments for webClient (together with webClient name!)
     * @param count           count of clients that will be started
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws TimeoutException
     */
    public JsonObject startClients(List<String> clientArguments, int count) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<JsonObject> responsePromise = new CompletableFuture<>();
        JsonObject request = new JsonObject();
        request.put("command", new JsonArray(clientArguments));
        request.put("count", count);

        webClient.post(endpoint.getPort(), endpoint.getHost(), "")
                .as(BodyCodec.jsonObject())
                .timeout(120_000)
                .sendJson(request,
                    ar -> responseHandler(ar, responsePromise, HttpURLConnection.HTTP_OK, "Error starting messaging clients"));
        return responsePromise.get(150_000, TimeUnit.SECONDS);

    }

    /**
     * Get all info about messaging webClient (stdOut, stdErr, code, isRunning)
     *
     * @param uuid webClient id
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws TimeoutException
     */
    public JsonObject getClientInfo(String uuid) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<JsonObject> responsePromise = new CompletableFuture<>();
        JsonObject request = new JsonObject();
        request.put("id", uuid);

        webClient.get(endpoint.getPort(), endpoint.getHost(), "")
                .as(BodyCodec.jsonObject())
                .timeout(120000)
                .sendJson(request,
                    ar -> responseHandler(ar, responsePromise, HttpURLConnection.HTTP_OK, "Error getting messaging clients info"));
        return responsePromise.get(150000, TimeUnit.SECONDS);

    }

    /**
     * Stop messaging webClient and get all informations about them (stdOut, stdErr, code, isRunning)
     *
     * @param uuid webClient id
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws TimeoutException
     */
    public JsonObject stopClient(String uuid) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<JsonObject> responsePromise = new CompletableFuture<>();
        JsonObject request = new JsonObject();
        request.put("id", uuid);

        webClient.delete(endpoint.getPort(), endpoint.getHost(), "")
                .as(BodyCodec.jsonObject())
                .timeout(120000)
                .sendJson(request,
                    ar -> responseHandler(ar, responsePromise, HttpURLConnection.HTTP_OK, "Error removing messaging clients"));
        return responsePromise.get(150000, TimeUnit.SECONDS);
    }

    /***
     * Send request with one webClient and receive result
     * @param client
     * @return result of webClient
     */
    public JsonObject sendAndGetStatus(AbstractClient client) throws InterruptedException, ExecutionException, TimeoutException {
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
     * Send request with one webClient and receive id
     * @param client
     * @return id of webClient
     */
    public String sendAndGetId(AbstractClient client) throws Exception {
        List<String> apiArgument = new LinkedList<>();
        apiArgument.add(client.getExecutable());
        apiArgument.addAll(client.getArguments());

        JsonObject response = startClients(apiArgument, 1);
        LOGGER.info(response.toString());

        JsonArray ids = response.getJsonArray("clients");
        return ids.getString(0);
    }
}
