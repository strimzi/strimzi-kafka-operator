/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.operator.assembly;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.strimzi.api.kafka.model.connect.ConnectorPlugin;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.vertx.core.json.JsonObject;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static java.util.Arrays.asList;

class KafkaConnectApiImpl implements KafkaConnectApi {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaConnectApiImpl.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final TypeReference<Map<String, Object>> TREE_TYPE = new TypeReference<>() { };
    public static final TypeReference<Map<String, String>> MAP_OF_STRINGS = new TypeReference<>() { };
    public static final TypeReference<Map<String, Map<String, String>>> MAP_OF_MAP_OF_STRINGS = new TypeReference<>() { };
    public static final TypeReference<Map<String, Map<String, List<String>>>> MAP_OF_MAP_OF_LIST_OF_STRING = new TypeReference<>() { };
    private final HttpClient httpClient;

    public KafkaConnectApiImpl() {
        this.httpClient = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
    }

    @Override
    public CompletableFuture<Map<String, Object>> createOrUpdatePutRequest(
            Reconciliation reconciliation,
            String host, int port,
            String connectorName, JsonObject configJson) {
        String data = configJson.toString();
        String path = "/connectors/" + connectorName + "/config";
        LOGGER.debugCr(reconciliation, "Making PUT request to {} with body {}", path, configJson);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(String.format("http://%s:%d%s", host, port, encodeURLString(path))))
                .PUT(HttpRequest.BodyPublishers.ofString(data))
                .setHeader("Accept", "application/json")
                .setHeader("Content-Type", "application/json")
                .build();

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenCompose(response -> {
                    int statusCode = response.statusCode();
                    if (statusCode == 200 || statusCode == 201) {
                        JsonNode json = parseToJsonNode(response);
                        Map<String, Object> t = OBJECT_MAPPER.convertValue(json, TREE_TYPE);
                        LOGGER.debugCr(reconciliation, "Got {} response to PUT request to {}: {}", statusCode, path, t);
                        return CompletableFuture.completedFuture(t);
                    } else if (statusCode == 409) {
                        return withBackoff(reconciliation, new BackOff(200L, 2, 10), connectorName, Collections.singleton(409),
                                () -> createOrUpdatePutRequest(reconciliation, host, port, connectorName, configJson), "create/update");
                    } else {
                        LOGGER.debugCr(reconciliation, "Got {} response to PUT request to {}", statusCode, path);
                        return CompletableFuture.failedFuture(new ConnectRestException(response, tryToExtractErrorMessage(reconciliation, response.body())));
                    }
                });
    }

    @Override
    public CompletableFuture<Map<String, Object>> getConnector(
            Reconciliation reconciliation,
            String host, int port,
            String connectorName) {
        return doGet(reconciliation, host, port, String.format("/connectors/%s", connectorName),
                new HashSet<>(asList(200, 201)),
                TREE_TYPE);
    }

    private JsonNode parseToJsonNode(HttpResponse<String> responseBody) {
        JsonNode json;
        try {
            json = OBJECT_MAPPER.readTree(responseBody.body());
        } catch (JsonProcessingException e) {
            throw new ConnectRestException(responseBody, "Could not deserialize response: " + e);
        }
        return json;
    }

    // Encodes the invalid characters for a URL because the connector name may contain them
    private String encodeURLString(String toEncode) {
        return toEncode.replace("->", "%2D%3E");
    }


    private <T> CompletableFuture<T> doGet(Reconciliation reconciliation, String host, int port, String path, Set<Integer> okStatusCodes, TypeReference<T> type) {
        LOGGER.debugCr(reconciliation, "Making GET request to {}", path);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(String.format("http://%s:%d%s", host, port, encodeURLString(path))))
                .GET()
                .setHeader("Accept", "application/json")
                .build();

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenCompose(response -> {
                    int statusCode = response.statusCode();
                    if (okStatusCodes.contains(statusCode)) {
                        JsonNode json = parseToJsonNode(response);
                        LOGGER.debugCr(reconciliation, "Got {} response to GET request to {}: {}", statusCode, path, json.asText());
                        return CompletableFuture.completedFuture(OBJECT_MAPPER.convertValue(json, type));
                    } else {
                        LOGGER.debugCr(reconciliation, "Got {} response to GET request to {}", statusCode, path);
                        return CompletableFuture.failedFuture(new ConnectRestException(response, tryToExtractErrorMessage(reconciliation, response.body())));
                    }
                });
    }

    @Override
    public CompletableFuture<Map<String, String>> getConnectorConfig(
            Reconciliation reconciliation,
            String host, int port,
            String connectorName) {
        return doGet(reconciliation, host, port, String.format("/connectors/%s/config", connectorName),
                new HashSet<>(asList(200, 201)),
                MAP_OF_STRINGS);
    }

    @Override
    public CompletableFuture<Map<String, String>> getConnectorConfig(Reconciliation reconciliation, BackOff backOff, String host, int port, String connectorName) {
        return withBackoff(reconciliation, backOff, connectorName, Collections.singleton(409),
            () -> getConnectorConfig(reconciliation, host, port, connectorName), "config");
    }

    @Override
    public CompletableFuture<Void> delete(Reconciliation reconciliation, String host, int port, String connectorName) {
        String path = "/connectors/" + connectorName;
        LOGGER.debugCr(reconciliation, "Making DELETE request to {}", path);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(String.format("http://%s:%d%s", host, port, encodeURLString(path))))
                .DELETE()
                .setHeader("Accept", "application/json")
                .setHeader("Content-Type", "application/json")
                .build();

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenCompose(response -> {
                    int statusCode = response.statusCode();
                    if (statusCode == 204) {
                        LOGGER.debugCr(reconciliation, "Connector was deleted. Waiting for status deletion!");
                        return withBackoff(reconciliation, new BackOff(200L, 2, 10), connectorName, Collections.singleton(200),
                                () -> status(reconciliation, host, port, connectorName, Collections.singleton(404)), "status").thenApply(r -> null);
                    } else if (statusCode == 409) {
                        return withBackoff(reconciliation, new BackOff(200L, 2, 10), connectorName, Collections.singleton(409),
                                () -> delete(reconciliation, host, port, connectorName), "delete").thenApply(r -> null);
                    } else {
                        LOGGER.debugCr(reconciliation, "Got {} response to PUT request to {}", statusCode, path);
                        return CompletableFuture.failedFuture(new ConnectRestException(response, tryToExtractErrorMessage(reconciliation, response.body())));
                    }
                });
    }

    @Override
    public CompletableFuture<Map<String, Object>> statusWithBackOff(Reconciliation reconciliation, BackOff backOff, String host, int port, String connectorName) {
        return withBackoff(reconciliation, backOff, connectorName, Collections.singleton(404),
            () -> status(reconciliation, host, port, connectorName), "status");
    }

    private <T> CompletableFuture<T> withBackoff(Reconciliation reconciliation,
                                                 BackOff backOff, String connectorName,
                                                 Set<Integer> retriableStatusCodes,
                                                 Supplier<CompletableFuture<T>> supplier,
                                                 String attribute) {
        CompletableFuture<T> statusFuture = new CompletableFuture<>();
        ScheduledExecutorService singleExecutor = Executors.newSingleThreadScheduledExecutor(
                runnable -> new Thread(runnable, "kafka-connect-" + connectorName + "-" + attribute));

        executeBackOffInternal(reconciliation, backOff, connectorName, retriableStatusCodes, singleExecutor, statusFuture, supplier, attribute, 0);
        statusFuture.whenComplete((r, e) -> singleExecutor.shutdown());
        return statusFuture;
    }

    private <T> void executeBackOffInternal(Reconciliation reconciliation,
                                                 BackOff backOff, String connectorName,
                                                 Set<Integer> retriableStatusCodes,
                                                 ScheduledExecutorService singleExecutor,
                                                 CompletableFuture<T> statusFuture,
                                                 Supplier<CompletableFuture<T>> supplier,
                                                 String attribute, long delay) {
        singleExecutor.schedule(() -> {
            supplier.get().whenComplete((result, error) -> {
                if (error == null) {
                    statusFuture.complete(result);
                } else {
                    Throwable cause = error.getCause();
                    if (cause instanceof ConnectRestException
                            && retriableStatusCodes.contains(((ConnectRestException) cause).getStatusCode())) {
                        if (backOff.done()) {
                            LOGGER.debugCr(reconciliation, "Connector {} {} returned HTTP {} and we run out of back off time", connectorName, attribute, ((ConnectRestException) cause).getStatusCode());
                            statusFuture.completeExceptionally(cause);
                        } else {
                            LOGGER.debugCr(reconciliation, "Connector {} {} returned HTTP {} - backing off", connectorName, attribute, ((ConnectRestException) cause).getStatusCode());
                            long delay1 = backOff.delayMs();

                            LOGGER.debugCr(reconciliation, "Status for connector {} not found; " +
                                            "backing off for {}ms (cumulative {}ms)",
                                    connectorName, delay, backOff.cumulativeDelayMs());
                            executeBackOffInternal(reconciliation, backOff, connectorName, retriableStatusCodes, singleExecutor, statusFuture, supplier, attribute, delay1);
                        }
                    } else {
                        statusFuture.completeExceptionally(cause);
                    }
                }
            });
        }, delay, TimeUnit.MILLISECONDS);
    }

    @Override
    public CompletableFuture<Map<String, Object>> status(Reconciliation reconciliation, String host, int port, String connectorName) {
        return status(reconciliation, host, port, connectorName, Collections.singleton(200));
    }

    @Override
    public CompletableFuture<Map<String, Object>> status(Reconciliation reconciliation, String host, int port, String connectorName, Set<Integer> okStatusCodes) {
        String path = "/connectors/" + connectorName + "/status";
        return doGet(reconciliation, host, port, path, okStatusCodes, TREE_TYPE);
    }

    @Override
    public CompletableFuture<Void> pause(Reconciliation reconciliation, String host, int port, String connectorName) {
        return updateState(reconciliation, host, port, "/connectors/" + connectorName + "/pause", 202);
    }

    @Override
    public CompletableFuture<Void> stop(Reconciliation reconciliation, String host, int port, String connectorName) {
        return updateState(reconciliation, host, port, "/connectors/" + connectorName + "/stop", 204);
    }

    @Override
    public CompletableFuture<Void> resume(Reconciliation reconciliation, String host, int port, String connectorName) {
        return updateState(reconciliation, host, port, "/connectors/" + connectorName + "/resume", 202);
    }

    private CompletableFuture<Void> updateState(Reconciliation reconciliation, String host, int port, String path, int expectedStatusCode) {
        LOGGER.debugCr(reconciliation, "Making PUT request to {} ", path);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(String.format("http://%s:%d%s", host, port, encodeURLString(path))))
                .PUT(HttpRequest.BodyPublishers.noBody())
                .setHeader("Accept", "application/json")
                .build();

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenCompose(response -> {
                    if (response.statusCode() == expectedStatusCode) {
                        return CompletableFuture.completedFuture(null);
                    } else {
                        return CompletableFuture.failedFuture(new ConnectRestException(response, "Unexpected status code"));
                    }
                });
    }

    @Override
    public CompletableFuture<List<String>> list(Reconciliation reconciliation, String host, int port) {
        String path = "/connectors";
        LOGGER.debugCr(reconciliation, "Making GET request to {} ", path);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(String.format("http://%s:%d%s", host, port, path)))
                .GET()
                .setHeader("Accept", "application/json")
                .build();

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenCompose(response -> {
                    if (response.statusCode() == 200) {
                        JsonNode json = parseToJsonNode(response);
                        if (!json.isArray()) {
                            return CompletableFuture.failedFuture(new ConnectRestException(response, "Response body is not a JSON array"));
                        }

                        ArrayNode objects = (ArrayNode) json;
                        List<String> list = new ArrayList<>(objects.size());

                        for (Object o : objects) {
                            if (o instanceof TextNode) {
                                list.add(((TextNode) o).asText());
                            } else {
                                return CompletableFuture.failedFuture(new ConnectRestException(response, o == null ? "null" : o.getClass().getName()));
                            }
                        }
                        return CompletableFuture.completedFuture(list);
                    } else {
                        return CompletableFuture.failedFuture(new ConnectRestException(response, "Unexpected status code"));
                    }
                });
    }

    @Override
    public CompletableFuture<List<ConnectorPlugin>> listConnectorPlugins(Reconciliation reconciliation, String host, int port) {
        String path = "/connector-plugins";
        LOGGER.debugCr(reconciliation, "Making GET request to {}", path);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(String.format("http://%s:%d%s", host, port, path)))
                .GET()
                .setHeader("Accept", "application/json")
                .build();

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenCompose(response -> {
                    int statusCode = response.statusCode();
                    if (statusCode == 200) {
                        try {
                            LOGGER.debugCr(reconciliation, "Got {} response to GET request to {}", statusCode);
                            return CompletableFuture.completedFuture(asList(OBJECT_MAPPER.readValue(response.body().getBytes(StandardCharsets.UTF_8), ConnectorPlugin[].class)));
                        } catch (IOException e) {
                            LOGGER.warnCr(reconciliation, "Failed to parse list of connector plugins", e);
                            return CompletableFuture.failedFuture(new ConnectRestException(response, "Failed to parse list of connector plugins", e));
                        }
                    } else {
                        return CompletableFuture.failedFuture(new ConnectRestException(response, "Unexpected status code"));
                    }
                });
    }

    private CompletableFuture<Void> updateConnectorLogger(Reconciliation reconciliation, String host, int port, String logger, String level) {
        String path = "/admin/loggers/" + logger + "?scope=cluster";

        ObjectNode levelJO = OBJECT_MAPPER.createObjectNode();
        levelJO.put("level", level);
        String data;
        try {
            data = OBJECT_MAPPER.writeValueAsString(levelJO);
        } catch (JsonProcessingException e) {
            return CompletableFuture.failedFuture(new RuntimeException("Could not deserialize the request data for updating logger " + logger + ": " + e));
        }

        LOGGER.debugCr(reconciliation, "Making PUT request to {} with body {}", path, levelJO);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(String.format("http://%s:%d%s", host, port, path)))
                .PUT(HttpRequest.BodyPublishers.ofString(data))
                .setHeader("Accept", "application/json")
                .setHeader("Content-Type", "application/json")
                .build();

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenCompose(response -> {
                    int statusCode = response.statusCode();
                    if (List.of(200, 204).contains(statusCode)) {
                        LOGGER.debugCr(reconciliation, "Logger {} updated to level {}", logger, level);
                        return CompletableFuture.completedFuture(null);
                    } else {
                        LOGGER.debugCr(reconciliation, "Logger {} did not update to level {} (http code {})", logger, level, statusCode);
                        return CompletableFuture.failedFuture(new ConnectRestException(response, "Unexpected status code"));
                    }
                });
    }

    @Override
    public CompletableFuture<Map<String, String>> listConnectLoggers(Reconciliation reconciliation, String host, int port) {
        String path = "/admin/loggers/";
        LOGGER.debugCr(reconciliation, "Making GET request to {}", path);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(String.format("http://%s:%d%s", host, port, path)))
                .GET()
                .setHeader("Accept", "application/json")
                .build();

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenCompose(response -> {
                    int statusCode = response.statusCode();
                    if (statusCode == 200) {
                        try {
                            LOGGER.debugCr(reconciliation, "Got {} response to GET request to {}", statusCode, path);
                            Map<String, Map<String, String>> fetchedLoggers = OBJECT_MAPPER.readValue(response.body().getBytes(StandardCharsets.UTF_8), MAP_OF_MAP_OF_STRINGS);
                            Map<String, String> loggerMap = new HashMap<>(fetchedLoggers.size());
                            for (var loggerEntry : fetchedLoggers.entrySet()) {
                                String level = loggerEntry.getValue().get("level");
                                if (level != null) {
                                    loggerMap.put(loggerEntry.getKey(), level);
                                }
                            }
                            return CompletableFuture.completedFuture(loggerMap);
                        } catch (IOException e) {
                            LOGGER.warnCr(reconciliation, "Failed to get list of connector loggers", e);
                            return CompletableFuture.failedFuture(new ConnectRestException(response, "Failed to get connector loggers", e));
                        }
                    } else {
                        return CompletableFuture.failedFuture(new ConnectRestException(response, "Unexpected status code"));
                    }
                });
    }

    /**
     * Parses logger level from couple LEVEL, APPENDER
     * @param couple tested input
     * @return logger Level
     */
    private String getLoggerLevelFromAppenderCouple(String couple) {
        int index = couple.indexOf(",");
        if (index > 0) {
            return couple.substring(0, index).trim();
        } else {
            return couple.trim();
        }
    }

    @Override
    public CompletableFuture<Map<String, Object>> restart(String host, int port, String connectorName, boolean includeTasks, boolean onlyFailed) {
        return restartConnectorOrTask(host, port, "/connectors/" + connectorName + "/restart?includeTasks=" + includeTasks + "&onlyFailed=" + onlyFailed);
    }

    @Override
    public CompletableFuture<Void> restartTask(String host, int port, String connectorName, int taskID) {
        return restartConnectorOrTask(host, port, "/connectors/" + connectorName + "/tasks/" + taskID + "/restart")
            .thenCompose(result -> CompletableFuture.completedFuture(null));
    }

    private CompletableFuture<Map<String, Object>> restartConnectorOrTask(String host, int port, String path) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(String.format("http://%s:%d%s", host, port, encodeURLString(path))))
                .POST(HttpRequest.BodyPublishers.noBody())
                .setHeader("Accept", "application/json")
                .build();

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenCompose(response -> {
                    if (response.statusCode() == 202) {
                        try {
                            return CompletableFuture.completedFuture(OBJECT_MAPPER.readValue(response.body().getBytes(StandardCharsets.UTF_8), TREE_TYPE));
                        } catch (IOException e) {
                            return CompletableFuture.failedFuture(new ConnectRestException(response, "Failed to parse restart status response", e));
                        }
                    } else if (response.statusCode() == 204) {
                        return CompletableFuture.completedFuture(null);
                    } else {
                        return CompletableFuture.failedFuture(new ConnectRestException(response, "Unexpected status code"));
                    }
                });
    }

    @Override
    public CompletableFuture<List<String>> getConnectorTopics(Reconciliation reconciliation, String host, int port, String connectorName) {
        String path = String.format("/connectors/%s/topics", connectorName);
        LOGGER.debugCr(reconciliation, "Making GET request to {}", path);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(String.format("http://%s:%d%s", host, port, encodeURLString(path))))
                .GET()
                .setHeader("Accept", "application/json")
                .build();

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenCompose(response -> {
                    int statusCode = response.statusCode();
                    if (statusCode == 200) {
                        try {
                            Map<String, Map<String, List<String>>> t = OBJECT_MAPPER.readValue(response.body().getBytes(StandardCharsets.UTF_8), MAP_OF_MAP_OF_LIST_OF_STRING);
                            LOGGER.debugCr(reconciliation, "Got {} response to GET request to {}: {}", statusCode, path, t);
                            return CompletableFuture.completedFuture(t.get(connectorName).get("topics"));
                        } catch (IOException e) {
                            LOGGER.warnCr(reconciliation, "Failed to parse list of connector topics", e);
                            return CompletableFuture.failedFuture(new ConnectRestException(response, "Failed to parse list of connector topics", e));
                        }
                    } else {
                        return CompletableFuture.failedFuture(new ConnectRestException(response, "Unexpected status code"));
                    }
                });
    }

    @Override
    public CompletableFuture<String> getConnectorOffsets(Reconciliation reconciliation, String host, int port, String connectorName) {
        String path = String.format("/connectors/%s/offsets", connectorName);
        LOGGER.debugCr(reconciliation, "Making GET request to {}", path);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(String.format("http://%s:%d%s", host, port, encodeURLString(path))))
                .GET()
                .setHeader("Accept", "application/json")
                .setHeader("Content-Type", "application/json")
                .build();

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenCompose(response -> {
                    int statusCode = response.statusCode();
                    if (statusCode == 200) {
                        try {
                            Object offsets = OBJECT_MAPPER.readValue(response.body().getBytes(StandardCharsets.UTF_8), Object.class);
                            String prettyPrintedOffsets = OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(offsets);
                            LOGGER.debugCr(reconciliation, "Got {} response to GET request to {}: {}", statusCode, path, offsets);
                            return CompletableFuture.completedFuture(prettyPrintedOffsets);
                        } catch (IOException e) {
                            LOGGER.warnCr(reconciliation, "Failed to parse connector offsets", e);
                            return CompletableFuture.failedFuture(new ConnectRestException(response, "Failed to parse connector offsets", e));
                        }
                    } else {
                        return CompletableFuture.failedFuture(new ConnectRestException(response, "Unexpected status code"));
                    }
                });
    }

    @Override
    public CompletableFuture<Void> alterConnectorOffsets(Reconciliation reconciliation, String host, int port, String connectorName, String newOffsets) {
        String path = String.format("/connectors/%s/offsets", connectorName);
        LOGGER.debugCr(reconciliation, "Making PATCH request to {}", path);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(String.format("http://%s:%d%s", host, port, encodeURLString(path))))
                .method("PATCH", HttpRequest.BodyPublishers.ofString(newOffsets))
                .setHeader("Accept", "application/json")
                .setHeader("Content-Type", "application/json")
                .build();

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenCompose(response -> {
                    int statusCode = response.statusCode();
                    if (statusCode == 200) {
                        try {
                            Map<String, String> body = OBJECT_MAPPER.readValue(response.body().getBytes(StandardCharsets.UTF_8), MAP_OF_STRINGS);
                            String message = body.get("message");
                            LOGGER.debugCr(reconciliation, "Got {} response to PATCH request to {}: {}", statusCode, path, message);
                            return CompletableFuture.completedFuture(null);
                        } catch (IOException e) {
                            LOGGER.warnCr(reconciliation, "Failed to parse connector offsets alter response", e);
                            return CompletableFuture.failedFuture(new ConnectRestException(response, "Failed to parse connector offsets alter response", e));
                        }
                    } else {
                        return CompletableFuture.failedFuture(new ConnectRestException(response, "Unexpected status code"));
                    }
                });
    }

    @Override
    public CompletableFuture<Void> resetConnectorOffsets(Reconciliation reconciliation, String host, int port, String connectorName) {
        String path = String.format("/connectors/%s/offsets", connectorName);
        LOGGER.debugCr(reconciliation, "Making DELETE request to {}", path);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(String.format("http://%s:%d%s", host, port, encodeURLString(path))))
                .DELETE()
                .setHeader("Accept", "application/json")
                .build();

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenCompose(response -> {
                    int statusCode = response.statusCode();
                    if (statusCode == 200) {
                        try {
                            Map<String, String> body = OBJECT_MAPPER.readValue(response.body().getBytes(StandardCharsets.UTF_8), MAP_OF_STRINGS);
                            String message = body.get("message");
                            LOGGER.debugCr(reconciliation, "Got {} response to DELETE request to {}: {}", statusCode, path, message);
                            return CompletableFuture.completedFuture(null);
                        } catch (IOException e) {
                            LOGGER.warnCr(reconciliation, "Failed to parse connector offsets reset response", e);
                            return CompletableFuture.failedFuture(new ConnectRestException(response, "Failed to parse connector offsets reset response", e));
                        }
                    } else {
                        return CompletableFuture.failedFuture(new ConnectRestException(response, "Unexpected status code"));
                    }
                });
    }

    /* test */ static String tryToExtractErrorMessage(Reconciliation reconciliation, String body) {
        JsonNode json;
        try {
            json = OBJECT_MAPPER.readTree(body);
            if (json.has("message")) {
                return json.get("message").asText();
            } else {
                LOGGER.warnCr(reconciliation, "Failed to decode the error message from the response: " + body);
            }
        } catch (JsonProcessingException e) {
            LOGGER.warnCr(reconciliation, "Failed to deserialize the error message from the response: " + body);
        }
        return "Unknown error message";
    }
}
