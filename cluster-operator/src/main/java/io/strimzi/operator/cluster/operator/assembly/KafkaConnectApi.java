/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.strimzi.api.kafka.model.connect.ConnectorPlugin;
import io.strimzi.operator.common.model.OrderedProperties;
import io.strimzi.operator.common.BackOff;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Arrays.asList;

/**
 * A Java client for the Kafka Connect REST API.
 */
public interface KafkaConnectApi {
    /**
     * Make a {@code PUT} request to {@code /connectors/${connectorName}/config}.
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @param connectorName The name of the connector to create or update.
     * @param configJson The connectors configuration.
     * @return A Future which completes with the result of the request. If the request was successful,
     * this returns information about the connector, including its name, config and tasks.
     */
    Future<Map<String, Object>> createOrUpdatePutRequest(String host, int port, String connectorName, JsonObject configJson);

    /**
     * Make a {@code GET} request to {@code /connectors/${connectorName}/config}.
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @param connectorName The name of the connector to get the config of.
     * @return A Future which completes with the result of the request. If the request was successful,
     * this returns the connector's config.
     */
    Future<Map<String, String>> getConnectorConfig(String host, int port, String connectorName);

    public Future<Map<String, String>> getConnectorConfig(BackOff backOff, String host, int port, String connectorName);

    /**
     * Make a {@code GET} request to {@code /connectors/${connectorName}}.
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @param connectorName The name of the connector to create or update.
     * @return A Future which completes with the result of the request. If the request was successful,
     * this returns information about the connector, including its name, config and tasks.
     */
    Future<Map<String, Object>> getConnector(String host, int port, String connectorName);

    /**
     * Make a {@code DELETE} request to {@code /connectors/${connectorName}}.
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @param connectorName The name of the connector to delete.
     * @return A Future which completes with the result of the request.
     */
    Future<Void> delete(String host, int port, String connectorName);

    /**
     * Make a {@code GET} request to {@code /connectors/${connectorName}/status}.
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @param connectorName The name of the connector to get the status of.
     * @return A Future which completes with the result of the request. If the request was successful,
     * this returns the connector's status.
     */
    Future<Map<String, Object>> status(String host, int port, String connectorName);

    /**
     * Make a {@code GET} request to {@code /connectors/${connectorName}/status}, retrying according to {@code backoff}.
     * @param backOff The backoff parameters.
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @param connectorName The name of the connector to get the status of.
     * @return A Future which completes with the result of the request. If the request was successful,
     * this returns the connector's status.
     */
    Future<Map<String, Object>> statusWithBackOff(BackOff backOff, String host, int port, String connectorName);

    /**
     * Make a {@code PUT} request to {@code /connectors/${connectorName}/pause}.
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @param connectorName The name of the connector to pause.
     * @return A Future which completes with the result of the request.
     */
    Future<Void> pause(String host, int port, String connectorName);

    /**
     * Make a {@code PUT} request to {@code /connectors/${connectorName}/resume}.
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @param connectorName The name of the connector to resume.
     * @return A Future which completes with the result of the request.
     */
    Future<Void> resume(String host, int port, String connectorName);

    /**
     * Make a {@code GET} request to {@code /connectors}
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @return A Future which completes with the result of the request. If the request was successful,
     * this returns the list of connectors.
     */
    Future<List<String>> list(String host, int port);

    /**
     * Make a {@code GET} request to {@code /connector-plugins}.
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @return A Future which completes with the result of the request. If the request was successful,
     * this returns the list of connector plugins.
     */
    Future<List<ConnectorPlugin>> listConnectorPlugins(String host, int port);

    /**
     * Make a {@code GET} request to {@code /admin/loggers}.
     * @param host The host to make the request to.
     * @param port The port to make the request to.
     * @param desiredLogging Desired logging.
     * @return A Future which completes with the result of the request. If the request was successful,
     * this returns the list of connector loggers.
     */
    Future<Void> updateConnectorLoggers(String host, int port, String desiredLogging);

    Future<Map<String, Map<String, String>>> listConnectorLoggers(String host, int port);
}

class ConnectRestException extends RuntimeException {
    private final int statusCode;

    ConnectRestException(String method, String path, int statusCode, String statusMessage, String message) {
        super(method + " " + path + " returned " + statusCode + " (" + statusMessage + "): " + message);
        this.statusCode = statusCode;
    }

    public ConnectRestException(HttpClientResponse response, String message) {
        this(response.request().method().toString(), response.request().path(), response.statusCode(), response.statusMessage(), message);
    }

    ConnectRestException(String method, String path, int statusCode, String statusMessage, String message, Throwable cause) {
        super(method + " " + path + " returned " + statusCode + " (" + statusMessage + "): " + message, cause);
        this.statusCode = statusCode;
    }

    public ConnectRestException(HttpClientResponse response, String message, Throwable cause) {
        this(response.request().method().toString(), response.request().path(), response.statusCode(), response.statusMessage(), message, cause);
    }

    public int getStatusCode() {
        return statusCode;
    }
}

@SuppressWarnings({"deprecation"})
class KafkaConnectApiImpl implements KafkaConnectApi {
    private static final Logger log = LogManager.getLogger(KafkaConnectApiImpl.class);
    public static final TypeReference<Map<String, Object>> TREE_TYPE = new TypeReference<Map<String, Object>>() {
    };
    public static final TypeReference<Map<String, String>> MAP_OF_STRINGS = new TypeReference<Map<String, String>>() {
    };
    private final Vertx vertx;

    public KafkaConnectApiImpl(Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Future<Map<String, Object>> createOrUpdatePutRequest(
            String host, int port,
            String connectorName, JsonObject configJson) {
        Promise<Map<String, Object>> result = Promise.promise();
        HttpClientOptions options = new HttpClientOptions().setLogActivity(true);
        Buffer data = configJson.toBuffer();
        String path = "/connectors/" + connectorName + "/config";
        log.debug("Making PUT request to {} with body {}", path, configJson);
        vertx.createHttpClient(options)
                .put(port, host, path, response -> {
                    response.exceptionHandler(error -> {
                        result.fail(error);
                    });
                    if (response.statusCode() == 200 || response.statusCode() == 201) {
                        response.bodyHandler(buffer -> {
                            ObjectMapper mapper = new ObjectMapper();
                            try {
                                Map t = mapper.readValue(buffer.getBytes(), Map.class);
                                log.debug("Got {} response to PUT request to {}: {}", response.statusCode(), path, t);
                                result.complete(t);
                            } catch (IOException e) {
                                result.fail(new ConnectRestException(response, "Could not deserialize response: " + e));
                            }
                        });
                    } else {
                        // TODO Handle 409 (Conflict) indicating a rebalance in progress
                        log.debug("Got {} response to PUT request to {}", response.statusCode(), path);
                        response.bodyHandler(buffer -> {
                            JsonObject x = buffer.toJsonObject();
                            result.fail(new ConnectRestException(response, x.getString("message")));
                        });
                    }
                })
                .exceptionHandler(result::fail)
                .setFollowRedirects(true)
                .putHeader("Accept", "application/json")
                .putHeader("Content-Type", "application/json")
                .putHeader("Content-Length", String.valueOf(data.length()))
                .write(data)
                .end();
        return result.future();
    }

    @Override
    public Future<Map<String, Object>> getConnector(
            String host, int port,
            String connectorName) {
        return doGet(host, port, String.format("/connectors/%s", connectorName),
                new HashSet<>(asList(200, 201)),
                TREE_TYPE);
    }

    @SuppressWarnings("unchecked")
    private <T> Future<T> doGet(String host, int port, String path, Set<Integer> okStatusCodes, TypeReference<T> type) {
        Promise<T> result = Promise.promise();
        HttpClientOptions options = new HttpClientOptions().setLogActivity(true);
        log.debug("Making GET request to {}", path);
        vertx.createHttpClient(options)
                .get(port, host, path, response -> {
                    response.exceptionHandler(error -> {
                        result.fail(error);
                    });
                    if (okStatusCodes.contains(response.statusCode())) {
                        response.bodyHandler(buffer -> {
                            ObjectMapper mapper = new ObjectMapper();
                            try {
                                T t = mapper.readValue(buffer.getBytes(), type);
                                log.debug("Got {} response to GET request to {}: {}", response.statusCode(), path, t);
                                result.complete(t);
                            } catch (IOException e) {
                                result.fail(new ConnectRestException(response, "Could not deserialize response: " + e));
                            }
                        });
                    } else {
                        // TODO Handle 409 (Conflict) indicating a rebalance in progress
                        log.debug("Got {} response to GET request to {}", response.statusCode(), path);
                        response.bodyHandler(buffer -> {
                            JsonObject x = buffer.toJsonObject();
                            result.fail(new ConnectRestException(response, x.getString("message")));
                        });
                    }
                })
                .exceptionHandler(result::fail)
                .setFollowRedirects(true)
                .putHeader("Accept", "application/json")
                .end();
        return result.future();
    }

    @Override
    public Future<Map<String, String>> getConnectorConfig(
            String host, int port,
            String connectorName) {
        return doGet(host, port, String.format("/connectors/%s/config", connectorName),
                new HashSet<>(asList(200, 201)),
                MAP_OF_STRINGS);
    }

    @Override
    public Future<Map<String, String>> getConnectorConfig(BackOff backOff, String host, int port, String connectorName) {
        return withBackoff(backOff, connectorName, Collections.singleton(409),
            () -> getConnectorConfig(host, port, connectorName), "config");
    }

    @Override
    public Future<Void> delete(String host, int port, String connectorName) {
        Promise<Void> result = Promise.promise();
        HttpClientOptions options = new HttpClientOptions().setLogActivity(true);
        String path = "/connectors/" + connectorName;
        vertx.createHttpClient(options)
                .delete(port, host, path, response -> {
                    if (response.statusCode() == 204) {
                        result.complete();
                    } else {
                        // TODO Handle 409 (Conflict) indicating a rebalance in progress
                        response.bodyHandler(buffer -> {
                            JsonObject x = buffer.toJsonObject();
                            result.fail(new ConnectRestException(response, x.getString("message")));
                        });
                    }
                })
                .exceptionHandler(result::fail)
                .setFollowRedirects(true)
                .putHeader("Accept", "application/json")
                .putHeader("Content-Type", "application/json")
                .end();
        return result.future();
    }

    @Override
    public Future<Map<String, Object>> statusWithBackOff(BackOff backOff, String host, int port, String connectorName) {
        return withBackoff(backOff, connectorName, Collections.singleton(404),
            () -> status(host, port, connectorName), "status");
    }

    private <T> Future<T> withBackoff(BackOff backOff, String connectorName,
                                                    Set<Integer> retriableStatusCodes,
                                                    Supplier<Future<T>> supplier,
                                                    String attribute) {
        Promise<T> result = Promise.promise();

        Handler<Long> handler = new Handler<Long>() {
            @Override
            public void handle(Long tid) {
                supplier.get().onComplete(connectorStatus -> {
                    if (connectorStatus.succeeded()) {
                        result.complete(connectorStatus.result());
                    } else {
                        Throwable cause = connectorStatus.cause();
                        if (cause != null
                                && cause instanceof ConnectRestException
                                && retriableStatusCodes.contains(((ConnectRestException) cause).getStatusCode())) {
                            if (backOff.done()) {
                                log.debug("Connector {} {} returned HTTP {} and we run out of back off time", connectorName, attribute, ((ConnectRestException) cause).getStatusCode());
                                result.fail(cause);
                            } else {
                                log.debug("Connector {} {} returned HTTP {} - backing off", connectorName, attribute, ((ConnectRestException) cause).getStatusCode());
                                rescheduleOrComplete(tid);
                            }
                        } else {
                            result.fail(cause);
                        }
                    }
                });
            }

            void rescheduleOrComplete(Long tid) {
                if (backOff.done()) {
                    log.warn("Giving up waiting for status of connector {} after {} attempts taking {}ms",
                            connectorName,  backOff.maxAttempts(), backOff.totalDelayMs());
                } else {
                    // Schedule ourselves to run again
                    long delay = backOff.delayMs();
                    log.debug("Status for connector {} not found; " +
                                    "backing off for {}ms (cumulative {}ms)",
                            connectorName, delay, backOff.cumulativeDelayMs());
                    if (delay < 1) {
                        this.handle(tid);
                    } else {
                        vertx.setTimer(delay, this);
                    }
                }
            }
        };

        handler.handle(null);
        return result.future();
    }

    @Override
    public Future<Map<String, Object>> status(String host, int port, String connectorName) {
        String path = "/connectors/" + connectorName + "/status";
        return doGet(host, port, path, Collections.singleton(200), TREE_TYPE);
    }

    @Override
    public Future<Void> pause(String host, int port, String connectorName) {
        return pauseResume(host, port, "/connectors/" + connectorName + "/pause");
    }

    @Override
    public Future<Void> resume(String host, int port, String connectorName) {
        return pauseResume(host, port, "/connectors/" + connectorName + "/resume");
    }

    private Future<Void> pauseResume(String host, int port, String path) {
        Promise<Void> result = Promise.promise();
        HttpClientOptions options = new HttpClientOptions().setLogActivity(true);
        vertx.createHttpClient(options)
                .put(port, host, path, response -> {
                    response.exceptionHandler(error -> {
                        result.fail(error);
                    });
                    if (response.statusCode() == 202) {
                        result.complete();
                    } else {
                        result.fail("Unexpected status code " + response.statusCode()
                                + " for GET request to " + host + ":" + port + path);
                    }
                })
                .exceptionHandler(result::fail)
                .setFollowRedirects(true)
                .putHeader("Accept", "application/json")
                .end();
        return result.future();
    }

    @Override
    public Future<List<String>> list(String host, int port) {
        String path = "/connectors";
        Promise<List<String>> result = Promise.promise();
        HttpClientOptions options = new HttpClientOptions().setLogActivity(true);

        vertx.createHttpClient(options)
                .get(port, host, path, response -> {
                    response.exceptionHandler(error -> {
                        result.fail(error);
                    });
                    if (response.statusCode() == 200) {
                        response.bodyHandler(buffer -> {
                            JsonArray objects = buffer.toJsonArray();
                            List<String> list = new ArrayList<>(objects.size());
                            for (Object o : objects) {
                                if (o instanceof String) {
                                    list.add((String) o);
                                } else {
                                    result.fail(o == null ? "null" : o.getClass().getName());
                                }
                            }
                            result.complete(list);
                        });
                    } else {
                        result.fail(new ConnectRestException(response, "Unexpected status code"));
                    }
                })
                .exceptionHandler(result::fail)
                .setFollowRedirects(true)
                .putHeader("Accept", "application/json")
                .end();
        return result.future();
    }

    @Override
    public Future<List<ConnectorPlugin>> listConnectorPlugins(String host, int port) {
        Promise<List<ConnectorPlugin>> result = Promise.promise();
        HttpClientOptions options = new HttpClientOptions().setLogActivity(true);
        String path = "/connector-plugins";
        vertx.createHttpClient(options)
                .get(port, host, path, response -> {
                    response.exceptionHandler(error -> {
                        result.fail(error);
                    });
                    if (response.statusCode() == 200) {
                        response.bodyHandler(buffer -> {
                            ObjectMapper mapper = new ObjectMapper();

                            try {
                                result.complete(asList(mapper.readValue(buffer.getBytes(), ConnectorPlugin[].class)));
                            } catch (IOException e)  {
                                log.warn("Failed to parse list of connector plugins", e);
                                result.fail(new ConnectRestException(response, "Failed to parse list of connector plugins", e));
                            }
                        });
                    } else {
                        result.fail(new ConnectRestException(response, "Unexpected status code"));
                    }
                })
                .exceptionHandler(result::fail)
                .setFollowRedirects(true)
                .putHeader("Accept", "application/json")
                .end();
        return result.future();
    }

    private Future<Void> updateConnectorLogger(String host, int port, String logger, String level) {
        Promise<Void> result = Promise.promise();
        HttpClientOptions options = new HttpClientOptions().setLogActivity(true);
        String path = "/admin/loggers/" + logger;
        JsonObject levelJO = new JsonObject();
        levelJO.put("level", level);
        log.debug("Making PUT request to {} with body {}", path, levelJO);
        vertx.createHttpClient(options)
                .put(port, host, path, response -> {
                    response.exceptionHandler(error -> {
                        result.fail(error);
                    });
                    response.bodyHandler(body -> {
                    });
                    if (response.statusCode() == 200) {
                        log.debug("Logger {} updated to level {}", logger, level);
                        result.complete();
                    } else {
                        log.debug("Logger {} did not update to level {} (http code {})", logger, level, response.statusCode());
                        result.fail(new ConnectRestException(response, "Unexpected status code"));
                    }
                })
                .exceptionHandler(result::fail)
                .putHeader("Content-Type", "application/json")
                .putHeader("Content-Length", String.valueOf(levelJO.toBuffer().length()))
                .setFollowRedirects(true)
                .write(levelJO.toBuffer())
                .end();

        return result.future();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Future<Map<String, Map<String, String>>> listConnectorLoggers(String host, int port) {
        Promise<Map<String, Map<String, String>>> result = Promise.promise();
        HttpClientOptions options = new HttpClientOptions().setLogActivity(true);
        String path = "/admin/loggers/";
        vertx.createHttpClient(options)
                .get(port, host, path, response -> {
                    response.exceptionHandler(error -> {
                        result.fail(error);
                    });
                    if (response.statusCode() == 200) {
                        response.bodyHandler(buffer -> {
                            ObjectMapper mapper = new ObjectMapper();

                            try {
                                Map<String, Map<String, String>> fetchedLoggers = mapper.readValue(buffer.getBytes(), Map.class);
                                result.complete(fetchedLoggers);
                            } catch (IOException e)  {
                                log.warn("Failed to get list of connector loggers", e);
                                result.fail(new ConnectRestException(response, "Failed to get connector loggers", e));
                            }
                        });
                    } else {
                        result.fail(new ConnectRestException(response, "Unexpected status code"));
                    }
                })
                .exceptionHandler(result::fail)
                .setFollowRedirects(true)
                .putHeader("Accept", "application/json")
                .end();
        return result.future();
    }

    private Future updateLoggers(String host, int port, String desiredLogging, Map<String, Map<String, String>> fetchedLoggers) {
        Map<String, String> updateLoggers = new LinkedHashMap<>();
        fetchedLoggers.entrySet().forEach(entry -> {
            // set all logger levels to ERROR
            updateLoggers.put(entry.getKey(), "ERROR");
        });

        OrderedProperties ops = new OrderedProperties();
        ops.addStringPairs(desiredLogging);
        ops.asMap().entrySet().forEach(entry -> {
            // set desired loggers to desired levels
            if (entry.getKey().equals("log4j.rootLogger")) {
                updateLoggers.put("root", entry.getValue());
            } else if (!entry.getKey().startsWith("log4j.appender")) {
                updateLoggers.put(entry.getKey().replace("log4j.logger.", ""), entry.getValue());
            }
        });

        LinkedHashMap<String, String> updateSortedLoggers = sortLoggers(updateLoggers);
        Future<Void> result = Future.succeededFuture();
        for (Map.Entry<String, String> logger: updateSortedLoggers.entrySet()) {
            result = result.compose(previous -> updateConnectorLogger(host, port, logger.getKey(), logger.getValue().split(",")[0].replaceAll("\\s", "")));
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Future<Void> updateConnectorLoggers(String host, int port, String desiredLogging) {
        return listConnectorLoggers(host, port).compose(fetchedLoggers -> updateLoggers(host, port, desiredLogging, fetchedLoggers));
    }

    private int numberOfDotsInString(String tested) {
        return tested.length() - tested.replaceAll("\\.", "").length();
    }

    /**
     * To apply loggers correctly, we need to sort them. The sorting is performed on base of logger generality.
     * Logger "abc.company" is more general than "abc.company.name"
     * @param loggers map of loggers to be sorted
     * @return map of sorted loggers
     */
    private LinkedHashMap<String, String> sortLoggers(Map<String, String> loggers) {
        Comparator<Map.Entry<String, String>> loggerComparator = (e1, e2) -> {
            String k1 = e1.getKey();
            String k2 = e2.getKey();
            if (k1.equals("root")) {
                // we need root logger always to be the first logger to be set via REST API
                return Integer.MIN_VALUE;
            }
            return numberOfDotsInString(k1) - numberOfDotsInString(k2);
        };
        List<Map.Entry<String, String>> listOfEntries = new ArrayList<>(loggers.entrySet());
        Collections.sort(listOfEntries, loggerComparator);

        LinkedHashMap<String, String> sortedLoggers = new LinkedHashMap<>(listOfEntries.size());
        for (Map.Entry<String, String> entry : listOfEntries) {
            sortedLoggers.put(entry.getKey(), entry.getValue());
        }
        return sortedLoggers;
    }
}
