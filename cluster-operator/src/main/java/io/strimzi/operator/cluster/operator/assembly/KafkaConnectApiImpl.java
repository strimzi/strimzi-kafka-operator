/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.operator.assembly;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.strimzi.api.kafka.model.connect.ConnectorPlugin;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.OrderedProperties;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
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
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static java.util.Arrays.asList;

@SuppressWarnings({"deprecation"})
class KafkaConnectApiImpl implements KafkaConnectApi {
    private static final Logger log = LogManager.getLogger(KafkaConnectApiImpl.class);
    public static final TypeReference<Map<String, Object>> TREE_TYPE = new TypeReference<Map<String, Object>>() {
    };
    public static final TypeReference<Map<String, String>> MAP_OF_STRINGS = new TypeReference<Map<String, String>>() {
    };

    public static final TypeReference<Map<String, Map<String, String>>> MAP_OF_MAP_OF_STRINGS = new TypeReference<Map<String, Map<String, String>>>() {

    };
    private final ObjectMapper mapper = new ObjectMapper();
    private final Vertx vertx;

    public KafkaConnectApiImpl(Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Future<Map<String, Object>> createOrUpdatePutRequest(
            String host, int port,
            String connectorName, JsonObject configJson) {
        Buffer data = configJson.toBuffer();
        String path = "/connectors/" + connectorName + "/config";
        log.debug("Making PUT request to {} with body {}", path, configJson);
        return withHttpClient((httpClient, result) ->
            httpClient.put(port, host, path, response -> {
                response.exceptionHandler(result::tryFail);
                if (response.statusCode() == 200 || response.statusCode() == 201) {
                    response.bodyHandler(buffer -> {
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
            .exceptionHandler(result::tryFail)
            .setFollowRedirects(true)
            .putHeader("Accept", "application/json")
            .putHeader("Content-Type", "application/json")
            .putHeader("Content-Length", String.valueOf(data.length()))
            .write(data)
            .end());
    }

    /**
     * Perform the given operation, which completes the promise, using an HTTP client instance,
     * after which the client is closed and the future for the promise returned.
     * @param operation The operation to perform.
     * @param <T> The type of the result
     * @return A future which is completed with the result performed by the operation
     */
    private <T> Future<T> withHttpClient(BiConsumer<HttpClient, Promise<T>> operation) {
        HttpClient httpClient = vertx.createHttpClient(new HttpClientOptions().setLogActivity(true));
        Promise<T> promise = Promise.promise();
        operation.accept(httpClient, promise);
        return promise.future().compose(
            result -> {
                httpClient.close();
                return Future.succeededFuture(result);
            },
            error -> {
                httpClient.close();
                return Future.failedFuture(error);
            });
    }

    @Override
    public Future<Map<String, Object>> getConnector(
            String host, int port,
            String connectorName) {
        return doGet(host, port, String.format("/connectors/%s", connectorName),
                new HashSet<>(asList(200, 201)),
                TREE_TYPE);
    }

    private <T> Future<T> doGet(String host, int port, String path, Set<Integer> okStatusCodes, TypeReference<T> type) {
        log.debug("Making GET request to {}", path);
        return withHttpClient((httpClient, result) ->
            httpClient.get(port, host, path, response -> {
                response.exceptionHandler(result::tryFail);
                if (okStatusCodes.contains(response.statusCode())) {
                    response.bodyHandler(buffer -> {
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
            .exceptionHandler(result::tryFail)
            .setFollowRedirects(true)
            .putHeader("Accept", "application/json")
            .end());
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
        String path = "/connectors/" + connectorName;
        return withHttpClient((httpClient, result) ->
            httpClient.delete(port, host, path, response -> {
                response.exceptionHandler(result::tryFail);
                if (response.statusCode() == 204) {
                    log.debug("Connector was deleted. Waiting for status deletion!");
                    withBackoff(new BackOff(200L, 2, 10), connectorName, Collections.singleton(200),
                        () -> status(host, port, connectorName, Collections.singleton(404)), "status")
                        .onComplete(res -> {
                            if (res.succeeded()) {
                                result.complete();
                            } else {
                                result.fail(res.cause());
                            }
                        });
                } else {
                    // TODO Handle 409 (Conflict) indicating a rebalance in progress
                    response.bodyHandler(buffer -> {
                        JsonObject x = buffer.toJsonObject();
                        result.fail(new ConnectRestException(response, x.getString("message")));
                    });
                }
            })
            .exceptionHandler(result::tryFail)
            .setFollowRedirects(true)
            .putHeader("Accept", "application/json")
            .putHeader("Content-Type", "application/json")
            .end());
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
                        if (cause instanceof ConnectRestException
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
                            connectorName, backOff.maxAttempts(), backOff.totalDelayMs());
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
        return status(host, port, connectorName, Collections.singleton(200));
    }

    @Override
    public Future<Map<String, Object>> status(String host, int port, String connectorName, Set<Integer> okStatusCodes) {
        String path = "/connectors/" + connectorName + "/status";
        return doGet(host, port, path, okStatusCodes, TREE_TYPE);
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
        return withHttpClient((httpClient, result) -> httpClient
                .put(port, host, path, response -> {
                    response.exceptionHandler(result::tryFail);
                    if (response.statusCode() == 202) {
                        result.complete();
                    } else {
                        result.fail("Unexpected status code " + response.statusCode()
                                + " for GET request to " + host + ":" + port + path);
                    }
                })
                .exceptionHandler(result::tryFail)
                .setFollowRedirects(true)
                .putHeader("Accept", "application/json")
                .end());
    }

    @Override
    public Future<List<String>> list(String host, int port) {
        String path = "/connectors";
        return withHttpClient((httpClient, result) -> httpClient
                .get(port, host, path, response -> {
                    response.exceptionHandler(result::tryFail);
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
                .exceptionHandler(result::tryFail)
                .setFollowRedirects(true)
                .putHeader("Accept", "application/json")
                .end());
    }

    @Override
    public Future<List<ConnectorPlugin>> listConnectorPlugins(String host, int port) {
        String path = "/connector-plugins";
        return withHttpClient((httpClient, result) -> httpClient
                .get(port, host, path, response -> {
                    response.exceptionHandler(result::tryFail);
                    if (response.statusCode() == 200) {
                        response.bodyHandler(buffer -> {
                            try {
                                result.complete(asList(mapper.readValue(buffer.getBytes(), ConnectorPlugin[].class)));
                            } catch (IOException e) {
                                log.warn("Failed to parse list of connector plugins", e);
                                result.fail(new ConnectRestException(response, "Failed to parse list of connector plugins", e));
                            }
                        });
                    } else {
                        result.fail(new ConnectRestException(response, "Unexpected status code"));
                    }
                })
                .exceptionHandler(result::tryFail)
                .setFollowRedirects(true)
                .putHeader("Accept", "application/json")
                .end());
    }

    private Future<Void> updateConnectorLogger(String host, int port, String logger, String level) {
        String path = "/admin/loggers/" + logger;
        JsonObject levelJO = new JsonObject();
        levelJO.put("level", level);
        log.debug("Making PUT request to {} with body {}", path, levelJO);
        return withHttpClient((httpClient, result) -> {
            Buffer buffer = levelJO.toBuffer();
            httpClient
                    .put(port, host, path, response -> {
                        response.exceptionHandler(result::tryFail);
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
                    .exceptionHandler(result::tryFail)
                    .putHeader("Content-Type", "application/json")
                    .setFollowRedirects(true)
                    .end(buffer);
        });
    }

    @Override
    public Future<Map<String, Map<String, String>>> listConnectLoggers(String host, int port) {
        String path = "/admin/loggers/";
        return withHttpClient((httpClient, result) -> httpClient
                .get(port, host, path, response -> {
                    response.exceptionHandler(result::tryFail);
                    if (response.statusCode() == 200) {
                        response.bodyHandler(buffer -> {
                            try {
                                Map<String, Map<String, String>> fetchedLoggers = mapper.readValue(buffer.getBytes(), MAP_OF_MAP_OF_STRINGS);
                                result.complete(fetchedLoggers);
                            } catch (IOException e) {
                                log.warn("Failed to get list of connector loggers", e);
                                result.fail(new ConnectRestException(response, "Failed to get connector loggers", e));
                            }
                        });
                    } else {
                        result.fail(new ConnectRestException(response, "Unexpected status code"));
                    }
                })
                .exceptionHandler(result::tryFail)
                .setFollowRedirects(true)
                .putHeader("Accept", "application/json")
                .end());
    }

    private Future<Void> updateLoggers(String host, int port, String desiredLogging, Map<String, Map<String, String>> fetchedLoggers, OrderedProperties defaultLogging) {
        desiredLogging = Util.expandVars(desiredLogging);
        Map<String, String> updateLoggers = new LinkedHashMap<>();
        defaultLogging.asMap().entrySet().forEach(entry -> {
            // set all logger levels to default
            if (entry.getKey().equals("log4j.rootLogger")) {
                updateLoggers.put("root", Util.expandVar(entry.getValue(), defaultLogging.asMap()));
            } else if (entry.getKey().startsWith("log4j.logger.")) {
                updateLoggers.put(entry.getKey().substring("log4j.logger.".length()), Util.expandVar(entry.getValue(), defaultLogging.asMap()));
            }
        });

        OrderedProperties ops = new OrderedProperties();
        ops.addStringPairs(desiredLogging);
        ops.asMap().entrySet().forEach(entry -> {
            // set desired loggers to desired levels
            if (entry.getKey().equals("log4j.rootLogger")) {
                if (fetchedLoggers.get("root") == null || fetchedLoggers.get("root").get("level") == null ||
                        !entry.getValue().equals(fetchedLoggers.get("root").get("level"))) {
                    updateLoggers.put("root", Util.expandVar(entry.getValue(), ops.asMap()));
                }
            } else if (entry.getKey().startsWith("log4j.logger.")) {
                Map<String, String> fetchedLogger = fetchedLoggers.get(entry.getKey().substring("log4j.logger.".length()));
                if (fetchedLogger == null || fetchedLogger.get("level") == null || !entry.getValue().equals(fetchedLogger.get("level"))) {
                    updateLoggers.put(entry.getKey().substring("log4j.logger.".length()), Util.expandVar(entry.getValue(), ops.asMap()));
                }
            }
        });

        LinkedHashMap<String, String> updateSortedLoggers = sortLoggers(updateLoggers);
        Future<Void> result = Future.succeededFuture();
        for (Map.Entry<String, String> logger : updateSortedLoggers.entrySet()) {
            result = result.compose(previous -> updateConnectorLogger(host, port, logger.getKey(), getLoggerLevelFromAppenderCouple(logger.getValue())));
        }
        return result;
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
    public Future<Void> updateConnectLoggers(String host, int port, String desiredLogging, OrderedProperties defaultLogging) {
        return listConnectLoggers(host, port)
                .compose(fetchedLoggers -> updateLoggers(host, port, desiredLogging, fetchedLoggers, defaultLogging));
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
            if (k2.equals("root")) {
                return Integer.MAX_VALUE;
            }
            return k1.compareTo(k2);
        };
        List<Map.Entry<String, String>> listOfEntries = new ArrayList<>(loggers.entrySet());
        listOfEntries.sort(loggerComparator);

        LinkedHashMap<String, String> sortedLoggers = new LinkedHashMap<>(listOfEntries.size());
        for (Map.Entry<String, String> entry : listOfEntries) {
            sortedLoggers.put(entry.getKey(), entry.getValue());
        }
        return sortedLoggers;
    }
}
