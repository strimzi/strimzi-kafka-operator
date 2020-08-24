/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.operator.assembly;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.strimzi.api.kafka.model.connect.ConnectorPlugin;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.model.OrderedProperties;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
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

    @Override
    public Future<Map<String, Map<String, String>>> listConnectLoggers(String host, int port) {
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
                .exceptionHandler(result::fail)
                .setFollowRedirects(true)
                .putHeader("Accept", "application/json")
                .end();
        return result.future();
    }

    private Future<Void> updateLoggers(String host, int port, String desiredLogging, Map<String, Map<String, String>> fetchedLoggers) {
        Map<String, String> updateLoggers = new LinkedHashMap<>();
        fetchedLoggers.entrySet().forEach(entry -> {
            // set all logger levels to OFF
            if (entry.getKey().startsWith("log4j.logger.")) {
                updateLoggers.put(entry.getKey().substring("log4j.logger.".length()), "OFF");
            } else {
                updateLoggers.put(entry.getKey(), "OFF");
            }
        });

        OrderedProperties ops = new OrderedProperties();
        ops.addStringPairs(desiredLogging);
        ops.asMap().entrySet().forEach(entry -> {
            // set desired loggers to desired levels
            if (entry.getKey().equals("log4j.rootLogger")) {
                updateLoggers.put("root", entry.getValue());
            } else if (entry.getKey().startsWith("log4j.logger.")) {
                updateLoggers.put(entry.getKey().substring("log4j.logger.".length()), entry.getValue());
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
    public Future<Void> updateConnectLoggers(String host, int port, String desiredLogging) {
        return listConnectLoggers(host, port).compose(fetchedLoggers -> updateLoggers(host, port, desiredLogging, fetchedLoggers));
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
        Collections.sort(listOfEntries, loggerComparator);

        LinkedHashMap<String, String> sortedLoggers = new LinkedHashMap<>(listOfEntries.size());
        for (Map.Entry<String, String> entry : listOfEntries) {
            sortedLoggers.put(entry.getKey(), entry.getValue());
        }
        return sortedLoggers;
    }
}
