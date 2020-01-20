/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.strimzi.api.kafka.model.connect.ConnectorPlugin;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public interface KafkaConnectApi {
    Future<Map<String, Object>> createOrUpdatePutRequest(String host, int port, String connectorName, JsonObject configJson);
    Future<Void> delete(String host, int port, String connectorName);
    Future<Map<String, Object>> status(String host, int port, String connectorName);
    Future<Map<String, Object>> statusWithBackOff(BackOff backOff, String host, int port, String connectorName);
    Future<Void> pause(String host, int port, String connectorName);
    Future<Void> resume(String host, int port, String connectorName);
    Future<List<String>> list(String host, int port);
    Future<List<ConnectorPlugin>> listConnectorPlugins(String host, int port);
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
    private final Vertx vertx;

    public KafkaConnectApiImpl(Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Future<Map<String, Object>> createOrUpdatePutRequest(
            String host, int port,
            String connectorName, JsonObject configJson) {
        Future<Map<String, Object>> result = Future.future();
        HttpClientOptions options = new HttpClientOptions().setLogActivity(true);
        Buffer data = configJson.toBuffer();
        String path = "/connectors/" + connectorName + "/config";
        vertx.createHttpClient(options)
                .put(port, host, path, response -> {
                    response.exceptionHandler(error -> {
                        result.fail(error);
                    });
                    if (response.statusCode() == 200 || response.statusCode() == 201) {
                        response.bodyHandler(buffer -> {
                            ObjectMapper mapper = new ObjectMapper();
                            try {
                                result.complete(mapper.readValue(buffer.getBytes(), Map.class));
                            } catch (IOException e) {
                                result.fail(new ConnectRestException(response, "Could not deserialize response: " + e));
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
                .exceptionHandler(result::fail)
                .setFollowRedirects(true)
                .putHeader("Accept", "application/json")
                .putHeader("Content-Type", "application/json")
                .putHeader("Content-Length", String.valueOf(data.length()))
                .write(data)
                .end();
        return result;
    }

    @Override
    public Future<Void> delete(String host, int port, String connectorName) {
        Future<Void> result = Future.future();
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
        return result;
    }

    @Override
    public Future<Map<String, Object>> statusWithBackOff(BackOff backOff, String host, int port, String connectorName) {
        Promise<Map<String, Object>> result = Promise.promise();

        Handler<Long> handler = new Handler<Long>() {
            @Override
            public void handle(Long tid) {
                status(host, port, connectorName).setHandler(connectorStatus -> {
                    if (connectorStatus.succeeded()) {
                        result.complete(connectorStatus.result());
                    } else {
                        Throwable cause = connectorStatus.cause();
                        if (cause != null
                                && cause instanceof ConnectRestException
                                && ((ConnectRestException) cause).getStatusCode() == 404) {
                            if (backOff.done()) {
                                log.debug("Connector {} status returned HTTP 404 and we run out of back off time", connectorName);
                                result.fail(cause);
                            } else {
                                log.debug("Connector {} status returned HTTP 404 - backing off", connectorName);
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
    @SuppressWarnings("unchecked")
    public Future<Map<String, Object>> status(String host, int port, String connectorName) {
        Future<Map<String, Object>> result = Future.future();
        HttpClientOptions options = new HttpClientOptions().setLogActivity(true);
        String path = "/connectors/" + connectorName + "/status";
        vertx.createHttpClient(options)
                .get(port, host, path, response -> {
                    if (response.statusCode() == 200) {
                        response.bodyHandler(buffer -> {
                            ObjectMapper mapper = new ObjectMapper();
                            try {
                                result.complete(mapper.readValue(buffer.getBytes(), Map.class));
                            } catch (IOException e) {
                                result.fail(new ConnectRestException(response, "Could not deserialize response: " + e));
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
                .exceptionHandler(result::fail)
                .setFollowRedirects(true)
                .putHeader("Accept", "application/json")
                .putHeader("Content-Type", "application/json")
                .end();
        return result;
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
        Future<Void> result = Future.future();
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
        return result;
    }

    @Override
    public Future<List<String>> list(String host, int port) {
        Future<List<String>> result = Future.future();
        HttpClientOptions options = new HttpClientOptions().setLogActivity(true);
        String path = "/connectors";
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
        return result;
    }

    @Override
    public Future<List<ConnectorPlugin>> listConnectorPlugins(String host, int port) {
        Future<List<ConnectorPlugin>> result = Future.future();
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
                                result.complete(Arrays.asList(mapper.readValue(buffer.getBytes(), ConnectorPlugin[].class)));
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
        return result;
    }
}
