/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
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
import java.util.List;
import java.util.Map;

public interface KafkaConnectApi {
    Future<Map<String, Object>> createOrUpdatePutRequest(String host, int port, String connectorName, JsonObject configJson);
    Future<Void> delete(String host, int port, String connectorName);
    Future<Map<String, Object>> status(String host, int port, String connectorName);
    Future<Void> pause(String host, int port, String connectorName);
    Future<Void> resume(String host, int port, String connectorName);
    Future<List<String>> list(String host, int port);
}

class ConnectRestException extends RuntimeException {
    ConnectRestException(String method, String path, int statusCode, String statusMessage, String message) {
        super(method + " " + path + " returned " + statusCode + " (" + statusMessage + "): " + message);
    }
    public ConnectRestException(HttpClientResponse response, String message) {
        this(response.request().method().toString(), response.request().path(), response.statusCode(), response.statusMessage(), message);
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
}
