/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public interface KafkaConnectApi {
    Future<JsonObject> createOrUpdatePutRequest(String host, int port, String connectorName, JsonObject configJson);
    Future<Void> delete(String host, int port, String connectorName);
    Future<List<String>> list(String host, int port);
}

class ConnectRestException extends RuntimeException {
    public ConnectRestException(String message) {
        super(message);
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
    public Future<JsonObject> createOrUpdatePutRequest(
            String host, int port,
            String connectorName,
                                                       JsonObject configJson) {
        Future<JsonObject> result = Future.future();
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
                            result.complete(buffer.toJsonObject());
                        });
                    } else {
                        // TODO Handle 409 (Conflict) indicating a rebalance in progress
                        response.bodyHandler(buffer -> {
                            JsonObject x = buffer.toJsonObject();
                            result.fail(new ConnectRestException(x.getString("message")));
                        });
                    }
                })
                .exceptionHandler(error -> {
                    result.fail(error);
                })
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
                            result.fail(new ConnectRestException(x.getString("message")));
                        });
                    }
                })
                .setFollowRedirects(true)
                .putHeader("Accept", "application/json")
                .putHeader("Content-Type", "application/json")
                //.write(configJson.toBuffer())
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
                    if (response.statusCode() == 200 || response.statusCode() == 201) {
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
                .exceptionHandler(error -> {
                    result.fail(error);
                })
                .setFollowRedirects(true)
                .putHeader("Accept", "application/json")
                .end();
        return result;

    }
}
